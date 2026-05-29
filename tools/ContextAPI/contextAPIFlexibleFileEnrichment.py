#!/usr/bin/env python3
import requests
import json
import sys
import os
import pandas as pd
from datetime import datetime
import concurrent.futures
from requests.adapters import HTTPAdapter
import time
import threading
import signal

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
MAX_WORKERS = 32
CHUNK_SIZE = 10000
REQUEST_TIMEOUT = 10
MAX_RETRIES = 8

ADAPTER = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Shutdown Event ---
SHUTDOWN_EVENT = threading.Event()

def sigint_handler(sig, frame):
    if not SHUTDOWN_EVENT.is_set():
        print("\n\n" + "="*50)
        print("[!] CANCELING SCRIPT (Ctrl+C Detected)".center(50))
        print("="*50)
        print("  - Stopping new API requests...")
        print("  - Finishing currently active requests...")
        print("  - (Press Ctrl+C again to FORCE QUIT immediately)")
        print("="*50 + "\n")
        SHUTDOWN_EVENT.set()
    else:
        print("\n[!] Force exit triggered. Shutting down...")
        os._exit(1)

# --- Functions ---
def get_composite_key(ip, timestamp):
    """Creates a unique string key combining IP and Timestamp."""
    ip_str = str(ip).strip().lower()
    ts_str = str(timestamp).strip() if pd.notna(timestamp) and timestamp else "none"
    return f"{ip_str}|{ts_str}"

def enrich_ip(row_data, api_token, perform_historic_lookup, use_maxmind_geo):
    if SHUTDOWN_EVENT.is_set():
        row_data['Error_Reason'] = "Canceled (Graceful Shutdown)"
        return (False, row_data)

    ip_address = row_data.get('IP')
    timestamp = row_data.get('Timestamp') if perform_historic_lookup else None

    if not ip_address or str(ip_address).lower() == 'nan':
        return (False, {**row_data, 'Error_Reason': 'Missing or Invalid IP'})

    url = f"{api_url_base}{ip_address}"
    query_params = []
    
    if timestamp:
        query_params.append(f"dt={timestamp}")
    if use_maxmind_geo:
        query_params.append("mmgeo=1")

    if query_params:
        url += "?" + "&".join(query_params)
        
    headers = {'TOKEN': api_token}

    for attempt in range(MAX_RETRIES + 1):
        if SHUTDOWN_EVENT.is_set():
            row_data['Error_Reason'] = "Canceled during retry (Graceful Shutdown)"
            return (False, row_data)

        try:
            response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 404:
                row_data['Error_Reason'] = "404 Not Found (No Data)"
                return (False, row_data)
                
            response.raise_for_status()
            json_response = response.json()
            
            if attempt > 0:
                print(f"  [+] IP {ip_address} successfully enriched after {attempt} retry(s).")
                
            # Merge original row data with API response
            merged_data = {**row_data, **json_response}
            # Remove redundant 'IP' key (API provides 'ip' natively)
            merged_data.pop('IP', None)
            
            return (True, merged_data)
            
        except requests.exceptions.RequestException as e:
            is_retryable = True
            error_desc = str(e)

            if isinstance(e, requests.exceptions.ConnectionError):
                error_desc = "Connection Error"
            elif isinstance(e, requests.exceptions.Timeout):
                error_desc = "Read Timeout"
            elif hasattr(e, 'response') and e.response is not None:
                if e.response.status_code not in [429, 500, 502, 503, 504]:
                    is_retryable = False
                error_desc = f"HTTP {e.response.status_code}"
            
            if is_retryable and attempt < MAX_RETRIES:
                backoff_time = 2 * (2 ** attempt)
                print(f"  [!] Error on {ip_address} ({error_desc}). Backing off {backoff_time}s (Attempt {attempt + 1}/{MAX_RETRIES})...")
                
                for _ in range(backoff_time):
                    if SHUTDOWN_EVENT.is_set():
                        break
                    time.sleep(1)
            else:
                fail_prefix = f"Failed after {MAX_RETRIES} retries" if is_retryable else "Failed (Non-retryable)"
                row_data['Error_Reason'] = f"{fail_prefix}: {error_desc}"
                
                if attempt >= MAX_RETRIES:
                    print(f"  [-] IP {ip_address} permanently failed after maximum retries.")
                    
                return (False, row_data)
        except Exception as e:
            row_data['Error_Reason'] = f"Unexpected Error: {str(e)}"
            print(f"  [-] IP {ip_address} crashed unexpectedly: {str(e)}")
            return (False, row_data)

def find_and_map_columns(df):
    original_columns = df.columns
    normalized_columns = original_columns.str.lower().str.strip()
    
    ip_col_original = None
    ts_col_original = None

    for i, col in enumerate(normalized_columns):
        if ('ip address' in col or 'ips' in col or 'ip' in col) and ip_col_original is None:
            ip_col_original = original_columns[i]
        if 'timestamp' in col and ts_col_original is None:
            ts_col_original = original_columns[i]

    if ip_col_original is None:
        raise ValueError("Input file must contain a column for IP (e.g., 'ip address', 'ips').")
    
    return ip_col_original, ts_col_original

def process_chunk(df_chunk, ip_col, ts_col, perform_historic_lookup):
    all_rows_data = []
    for index, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        row_dict['IP'] = row_dict.pop(ip_col)
        
        if ts_col:
            val = row_dict.pop(ts_col)
            formatted_timestamp = None
            
            if pd.notna(val) and str(val).lower() != 'nan':
                if isinstance(val, (pd.Timestamp, datetime)):
                    formatted_timestamp = val.strftime('%Y%m%d')
                else:
                    try:
                        epoch_val = float(val)
                        if epoch_val > 100000000: 
                            dt_obj = datetime.fromtimestamp(epoch_val)
                            formatted_timestamp = dt_obj.strftime('%Y%m%d')
                    except (ValueError, OverflowError, TypeError): 
                        pass 

                    if formatted_timestamp is None:
                        ts_str = str(val).strip()
                        if ts_str.endswith('.0'):
                            ts_str = ts_str[:-2]

                        formats = [
                            '%Y%m%d', '%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%m-%d-%Y',
                            '%m/%d/%Y %H:%M', '%m/%d/%y %H:%M', '%Y-%m-%d %H:%M:%S',
                            '%a, %b %d, %Y %I:%M %p %Z'
                        ]
                        for fmt in formats:
                            try:
                                dt_obj = datetime.strptime(ts_str, fmt)
                                formatted_timestamp = dt_obj.strftime('%Y%m%d')
                                break
                            except ValueError:
                                continue
                        
                        if not formatted_timestamp:
                            try:
                                dt_obj = datetime.fromisoformat(ts_str.replace('Z', ''))
                                formatted_timestamp = dt_obj.strftime('%Y%m%d')
                            except ValueError:
                                formatted_timestamp = None

            row_dict['Timestamp'] = formatted_timestamp
            if not perform_historic_lookup:
                row_dict.pop('Timestamp', None)

        for key, value in row_dict.items():
            if isinstance(value, pd.Timestamp):
                row_dict[key] = str(value)

        all_rows_data.append(row_dict)
    return all_rows_data

def write_to_json_stream(results_iterator, output_path, failed_path, stats_ref, start_time):
    last_update_time = time.time()
    try:
        with open(output_path, 'a', encoding='utf-8') as outfile, \
             open(failed_path, 'a', encoding='utf-8') as failfile:
             
            for success, result_data in results_iterator:
                if success:
                    outfile.write(json.dumps(result_data, ensure_ascii=False) + '\n')
                    outfile.flush()
                    stats_ref['success'] += 1
                else:
                    failfile.write(json.dumps(result_data, ensure_ascii=False) + '\n')
                    failfile.flush()
                    stats_ref['failed'] += 1
                
                stats_ref['processed'] += 1
                
                current_time = time.time()
                if current_time - last_update_time >= 5 and not SHUTDOWN_EVENT.is_set():
                    elapsed = current_time - start_time
                    rps = stats_ref['processed'] / elapsed if elapsed > 0 else 0
                    skip_str = f", {stats_ref['skipped']} skipped" if stats_ref['skipped'] > 0 else ""
                    print(f"  Processed {stats_ref['processed']} ({stats_ref['success']} ok, {stats_ref['failed']} fail{skip_str}) - {rps:.2f} r/s")
                    last_update_time = current_time
    except Exception as e:
        print(f"\nError writing to file: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Script ---
if __name__ == "__main__":
    start_main_time = time.time()

    api_token = os.environ.get("TOKEN")
    if not api_token:
        api_token = input("Please enter your Spur API token: ").strip()
        if not api_token:
            print("No token provided. Exiting.", file=sys.stderr)
            sys.exit(1)
        os.environ['TOKEN'] = api_token

    # --- Mode Selection ---
    print("\n" + "="*40)
    print("Spur API Enrichment Tool".center(40))
    print("="*40)
    print("[1] Start New Enrichment Session")
    print("[2] Resume Previous Session")
    
    resume_mode = False
    while True:
        mode_choice = input("\nSelect an option (1 or 2): ").strip()
        if mode_choice in ['1', '2']:
            resume_mode = (mode_choice == '2')
            break
        print("Invalid choice.")

    processed_keys = set()
    prev_success_path = None
    prev_fail_path = None

    if resume_mode:
        print("\n--- Resume Setup ---")
        while True:
            prev_success_path = input("Path to previous SUCCESS JSON file (or press Enter if none): ").strip()
            if not prev_success_path or os.path.exists(prev_success_path): break
            print("File not found.")
            
        while True:
            prev_fail_path = input("Path to previous FAILED JSON file (or press Enter if none): ").strip()
            if not prev_fail_path or os.path.exists(prev_fail_path): break
            print("File not found.")

        print("\nScanning previous files to build memory cache...")
        for filepath in [prev_success_path, prev_fail_path]:
            if filepath and os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line)
                                # Fallback checks for lowercase 'ip' (successes) or uppercase 'IP' (failures)
                                ip = record.get('ip') or record.get('IP')
                                ts = record.get('Timestamp')
                                if ip:
                                    processed_keys.add(get_composite_key(ip, ts))
                            except json.JSONDecodeError:
                                pass
        print(f"✅ Loaded {len(processed_keys):,} previously processed records into RAM.")

    # --- Spreadsheet Input ---
    input_file_path = sys.argv[1] if len(sys.argv) == 2 else None
    if input_file_path is None or resume_mode:
        print("\n--- Input Spreadsheet ---")
        while True:
            file_input = input("Enter the path to your CSV or XLSX file to enrich: ").strip()
            if os.path.exists(file_input):
                input_file_path = file_input
                break
            print(f"Error: File not found.")

    output_dir = os.path.dirname(input_file_path)
    input_file_name_without_ext = os.path.splitext(os.path.basename(input_file_path))[0]

    print(f"\nReading data from {input_file_path}...")
    file_ext = input_file_path.lower().split('.')[-1]
    
    if file_ext == 'csv':
        reader = pd.read_csv(input_file_path, chunksize=CHUNK_SIZE)
        first_chunk = next(reader)
        reader = pd.read_csv(input_file_path, chunksize=CHUNK_SIZE)
    else:
        first_chunk = pd.read_excel(input_file_path)
        reader = [first_chunk]
        
    first_chunk.columns = first_chunk.columns.str.lower().str.strip()
    ip_col, ts_col = find_and_map_columns(first_chunk)
    
    perform_historic_lookup = False
    if ts_col:
        print("-" * 35)
        lookup_input = input(f"A Timestamp column ('{ts_col}') was detected. Perform historical lookups? (yes/no): ").strip().lower()
        perform_historic_lookup = lookup_input in ['yes', 'y']
        print("-" * 35)

    use_maxmind_geo = False
    geo_input = input("Use MaxMind for geolocation (mmgeo=1)? (yes/no): ").strip().lower()
    use_maxmind_geo = geo_input in ['yes', 'y']
    print("-" * 35)

    # --- Output File Configuration ---
    if resume_mode:
        output_file_path = prev_success_path if prev_success_path else os.path.join(output_dir, f"{input_file_name_without_ext}_SpurEnrichment.json")
        failed_file_path = prev_fail_path if prev_fail_path else os.path.join(output_dir, f"{input_file_name_without_ext}_NoEnrichmentData.json")
        print(f"\nResuming! New results will be APPENDED to:")
        print(f"  Success -> {output_file_path}")
        print(f"  Failures -> {failed_file_path}")
    else:
        default_out = f"{input_file_name_without_ext}_SpurHistoricEnrichment.json" if perform_historic_lookup else f"{input_file_name_without_ext}_SpurEnrichment.json"
        out_input = input(f"\nEnter success output file name, or press Enter for default ({default_out}): ").strip()
        output_file_path = os.path.join(output_dir, out_input if out_input else default_out)
        failed_file_path = os.path.join(output_dir, f"{input_file_name_without_ext}_NoEnrichmentData.json")
        
        # Clear files if they already exist in a NEW session
        if os.path.exists(output_file_path): open(output_file_path, 'w').close()
        if os.path.exists(failed_file_path): open(failed_file_path, 'w').close()

    signal.signal(signal.SIGINT, sigint_handler)

    total_ips = 0
    stats = {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0}

    print("\n" + "-"*50)
    print("💡 TIP: Press Ctrl+Z to PAUSE, type 'fg' to RESUME.")
    print("💡 TIP: Press Ctrl+C to SAVE & QUIT gracefully.")
    print(f"💡 TIP: Failures are now streaming live to {os.path.basename(failed_file_path)}")
    print("-" * 50 + "\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in reader:
            chunk.columns = chunk.columns.str.lower().str.strip()
            total_ips += len(chunk)
            chunk_data = process_chunk(chunk, ip_col, ts_col, perform_historic_lookup)
            
            valid_to_process = []
            for r in chunk_data:
                ip_addr = r.get('IP')
                if not ip_addr or str(ip_addr).lower() == 'nan':
                    continue
                
                # Check memory cache before sending to API
                ts_val = r.get('Timestamp') if perform_historic_lookup else None
                composite_key = get_composite_key(ip_addr, ts_val)
                
                if resume_mode and composite_key in processed_keys:
                    stats['skipped'] += 1
                    stats['processed'] += 1
                else:
                    valid_to_process.append(r)
            
            results = executor.map(lambda r: enrich_ip(r, api_token, perform_historic_lookup, use_maxmind_geo), valid_to_process)
            write_to_json_stream(results, output_file_path, failed_file_path, stats, start_main_time)
            
            if SHUTDOWN_EVENT.is_set():
                break
            
    print("\n" + "="*50)
    print("COMPLETED / SAVED".center(50))
    print("="*50)
    print(f"Total IPs Evaluated:    {stats['processed']}")
    print(f"Already Processed (Skip):{stats['skipped']}")
    print(f"Successfully Enriched:  {stats['success']}")
    print(f"Failed Lookups:         {stats['failed']}")
    print("-" * 50)
    print(f"Successes saved to: {output_file_path}")
    if stats['failed'] > 0:
        print(f"Failures saved to:  {failed_file_path}")

    print(f"\nTotal runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
