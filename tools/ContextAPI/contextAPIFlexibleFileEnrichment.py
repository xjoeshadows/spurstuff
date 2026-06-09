#!/usr/bin/env python3
import requests
import json
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
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

# --- Core Functions ---
def get_composite_key(ip, timestamp):
    ip_str = str(ip).strip().lower()
    ts_str = str(timestamp).strip() if pd.notna(timestamp) and timestamp else "none"
    return f"{ip_str}|{ts_str}"

def parse_to_datetime(val):
    """Universal date parser. Returns a python datetime object or None."""
    if pd.isna(val) or str(val).lower() == 'nan':
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return val
    try:
        epoch_val = float(val)
        if epoch_val > 100000000: 
            return datetime.fromtimestamp(epoch_val)
    except (ValueError, OverflowError, TypeError): 
        pass 

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
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(ts_str.replace('Z', ''))
    except ValueError:
        return None

def find_and_map_columns(df):
    """Detects IP, Start Date, and End Date columns dynamically."""
    original_columns = df.columns
    normalized_columns = original_columns.str.lower().str.strip()
    
    ip_col = start_col = end_col = ts_col = None

    for i, col in enumerate(normalized_columns):
        orig_name = original_columns[i]
        if ('ip address' in col or 'ips' in col or 'ip' == col) and not ip_col:
            ip_col = orig_name
        elif ('first' in col or 'start' in col) and not start_col:
            start_col = orig_name
        elif ('last' in col or 'end' in col) and not end_col:
            end_col = orig_name
        elif ('timestamp' in col or 'date' in col) and not ts_col:
            ts_col = orig_name

    if not ip_col:
        raise ValueError("Input file must contain a column for IP (e.g., 'IP Address', 'IP').")
    
    # Resolve missing date columns
    if not start_col and not end_col:
        start_col = ts_col
        end_col = ts_col
    elif start_col and not end_col:
        end_col = start_col
    elif end_col and not start_col:
        start_col = end_col

    return ip_col, start_col, end_col

def pre_scan_max_days(file_path, is_csv, start_col, end_col):
    """Reads the entire file to find the maximum day delta between start and end dates."""
    print(f"\n[Scanning] Parsing dates across entire file to find max range...")
    max_delta = 0
    
    if is_csv:
        reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
    else:
        reader = [pd.read_excel(file_path)]
        
    for chunk in reader:
        # Sanitize chunk columns to match the mapped column names
        chunk.columns = chunk.columns.str.lower().str.strip()
        for index, row in chunk.iterrows():
            s_val, e_val = row.get(start_col), row.get(end_col)
            s_dt = parse_to_datetime(s_val)
            e_dt = parse_to_datetime(e_val)
            
            if s_dt and e_dt:
                if s_dt > e_dt: # Swap if backward
                    s_dt, e_dt = e_dt, s_dt
                delta = (e_dt - s_dt).days + 1
                if delta > max_delta:
                    max_delta = delta
                    
    return max_delta

def process_chunk(df_chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path):
    """Expands rows into daily increments and logs overflow to the skipped file."""
    all_rows_data = []
    
    for index, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        ip_val = row_dict.pop(ip_col)
        row_dict['IP'] = ip_val
        
        # Clean up existing pandas timestamps for JSON serialization
        for key, value in row_dict.items():
            if isinstance(value, pd.Timestamp):
                row_dict[key] = str(value)

        if not perform_historic_lookup or not start_col:
            row_dict.pop(start_col, None)
            row_dict.pop(end_col, None)
            row_dict['Timestamp'] = None
            all_rows_data.append(row_dict)
            continue

        s_val = row_dict.pop(start_col, None)
        e_val = row_dict.pop(end_col, None) if end_col != start_col else s_val
        
        s_dt = parse_to_datetime(s_val)
        e_dt = parse_to_datetime(e_val)

        # Fallback if parsing completely fails
        if not s_dt and not e_dt:
            row_dict['Timestamp'] = None
            all_rows_data.append(row_dict)
            continue
        elif s_dt and not e_dt:
            e_dt = s_dt
        elif e_dt and not s_dt:
            s_dt = e_dt

        # Swap if dates were entered backwards
        if s_dt > e_dt:
            s_dt, e_dt = e_dt, s_dt

        total_days = (e_dt - s_dt).days + 1
        days_to_process = total_days
        capped = False

        if max_days_cap and total_days > max_days_cap:
            days_to_process = max_days_cap
            capped = True

        actual_end_dt = s_dt + timedelta(days=days_to_process - 1)

        # Write to audit trail if we are dropping days
        if capped and skipped_file_path:
            skipped_start = actual_end_dt + timedelta(days=1)
            audit_record = {
                "IP": ip_val,
                "Processed_Range": f"{s_dt.strftime('%Y%m%d')} to {actual_end_dt.strftime('%Y%m%d')}",
                "Skipped_Range": f"{skipped_start.strftime('%Y%m%d')} to {e_dt.strftime('%Y%m%d')}",
                "Reason": f"Exceeded {max_days_cap}-day cap. (Total span: {total_days} days)"
            }
            try:
                with open(skipped_file_path, 'a', encoding='utf-8') as sf:
                    sf.write(json.dumps(audit_record, ensure_ascii=False) + '\n')
            except Exception:
                pass

        # Expand the row for each day in the processed range
        for i in range(days_to_process):
            curr_dt = s_dt + timedelta(days=i)
            new_row = row_dict.copy()
            new_row['Timestamp'] = curr_dt.strftime('%Y%m%d')
            all_rows_data.append(new_row)

    return all_rows_data

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
                
            merged_data = {**row_data, **json_response}
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
                print(f"  [!] Error on {ip_address} ({error_desc}). Backing off {backoff_time}s...")
                
                for _ in range(backoff_time):
                    if SHUTDOWN_EVENT.is_set(): break
                    time.sleep(1)
            else:
                fail_prefix = f"Failed after {MAX_RETRIES} retries" if is_retryable else "Failed (Non-retryable)"
                row_data['Error_Reason'] = f"{fail_prefix}: {error_desc}"
                if attempt >= MAX_RETRIES:
                    print(f"  [-] IP {ip_address} permanently failed.")
                return (False, row_data)
        except Exception as e:
            row_data['Error_Reason'] = f"Unexpected Error: {str(e)}"
            print(f"  [-] IP {ip_address} crashed unexpectedly: {str(e)}")
            return (False, row_data)

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
                    print(f"  API Hits {stats_ref['processed']} ({stats_ref['success']} ok, {stats_ref['failed']} fail{skip_str}) - {rps:.2f} r/s")
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
    prev_success_path = prev_fail_path = None

    if resume_mode:
        print("\n--- Resume Setup ---")
        while True:
            prev_success_path = input("Path to previous SUCCESS JSON (or press Enter if none): ").strip()
            if not prev_success_path or os.path.exists(prev_success_path): break
            print("File not found.")
            
        while True:
            prev_fail_path = input("Path to previous FAILED JSON (or press Enter if none): ").strip()
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
                                ip = record.get('ip') or record.get('IP')
                                ts = record.get('Timestamp')
                                if ip:
                                    processed_keys.add(get_composite_key(ip, ts))
                            except json.JSONDecodeError:
                                pass
        print(f"✅ Loaded {len(processed_keys):,} previously processed records into RAM.")

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
    is_csv_file = input_file_path.lower().endswith('.csv')

    print(f"\nReading data from {input_file_path}...")
    
    if is_csv_file:
        reader = pd.read_csv(input_file_path, chunksize=CHUNK_SIZE)
        first_chunk = next(reader)
    else:
        first_chunk = pd.read_excel(input_file_path)
        reader = [first_chunk]
        
    # FIX: Sanitize the very first chunk's columns BEFORE passing to find_and_map_columns
    first_chunk.columns = first_chunk.columns.str.lower().str.strip()
    ip_col, start_col, end_col = find_and_map_columns(first_chunk)
    
    perform_historic_lookup = False
    max_days_cap = None

    if start_col:
        print("-" * 50)
        col_display = f"'{start_col}' & '{end_col}'" if start_col != end_col else f"'{start_col}'"
        lookup_input = input(f"Date column(s) detected: {col_display}.\nPerform historical/range lookups? (yes/no): ").strip().lower()
        perform_historic_lookup = lookup_input in ['yes', 'y']
        
        if perform_historic_lookup and start_col != end_col:
            print("\n--- API Volume Protection ---")
            print("Because you have Start and End dates, rows will multiply into daily requests.")
            print("  [1] Set a manual cap (e.g., max 30 days per IP)")
            print("  [2] Scan file first to see the maximum date range (May take minutes)")
            print("  [3] No cap (Process ALL days)")
            
            while True:
                cap_choice = input("Select an option (1, 2, or 3): ").strip()
                if cap_choice in ['1', '2', '3']: break
                print("Invalid choice.")
                
            if cap_choice == '2':
                found_max = pre_scan_max_days(input_file_path, is_csv_file, start_col, end_col)
                print(f"✅ Maximum date range found in file: {found_max} days.")
                cap_choice = '1' # Funnel into manual entry now
                
            if cap_choice == '1':
                while True:
                    try:
                        max_days_cap = int(input("Enter maximum days to process per IP (e.g., 30): ").strip())
                        if max_days_cap > 0: break
                        print("Must be a positive number.")
                    except ValueError:
                        print("Please enter a valid number.")
        print("-" * 50)

    use_maxmind_geo = False
    geo_input = input("Use MaxMind for geolocation (mmgeo=1)? (yes/no): ").strip().lower()
    use_maxmind_geo = geo_input in ['yes', 'y']
    print("-" * 50)

    # Re-initialize reader because we consumed the first chunk (or the whole file during pre-scan)
    if is_csv_file:
        reader = pd.read_csv(input_file_path, chunksize=CHUNK_SIZE)
    else:
        reader = [pd.read_excel(input_file_path)]

    if resume_mode:
        output_file_path = prev_success_path if prev_success_path else os.path.join(output_dir, f"{input_file_name_without_ext}_SpurEnrichment.json")
        failed_file_path = prev_fail_path if prev_fail_path else os.path.join(output_dir, f"{input_file_name_without_ext}_NoEnrichmentData.json")
    else:
        default_out = f"{input_file_name_without_ext}_SpurHistoricEnrichment.json" if perform_historic_lookup else f"{input_file_name_without_ext}_SpurEnrichment.json"
        out_input = input(f"\nEnter success output file name, or press Enter for default ({default_out}): ").strip()
        output_file_path = os.path.join(output_dir, out_input if out_input else default_out)
        failed_file_path = os.path.join(output_dir, f"{input_file_name_without_ext}_NoEnrichmentData.json")
        
        if os.path.exists(output_file_path): open(output_file_path, 'w').close()
        if os.path.exists(failed_file_path): open(failed_file_path, 'w').close()

    skipped_file_path = None
    if max_days_cap:
        skipped_file_path = os.path.join(output_dir, f"{input_file_name_without_ext}_SkippedDateRanges.json")
        if not resume_mode and os.path.exists(skipped_file_path):
            open(skipped_file_path, 'w').close() # Clear on new session

    signal.signal(signal.SIGINT, sigint_handler)

    stats = {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0}

    print("\n" + "-"*50)
    print("💡 TIP: Press Ctrl+Z to PAUSE, type 'fg' to RESUME.")
    print("💡 TIP: Press Ctrl+C to SAVE & QUIT gracefully.")
    if max_days_cap:
        print(f"💡 TIP: Capped Date logic writing to {os.path.basename(skipped_file_path)}")
    print("-" * 50 + "\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in reader:
            chunk.columns = chunk.columns.str.lower().str.strip()
            
            # Expand rows (or pass them through if no range)
            chunk_data = process_chunk(chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path)
            
            valid_to_process = []
            for r in chunk_data:
                ip_addr = r.get('IP')
                if not ip_addr or str(ip_addr).lower() == 'nan':
                    continue
                
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
    print(f"Total API Hits Evaluated: {stats['processed']}")
    print(f"Already Processed (Skip): {stats['skipped']}")
    print(f"Successfully Enriched:    {stats['success']}")
    print(f"Failed Lookups:           {stats['failed']}")
    print("-" * 50)
    print(f"Successes saved to: {output_file_path}")
    if stats['failed'] > 0:
        print(f"Failures saved to:  {failed_file_path}")
    if max_days_cap:
        print(f"Audit Log saved to: {skipped_file_path}")

    print(f"\nTotal runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
