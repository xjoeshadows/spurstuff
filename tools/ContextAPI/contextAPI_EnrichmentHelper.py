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

# --- Date Parsing Helpers ---
def parse_single_date(val):
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
        '%a, %b %d, %Y %I:%M %p %Z', '%Y-%m-%dT%H:%M:%S'
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

def extract_dates_from_input(input_val):
    """
    Parses user input or spreadsheet cell.
    Extracts all discrete dates from arrays and deduplicates them by day.
    """
    if pd.isna(input_val):
        return []

    # If it's already a native datetime object from pandas, bypass string splitting
    if isinstance(input_val, (pd.Timestamp, datetime)):
        return [input_val.strftime('%Y%m%d')]

    input_str = str(input_val).strip()
    if not input_str or input_str.lower() == 'nan':
        return []

    # Remove surrounding brackets if they exist
    if input_str.startswith('[') and input_str.endswith(']'):
        input_str = input_str[1:-1]

    # Split the string by commas
    parts = [p.strip() for p in input_str.split(',')]
    
    # Use a set to automatically deduplicate multiple timestamps on the same day
    unique_dates = set()
    
    for p in parts:
        # Clean up any stray quotes inside the array items
        p = p.replace('"', '').replace("'", "")
        if not p:
            continue
            
        dt = parse_single_date(p)
        if dt:
            # Convert to YYYYMMDD and add to the set
            unique_dates.add(dt.strftime('%Y%m%d'))
            
    return list(unique_dates)

# --- Core Functions ---
def enrich_ip_historic(task, api_token):
    row_data, ip_address, target_date = task
    date_display = target_date if target_date else "Current"
    
    result_row = dict(row_data)
    result_row['Queried_Date'] = date_display
    
    if SHUTDOWN_EVENT.is_set():
        result_row['Error_Reason'] = "Canceled (Graceful Shutdown)"
        return (False, result_row)

    if not ip_address or str(ip_address).lower() == 'nan':
        result_row['Error_Reason'] = 'Missing or Invalid IP'
        return (False, result_row)

    url = f"{api_url_base}{ip_address}"
    if target_date:
        url += f"?dt={target_date}"
        
    headers = {'TOKEN': api_token}

    for attempt in range(MAX_RETRIES + 1):
        if SHUTDOWN_EVENT.is_set():
            result_row['Error_Reason'] = "Canceled during retry (Graceful Shutdown)"
            return (False, result_row)

        try:
            response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 404:
                result_row['Error_Reason'] = f"404 Not Found (No Data for {date_display})"
                return (False, result_row)
                
            response.raise_for_status()
            json_response = response.json()
            
            if attempt > 0:
                print(f"  [+] IP {ip_address} ({date_display}) successfully enriched after {attempt} retry(s).")
                
            return (True, {**result_row, **json_response})
            
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
                print(f"  [!] Error on {ip_address} ({date_display}) [{error_desc}]. Backing off {backoff_time}s (Attempt {attempt + 1}/{MAX_RETRIES})...")
                
                for _ in range(backoff_time):
                    if SHUTDOWN_EVENT.is_set():
                        break
                    time.sleep(1)
            else:
                fail_prefix = f"Failed after {MAX_RETRIES} retries" if is_retryable else "Failed (Non-retryable)"
                result_row['Error_Reason'] = f"{fail_prefix}: {error_desc}"
                
                if attempt >= MAX_RETRIES:
                    print(f"  [-] IP {ip_address} ({date_display}) permanently failed after max retries.")
                    
                return (False, result_row)
        except Exception as e:
            result_row['Error_Reason'] = f"Unexpected Error: {str(e)}"
            print(f"  [-] IP {ip_address} crashed unexpectedly: {str(e)}")
            return (False, result_row)

def write_to_json_stream(results_iterator, output_path, failed_path, stats_ref, start_time):
    last_update_time = time.time()
    try:
        with open(output_path, 'a', encoding='utf-8') as outfile, \
             open(failed_path, 'a', encoding='utf-8') as failfile:
             
            for success, result_data in results_iterator:
                if success:
                    result_data.pop('Error_Reason', None)
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
                    print(f"  Processed {stats_ref['processed']} queries ({stats_ref['success']} ok, {stats_ref['failed']} fail) - {rps:.2f} r/s")
                    last_update_time = current_time
    except Exception as e:
        print(f"\nError writing to file: {e}", file=sys.stderr)
        sys.exit(1)

def find_spreadsheet_columns(df):
    original_columns = df.columns
    normalized_columns = original_columns.str.lower().str.strip()
    
    ip_col, ts_col = None, None

    for i, col in enumerate(normalized_columns):
        if ('ip address' in col or 'ips' in col or 'ip' in col) and ip_col is None:
            ip_col = original_columns[i]
        if ('timestamp' in col or 'date' in col or 'time' in col) and ts_col is None:
            ts_col = original_columns[i]

    if ip_col is None or ts_col is None:
        raise ValueError("Spreadsheet must contain a column for IP and a column for Date/Timestamp.")
    
    return ip_col, ts_col

# --- Main Script ---
if __name__ == "__main__":
    start_main_time = time.time()
    signal.signal(signal.SIGINT, sigint_handler)

    api_token = os.environ.get("TOKEN")
    if not api_token:
        api_token = input("Please enter your Spur API token: ").strip()
        if not api_token:
            print("No token provided. Exiting.", file=sys.stderr)
            sys.exit(1)
        os.environ['TOKEN'] = api_token

    # Check for argument, otherwise prompt
    no_enrich_file = sys.argv[1] if len(sys.argv) > 1 else None

    if no_enrich_file is None or not os.path.exists(no_enrich_file):
        print("\n--- Load Failed Enrichments ---")
        while True:
            no_enrich_file = input("Enter the path to the 'NoEnrichment' JSON file: ").strip()
            if os.path.exists(no_enrich_file) and no_enrich_file.endswith('.json'):
                break
            print("Error: Valid JSON file not found.")
    else:
        print(f"\n--- Loading Failed Enrichments from Argument: {no_enrich_file} ---")

    print("\nReading JSON lines into memory...")
    failed_records = []
    with open(no_enrich_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if line.strip():
                try:
                    failed_records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
            
            if (i + 1) % 25000 == 0:
                print(f"  Read {i + 1} lines...")

    print(f"Loaded {len(failed_records)} records from {no_enrich_file}.")
    if not failed_records:
        print("No records to process. Exiting.")
        sys.exit(0)

    # Determine Date Input Method
    print("\n" + "-" * 50)
    print("How would you like to provide the date(s) for lookup?")
    print("  1: Terminal Input (Apply current data or specific dates to ALL records)")
    print("  2: Spreadsheet Mapping (Map specific dates to specific IPs via an external file)")
    print("  3: Global Range Discovery (Detect global range from ORIGINAL spreadsheet and apply to ALL failed IPs)")
    print("  4: Per-IP Proximity Buffer (Dynamically query a date range surrounding each IP's recorded timestamp)")
    print("-" * 50)
    
    mode = input("Select mode (1, 2, 3, or 4): ").strip()
    
    tasks = [] # List of tuples: (row_data, ip, YYYYMMDD or None)

    if mode == '1':
        print("\n" + "-" * 50)
        print("Select Lookup Type:")
        print("  A: Current Data (Live IP lookup, no historical dates)")
        print("  B: Historical Data (Provide a discrete date or array of dates)")
        print("-" * 50)
        
        sub_mode = input("Select choice (A or B): ").strip().upper()
        
        if sub_mode == 'A':
            print("\nQueuing Current Data lookups for all records...")
            for i, record in enumerate(failed_records):
                ip = record.get('IP')
                if ip:
                    tasks.append((record, ip, None))
                if (i + 1) % 50000 == 0:
                    print(f"  Queued {i + 1} records...")
                    
        elif sub_mode == 'B':
            print("\nFORMAT EXAMPLES:")
            print("  Single Date: 2026-02-06")
            print("  Array:       [2026-02-06, 2026-02-08, 2026-02-10]")
            
            date_input = input("\nEnter your date configuration: ").strip()
            global_dates = extract_dates_from_input(date_input)
            
            if not global_dates:
                print("Error: Could not parse any valid dates from input.")
                sys.exit(1)
                
            print(f"Parsed {len(global_dates)} unique date(s) to apply to all records.")
            
            print("\nBuilding task queue...")
            for i, record in enumerate(failed_records):
                ip = record.get('IP')
                if ip:
                    for dt in global_dates:
                        tasks.append((record, ip, dt))
                        
                if (i + 1) % 50000 == 0:
                    print(f"  Queued {i + 1} records...")
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)
                    
    elif mode == '2':
        while True:
            mapping_file = input("\nEnter the path to your mapping Spreadsheet (CSV/XLSX): ").strip()
            if os.path.exists(mapping_file):
                break
            print("Error: File not found.")
            
        print("\nLoading spreadsheet into Pandas...")
        if mapping_file.lower().endswith('.csv'):
            df = pd.read_csv(mapping_file)
        else:
            df = pd.read_excel(mapping_file)
            
        ip_col, ts_col = find_spreadsheet_columns(df)
        print(f"Mapped IP column: '{ip_col}' | Date column: '{ts_col}'")
        
        print("\nParsing spreadsheet dates (this may take a moment for large files)...")
        ip_date_map = {}
        total_rows = len(df)
        for i, row in df.iterrows():
            if (i + 1) % 5000 == 0:
                print(f"  Parsed {i + 1}/{total_rows} spreadsheet rows...")
                
            ip_val = str(row[ip_col]).strip()
            ts_val = row[ts_col]
            dates = extract_dates_from_input(ts_val)
            if dates:
                ip_date_map[ip_val] = dates
                
        print("\nCross-referencing failed IPs with spreadsheet mappings...")
        total_failed = len(failed_records)
        for i, record in enumerate(failed_records):
            if (i + 1) % 10000 == 0:
                print(f"  Cross-referenced {i + 1}/{total_failed} records...")
                
            ip = record.get('IP')
            if ip and ip in ip_date_map:
                for dt in ip_date_map[ip]:
                    tasks.append((record, ip, dt))
                    
        print(f"\nMatched IPs generated {len(tasks)} specific date lookups.")

    elif mode == '3':
        while True:
            original_file = input("\nEnter the path to your ORIGINAL input Spreadsheet (CSV/XLSX): ").strip()
            if os.path.exists(original_file):
                break
            print("Error: File not found.")
            
        print("\nLoading spreadsheet into Pandas...")
        if original_file.lower().endswith('.csv'):
            df = pd.read_csv(original_file)
        else:
            df = pd.read_excel(original_file)
            
        ip_col, ts_col = find_spreadsheet_columns(df)
        print(f"Mapped Date column: '{ts_col}'")
        
        print("\nScanning spreadsheet to discover the global date range...")
        all_dates = set()
        total_rows = len(df)
        for i, row in df.iterrows():
            if (i + 1) % 5000 == 0:
                print(f"  Scanned {i + 1}/{total_rows} rows...")
            ts_val = row[ts_col]
            dates = extract_dates_from_input(ts_val)
            all_dates.update(dates)

        if not all_dates:
            print("\n[!] Error: Could not find any valid dates in the original file to calculate a range.")
            sys.exit(1)
            
        dt_objects = [datetime.strptime(d, '%Y%m%d') for d in all_dates]
        min_date = min(dt_objects)
        max_date = max(dt_objects)
        
        print("\n" + "="*50)
        print("DISCOVERED GLOBAL DATE RANGE".center(50))
        print("="*50)
        print(f"Earliest Date: {min_date.strftime('%Y-%m-%d')}")
        print(f"Latest Date:   {max_date.strftime('%Y-%m-%d')}")
        print(f"Total Days:    {(max_date - min_date).days + 1} day(s)")
        print("-" * 50)
        
        print("\nWould you like to query EVERY DAY in this contiguous range for ALL failed IPs?")
        print("  1: Yes, use this discovered contiguous range")
        print("  2: No, let me enter a custom contiguous date range")
        range_choice = input("\nSelect choice (1 or 2): ").strip()
        
        if range_choice == '1':
            global_dates = pd.date_range(start=min_date, end=max_date).strftime('%Y%m%d').tolist()
        elif range_choice == '2':
            start_input = input("\nEnter Start Date (e.g., 2026-02-01): ").strip()
            end_input = input("Enter End Date (e.g., 2026-02-28): ").strip()
            
            start_dt = parse_single_date(start_input)
            end_dt = parse_single_date(end_input)
            
            if not start_dt or not end_dt:
                print("Error: Could not parse your start or end date.")
                sys.exit(1)
                
            if start_dt > end_dt:
                start_dt, end_dt = end_dt, start_dt
                
            global_dates = pd.date_range(start=start_dt, end=end_dt).strftime('%Y%m%d').tolist()
        else:
            print("Invalid choice. Exiting.")
            sys.exit(1)
            
        print(f"\nGenerated {len(global_dates)} sequential day(s) to query.")
        
        print("Building task queue...")
        for i, record in enumerate(failed_records):
            ip = record.get('IP')
            if ip:
                for dt in global_dates:
                    tasks.append((record, ip, dt))
                    
            if (i + 1) % 50000 == 0:
                print(f"  Queued {i + 1} records...")

    elif mode == '4':
        print("\n" + "-" * 50)
        print("Enter the number of buffer days to apply around each IP's recorded timestamp(s).")
        print("Example: A buffer of '1' will query the exact timestamp date, 1 day prior, and 1 day after.")
        print("-" * 50)
        
        while True:
            try:
                buffer_days = int(input("\nEnter buffer days (e.g., 1, 3, 7): ").strip())
                if buffer_days >= 0:
                    break
            except ValueError:
                print("Please enter a valid whole number.")
                
        print("\nCalculating buffered dates and building task queue...")
        missing_dates = 0
        
        for i, record in enumerate(failed_records):
            ip = record.get('IP')
            if not ip:
                continue
                
            # Scan the record for date-related columns
            record_dates = set()
            for key in ['Timestamp', 'Queried_Date', 'date', 'time']:
                # Do case-insensitive check of keys just in case
                actual_key = next((k for k in record.keys() if key.lower() in k.lower()), None)
                if actual_key and record[actual_key]:
                    val = str(record[actual_key]).strip()
                    if val.lower() not in ['nan', 'none', 'null']:
                        extracted = extract_dates_from_input(val)
                        if extracted:
                            for d_str in extracted:
                                dt_obj = datetime.strptime(d_str, '%Y%m%d')
                                # Apply the sliding window buffer
                                for offset in range(-buffer_days, buffer_days + 1):
                                    buffered_dt = dt_obj + timedelta(days=offset)
                                    record_dates.add(buffered_dt.strftime('%Y%m%d'))
                            break # Found the dates, stop checking other columns
                            
            if not record_dates:
                missing_dates += 1
                continue
                
            for dt in record_dates:
                tasks.append((record, ip, dt))
                
            if (i + 1) % 10000 == 0:
                print(f"  Processed {i + 1} records...")
                
        print(f"\nGenerated {len(tasks)} specific buffered date lookups.")
        if missing_dates > 0:
            print(f"[!] Warning: {missing_dates} records were skipped because they lacked a valid, parsable timestamp in the NoEnrichment file.")

    else:
        print("Invalid selection. Exiting.")
        sys.exit(1)

    if not tasks:
        print("\nNo lookup tasks generated. Exiting.")
        sys.exit(0)

    # Output file paths
    output_dir = os.path.dirname(os.path.abspath(no_enrich_file))
    input_file_name_without_ext = os.path.splitext(os.path.basename(no_enrich_file))[0]
    
    default_out = f"{input_file_name_without_ext}_HelperSuccess.json"
    out_input = input(f"\nEnter success output file name, or press Enter for default ({default_out}): ").strip()
    output_file_path = os.path.join(output_dir, out_input if out_input else default_out)
    
    failed_file_path = os.path.join(output_dir, f"{input_file_name_without_ext}_HelperFailed.json")

    if os.path.exists(output_file_path): open(output_file_path, 'w').close()
    if os.path.exists(failed_file_path): open(failed_file_path, 'w').close()

    stats = {'processed': 0, 'success': 0, 'failed': 0}

    print("\n" + "-"*50)
    print(f"Starting {len(tasks)} lookups...")
    print("💡 TIP: Press Ctrl+Z to PAUSE, type 'fg' to RESUME.")
    print("💡 TIP: Press Ctrl+C to SAVE & QUIT gracefully.")
    print("-" * 50 + "\n")

    execution_start_time = time.time() 

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(lambda t: enrich_ip_historic(t, api_token), tasks)
        write_to_json_stream(results, output_file_path, failed_file_path, stats, execution_start_time)

    print("\n" + "="*50)
    print("COMPLETED / SAVED".center(50))
    print("="*50)
    print(f"Total Queries Processed: {stats['processed']}")
    print(f"Successfully Enriched:   {stats['success']}")
    print(f"Failed Lookups:          {stats['failed']}")
    print("-" * 50)
    print(f"Successes saved to: {output_file_path}")
    if stats['failed'] > 0:
        print(f"Failures saved to:  {failed_file_path}")

    print(f"\nTotal runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
