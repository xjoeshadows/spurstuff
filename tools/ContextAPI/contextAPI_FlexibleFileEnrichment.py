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

def count_input_rows(file_path, is_csv):
    """Counts data rows without loading every column into memory."""
    if is_csv:
        return sum(len(chunk) for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE, usecols=[0]))
    return len(pd.read_excel(file_path))

def scan_file_date_range(file_path, is_csv, start_col, end_col):
    """Reads the entire file and returns (earliest, latest, total_rows). Dates are None if none parse."""
    earliest = latest = None
    total_rows = 0

    if is_csv:
        reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
    else:
        reader = [pd.read_excel(file_path)]

    for chunk in reader:
        chunk.columns = chunk.columns.str.lower().str.strip()
        total_rows += len(chunk)
        for col in {start_col, end_col}:
            for val in chunk[col]:
                dt = parse_to_datetime(val)
                if dt is None:
                    continue
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                if earliest is None or dt < earliest:
                    earliest = dt
                if latest is None or dt > latest:
                    latest = dt

    return earliest, latest, total_rows

def propose_span(file_scan):
    """Turns a scan result into a (start, end) pair of midnight datetimes to suggest, or None. Never proposes the future."""
    if not file_scan or file_scan[0] is None:
        return None
    midnight = datetime.min.time()
    earliest = datetime.combine(file_scan[0].date(), midnight)
    latest = min(datetime.combine(file_scan[1].date(), midnight), datetime.combine(datetime.now().date(), midnight))
    return (earliest, latest) if earliest <= latest else None

def prompt_yyyymmdd(prompt, allow_blank=False):
    """Asks for a date as YYYYMMDD. Returns a datetime at midnight, or None for a blank answer when allowed."""
    while True:
        raw = input(prompt).strip()
        if not raw and allow_blank:
            return None
        if len(raw) == 8 and raw.isdigit():
            try:
                dt = datetime.strptime(raw, '%Y%m%d')
            except ValueError:
                dt = None
            if dt is not None:
                if dt.date() > datetime.now().date():
                    print("That date is in the future. Please enter today or an earlier date.")
                    continue
                return dt
        print("Please enter a valid date as YYYYMMDD (e.g., 20260101).")

def prompt_static_dates(span):
    """Asks for a single date or a date range. Offers the input file's own date span when there is one."""
    if span:
        print(f"\nTimestamps in your input file span {span[0]:%Y%m%d} to {span[1]:%Y%m%d}.")
        print("  [1] Use that range")
        print("  [2] Enter a different date or range")
        while True:
            span_choice = input("Select an option (1 or 2): ").strip()
            if span_choice in ['1', '2']: break
            print("Invalid choice.")
        if span_choice == '1':
            return span

    start_dt = prompt_yyyymmdd("Enter the date (or the start of the range) as YYYYMMDD: ")
    end_dt = prompt_yyyymmdd("Enter the end date as YYYYMMDD (or press Enter for a single date): ", allow_blank=True)
    if end_dt is None:
        end_dt = start_dt
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
    return start_dt, end_dt

def format_original_timestamp(s_val, e_val):
    """The input file's own date value(s), kept for reference. Timestamp holds the date actually queried."""
    def clean(val):
        if val is None or pd.isna(val):
            return None
        return str(val).strip() or None

    s_str, e_str = clean(s_val), clean(e_val)
    if not s_str and not e_str:
        return None
    if not e_str or s_str == e_str:
        return s_str
    if not s_str:
        return e_str
    return f"{s_str} - {e_str}"

def process_chunk(df_chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior=None, run_date_str=None, static_dates=None):
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

        if static_dates:
            # Same static date(s) for every IP; the file's own timestamps (if any) are kept for reference only.
            s_val = row_dict.pop(start_col, None) if start_col else None
            e_val = row_dict.pop(end_col, None) if end_col and end_col != start_col else s_val
            original = format_original_timestamp(s_val, e_val)
            if original:
                row_dict['Original_Timestamp'] = original
            for date_str in static_dates:
                new_row = row_dict.copy()
                new_row['Timestamp'] = date_str
                if len(static_dates) == 1:
                    new_row['_AllowBuffer'] = True
                all_rows_data.append(new_row)
            continue

        if not perform_historic_lookup or not start_col:
            row_dict.pop(start_col, None)
            row_dict.pop(end_col, None)
            all_rows_data.append(row_dict)
            continue

        s_val = row_dict.pop(start_col, None)
        e_val = row_dict.pop(end_col, None) if end_col != start_col else s_val
        original = format_original_timestamp(s_val, e_val)
        if original:
            row_dict['Original_Timestamp'] = original
        
        s_dt = parse_to_datetime(s_val)
        e_dt = parse_to_datetime(e_val)

        # Fallback if parsing completely fails
        if not s_dt and not e_dt:
            if missing_timestamp_behavior == 'no_data':
                row_dict['Timestamp'] = None
                row_dict['_NoDataMissingTimestamp'] = True
            else:
                # 'current_lookup' (or unset default): drop the dt param and be
                # truthful in the output about what date the data reflects.
                row_dict['Timestamp'] = run_date_str
                row_dict['_ForceCurrentLookup'] = True
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
            if total_days == 1:
                new_row['_AllowBuffer'] = True
            all_rows_data.append(new_row)

    return all_rows_data

def buffer_candidates(timestamp, buffer_days):
    """Yields dates (YYYYMMDD) around `timestamp`: nearest first, earlier before later, never in the future."""
    try:
        base = datetime.strptime(str(timestamp), '%Y%m%d')
    except ValueError:
        return
    today = datetime.now().date()
    for offset in range(1, buffer_days + 1):
        for candidate in (base - timedelta(days=offset), base + timedelta(days=offset)):
            if candidate.date() <= today:
                yield candidate.strftime('%Y%m%d')

def query_api(ip_address, timestamp, api_token, use_maxmind_geo):
    """One Context API lookup, with retries. Returns (status, payload):
    ('ok', json) on success, ('nodata', reason) on a 404, ('error', reason) otherwise."""
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
            return ('error', "Canceled during retry (Graceful Shutdown)")

        try:
            response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 404:
                return ('nodata', "404 Not Found (No Data)")

            response.raise_for_status()
            json_response = response.json()

            if attempt > 0:
                print(f"  [+] IP {ip_address} successfully enriched after {attempt} retry(s).")

            return ('ok', json_response)

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
                if attempt >= MAX_RETRIES:
                    print(f"  [-] IP {ip_address} permanently failed.")
                return ('error', f"{fail_prefix}: {error_desc}")
        except Exception as e:
            print(f"  [-] IP {ip_address} crashed unexpectedly: {str(e)}")
            return ('error', f"Unexpected Error: {str(e)}")

def enrich_ip(row_data, api_token, perform_historic_lookup, use_maxmind_geo, buffer_days=0, current_fallback_date=None):
    force_current_lookup = row_data.pop('_ForceCurrentLookup', False)
    no_data_missing_ts = row_data.pop('_NoDataMissingTimestamp', False)
    allow_buffer = row_data.pop('_AllowBuffer', False)

    if SHUTDOWN_EVENT.is_set():
        row_data['Error_Reason'] = "Canceled (Graceful Shutdown)"
        return (False, row_data)

    ip_address = row_data.get('IP')

    if not ip_address or str(ip_address).lower() == 'nan':
        return (False, {**row_data, 'Error_Reason': 'Missing or Invalid IP'})

    if no_data_missing_ts:
        return (False, {**row_data, 'Error_Reason': 'Missing Timestamp (No Data per user selection)'})

    timestamp = row_data.get('Timestamp') if (perform_historic_lookup and not force_current_lookup) else None

    status, payload = query_api(ip_address, timestamp, api_token, use_maxmind_geo)

    # No data for the requested date: try the surrounding dates, stopping at the first hit.
    if status == 'nodata' and timestamp and allow_buffer and buffer_days > 0:
        found = False
        last_error = None
        for candidate in buffer_candidates(timestamp, buffer_days):
            c_status, c_payload = query_api(ip_address, candidate, api_token, use_maxmind_geo)
            if c_status == 'ok':
                # Timestamp is the date that produced the data; remember what was originally asked for (used by resume).
                row_data['Requested_Timestamp'] = timestamp
                row_data['Timestamp'] = candidate
                status, payload = c_status, c_payload
                found = True
                break
            if c_status == 'error':
                last_error = f"{c_payload} (on buffer date {candidate})"
                if SHUTDOWN_EVENT.is_set(): break

        if not found:
            row_data['Buffer_Days_Tried'] = buffer_days
            if last_error:
                status, payload = 'error', last_error
            else:
                payload = f"404 Not Found (No Data within {buffer_days}-day buffer)"

    # No data for the requested date: fall back to a current lookup, recorded under today's date.
    elif status == 'nodata' and timestamp and allow_buffer and current_fallback_date:
        c_status, c_payload = query_api(ip_address, None, api_token, use_maxmind_geo)
        if c_status == 'ok':
            row_data['Requested_Timestamp'] = timestamp
            row_data['Timestamp'] = current_fallback_date
            status, payload = c_status, c_payload
        elif c_status == 'error':
            status, payload = 'error', f"{c_payload} (on current-lookup fallback)"
        else:
            payload = "404 Not Found (No Data, including current-lookup fallback)"

    if status == 'ok':
        merged_data = {**row_data, **payload}
        merged_data.pop('IP', None)
        return (True, merged_data)

    row_data['Error_Reason'] = payload
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
                    if 'Requested_Timestamp' in result_data:
                        stats_ref['buffered'] += 1
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
    run_date_str = datetime.now().strftime('%Y%m%d')

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
    prev_dates = set()
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
                                # Buffered hits keep the originally requested date in Requested_Timestamp
                                ts = record.get('Requested_Timestamp') or record.get('Timestamp')
                                if ip:
                                    processed_keys.add(get_composite_key(ip, ts))
                                    if ts and str(ts).isdigit() and len(str(ts)) == 8:
                                        prev_dates.add(str(ts))
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
    missing_timestamp_behavior = None
    buffer_days = 0
    static_dates = None
    current_fallback_date = None
    file_scan = None  # (earliest, latest, total_rows) from scan_file_date_range, filled in lazily

    print("-" * 50)
    if resume_mode:
        print("⚠️  RESUME NOTICE")
        print("Already-processed records are matched on IP + queried date. Choose the SAME date option")
        print("as the original session, otherwise those IPs will be queried again.")
        if prev_dates:
            print(f"Dates found in the previous output: {min(prev_dates)} to {max(prev_dates)}")
        if start_col:
            print("\n[Scanning] Reading input file timestamps...")
            file_scan = scan_file_date_range(input_file_path, is_csv_file, start_col, end_col)
            if file_scan[0] is not None:
                print(f"Timestamps in the input file span {file_scan[0]:%Y%m%d} to {file_scan[1]:%Y%m%d}.")
                print("To match an original run, use the file's timestamps or a static date/range (e.g., that span).")
        print("-" * 50)

    if start_col:
        col_display = f"'{start_col}' & '{end_col}'" if start_col != end_col else f"'{start_col}'"
        print(f"Date column(s) detected: {col_display}.")
        print("How should dates be chosen for the lookups?")
        print("  [1] Use the timestamps from the input file")
        print("  [2] Set a static date or date range for every IP")
        print("  [3] No historical lookup (current, real-time data)")
        date_modes = {'1': 'file', '2': 'static', '3': 'current'}
    else:
        print("No date column detected in the input file.")
        print("  [1] No historical lookup (current, real-time data)")
        print("  [2] Set a static date or date range for every IP")
        date_modes = {'1': 'current', '2': 'static'}

    while True:
        date_choice = input(f"Select an option ({', '.join(date_modes)}): ").strip()
        if date_choice in date_modes: break
        print("Invalid choice.")
    date_mode = date_modes[date_choice]
    print("-" * 50)

    if date_mode == 'static':
        perform_historic_lookup = True
        if start_col and file_scan is None:
            print("\n[Scanning] Reading input file timestamps to propose a range...")
            file_scan = scan_file_date_range(input_file_path, is_csv_file, start_col, end_col)
        total_rows = file_scan[2] if file_scan else count_input_rows(input_file_path, is_csv_file)
        span = propose_span(file_scan)

        while True:
            start_dt, end_dt = prompt_static_dates(span)
            num_days = (end_dt - start_dt).days + 1
            print(f"\n{start_dt:%Y%m%d} to {end_dt:%Y%m%d} = {num_days} day(s) per IP.")
            print(f"Up to {total_rows:,} rows x {num_days} day(s) = ~{total_rows * num_days:,} API requests.")
            if input("Proceed with these dates? (yes/no): ").strip().lower() in ['yes', 'y']:
                break
        static_dates = [(start_dt + timedelta(days=i)).strftime('%Y%m%d') for i in range(num_days)]

        if num_days == 1:
            print("\n--- No-Data Handling ---")
            print(f"If the lookup for {static_dates[0]} returns no data for an IP, what should happen?")
            print("  [1] Add the IP to the NoEnrichment file")
            print(f"  [2] Do a current (real-time) lookup instead, and record today's date ({run_date_str}) as the timestamp")
            print("  [3] Try nearby dates (buffer): e.g., 1 day on 20260101 also tries 20251231 and 20260102")

            while True:
                no_data_choice = input("Select an option (1, 2, or 3): ").strip()
                if no_data_choice in ['1', '2', '3']: break
                print("Invalid choice.")

            if no_data_choice == '2':
                current_fallback_date = run_date_str
            elif no_data_choice == '3':
                print("Each buffered attempt is an additional API request.")
                while True:
                    try:
                        buffer_days = int(input("Buffer days to try on each side (e.g., 1): ").strip())
                        if buffer_days > 0: break
                        print("Must be a positive number.")
                    except ValueError:
                        print("Please enter a valid number.")

        print("-" * 50)

    elif date_mode == 'file':
        perform_historic_lookup = True

        print("\n--- Missing Timestamp Handling ---")
        print("If any rows turn out to be missing a usable date/timestamp value, how should they be handled?")
        print("  [1] Return no data for those IPs (skip the lookup entirely)")
        print(f"  [2] Do a current (real-time) lookup instead, and record today's date ({run_date_str}) as their timestamp")

        while True:
            missing_ts_choice = input("Select an option (1 or 2): ").strip()
            if missing_ts_choice in ['1', '2']:
                missing_timestamp_behavior = 'no_data' if missing_ts_choice == '1' else 'current_lookup'
                break
            print("Invalid choice.")
        print("-" * 50)

        print("\n--- No-Data Date Buffer ---")
        print("If a lookup returns no data for a row's date, nearby dates can be tried before giving up.")
        print("Example: a buffer of 1 on 20260101 also tries 20251231 and 20260102 (first date with data wins).")
        if start_col != end_col:
            print("Only rows with a single date use the buffer (Start/End ranges already cover multiple days).")
        print("Each buffered attempt is an additional API request.")

        while True:
            try:
                buffer_days = int(input("Buffer days to try on each side (0 = no buffer): ").strip())
                if buffer_days >= 0: break
                print("Must be 0 or a positive number.")
            except ValueError:
                print("Please enter a valid number.")
        print("-" * 50)

        if start_col != end_col:
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

    stats = {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'buffered': 0}

    print("\n" + "-"*50)
    print("💡 TIP: Press Ctrl+Z to PAUSE, type 'fg' to RESUME.")
    print("💡 TIP: Press Ctrl+C to SAVE & QUIT gracefully.")
    if max_days_cap:
        print(f"💡 TIP: Capped Date logic writing to {os.path.basename(skipped_file_path)}")
    if missing_timestamp_behavior == 'no_data':
        print("💡 TIP: Rows missing a timestamp will return No Data.")
    elif missing_timestamp_behavior == 'current_lookup':
        print(f"💡 TIP: Rows missing a timestamp will get a current lookup, dated {run_date_str}.")
    if static_dates:
        range_str = static_dates[0] if len(static_dates) == 1 else f"{static_dates[0]} to {static_dates[-1]}"
        print(f"💡 TIP: Every IP will be looked up for {range_str} ({len(static_dates)} day(s) each).")
    if buffer_days:
        print(f"💡 TIP: Single-date rows with no data will retry within +/-{buffer_days} day(s).")
    if current_fallback_date:
        print(f"💡 TIP: IPs with no data for {static_dates[0]} will get a current lookup, dated {current_fallback_date}.")
    print("-" * 50 + "\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in reader:
            chunk.columns = chunk.columns.str.lower().str.strip()
            
            # Expand rows (or pass them through if no range)
            chunk_data = process_chunk(chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior, run_date_str, static_dates)
            
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
            
            results = executor.map(lambda r: enrich_ip(r, api_token, perform_historic_lookup, use_maxmind_geo, buffer_days, current_fallback_date), valid_to_process)
            write_to_json_stream(results, output_file_path, failed_file_path, stats, start_main_time)
            
            if SHUTDOWN_EVENT.is_set():
                break
            
    print("\n" + "="*50)
    print("COMPLETED / SAVED".center(50))
    print("="*50)
    print(f"Total API Hits Evaluated: {stats['processed']}")
    print(f"Already Processed (Skip): {stats['skipped']}")
    print(f"Successfully Enriched:    {stats['success']}")
    if buffer_days or current_fallback_date:
        print(f"  (via buffer/fallback):  {stats['buffered']}")
    print(f"Failed Lookups:           {stats['failed']}")
    print("-" * 50)
    print(f"Successes saved to: {output_file_path}")
    if stats['failed'] > 0:
        print(f"Failures saved to:  {failed_file_path}")
    if max_days_cap:
        print(f"Audit Log saved to: {skipped_file_path}")

    print(f"\nTotal runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
