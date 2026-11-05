#!/usr/bin/env python3
import requests
import ipaddress
import json
import sys
import os
import pandas as pd
from collections import Counter
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
def get_composite_key(ip, timestamp, input_row=None):
    """Resume key. Includes the input row when known so rows sharing an IP and date (e.g., different transaction IDs) stay distinct."""
    ip_str = str(ip).strip().lower()
    ts_str = str(timestamp).strip() if pd.notna(timestamp) and timestamp else "none"
    key = f"{ip_str}|{ts_str}"
    return f"{key}|{input_row}" if input_row is not None else key

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

def parse_network(text):
    """Returns an ip_network for CIDR text such as '10.0.0.0/24' (IPv4 or IPv6), or None if it isn't valid."""
    try:
        return ipaddress.ip_network(text, strict=False)
    except ValueError:
        return None

def resolve_date_range(s_val, e_val):
    """Parses a start/end pair into (start, end) datetimes, filling in a missing side and swapping a backwards pair.
    Returns None when neither value is usable."""
    s_dt = parse_to_datetime(s_val)
    e_dt = parse_to_datetime(e_val)

    if not s_dt and not e_dt:
        return None
    s_dt = s_dt or e_dt
    e_dt = e_dt or s_dt

    # Compare as naive datetimes so a mix of timezone-aware and naive values can't raise
    s_dt, e_dt = (dt.replace(tzinfo=None) if dt.tzinfo is not None else dt for dt in (s_dt, e_dt))
    if s_dt > e_dt:
        s_dt, e_dt = e_dt, s_dt
    return s_dt, e_dt

def scan_input_file(file_path, is_csv, ip_col, start_col, end_col):
    """One pass over the whole input file to size the job before any API calls are made.
    Counts rows, CIDR ranges and individual IPs, and how many IPs fall on each days-per-IP value
    (0 = no usable date) so the request count can be worked out for any date option afterwards."""
    scan = {'rows': 0, 'blank_ip_rows': 0, 'invalid_cidr_rows': 0, 'cidr_rows': 0, 'ips': 0,
            'largest_cidr': None, 'ips_by_days': Counter(), 'earliest': None, 'latest': None}

    reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE) if is_csv else [pd.read_excel(file_path)]

    for chunk in reader:
        chunk.columns = chunk.columns.str.lower().str.strip()
        starts = chunk[start_col] if start_col else [None] * len(chunk)
        ends = chunk[end_col] if end_col else starts

        for ip_val, s_val, e_val in zip(chunk[ip_col], starts, ends):
            scan['rows'] += 1
            ip_text = '' if pd.isna(ip_val) else str(ip_val).strip()
            if not ip_text or ip_text.lower() == 'nan':
                scan['blank_ip_rows'] += 1
                continue

            ip_count = 1
            if '/' in ip_text:
                network = parse_network(ip_text)
                if network is None:
                    scan['invalid_cidr_rows'] += 1
                    continue
                ip_count = network.num_addresses
                scan['cidr_rows'] += 1
                if scan['largest_cidr'] is None or ip_count > scan['largest_cidr'][1]:
                    scan['largest_cidr'] = (ip_text, ip_count)

            days = 1
            if start_col:
                date_range = resolve_date_range(s_val, e_val)
                if date_range is None:
                    days = 0
                else:
                    days = (date_range[1].date() - date_range[0].date()).days + 1
                    if scan['earliest'] is None or date_range[0] < scan['earliest']:
                        scan['earliest'] = date_range[0]
                    if scan['latest'] is None or date_range[1] > scan['latest']:
                        scan['latest'] = date_range[1]

            scan['ips'] += ip_count
            scan['ips_by_days'][days] += ip_count

    return scan

def ensure_scan(scan, file_path, is_csv, ip_col, start_col, end_col):
    """Returns the existing scan, running it first if it hasn't been done yet."""
    if scan is None:
        print("\n[Scanning] Counting IPs, CIDR ranges and dates across the entire file (may take a while for large files)...")
        scan = scan_input_file(file_path, is_csv, ip_col, start_col, end_col)
    return scan

def propose_span(scan):
    """Turns a scan into a (start, end) pair of midnight datetimes to suggest, or None. Never proposes the future."""
    if not scan or scan['earliest'] is None:
        return None
    midnight = datetime.min.time()
    earliest = datetime.combine(scan['earliest'].date(), midnight)
    latest = min(datetime.combine(scan['latest'].date(), midnight), datetime.combine(datetime.now().date(), midnight))
    return (earliest, latest) if earliest <= latest else None

def estimate_requests(scan, date_mode, static_days, max_days_cap, missing_timestamp_behavior):
    """Minimum API requests: one per IP per day. Buffer and fallback retries can add more."""
    if date_mode == 'static':
        return scan['ips'] * static_days
    if date_mode == 'current':
        return scan['ips']

    total = 0
    for days, ips in scan['ips_by_days'].items():
        if days == 0:
            total += ips if missing_timestamp_behavior == 'current_lookup' else 0
        else:
            total += ips * (min(days, max_days_cap) if max_days_cap else days)
    return total

def print_workload_summary(scan, date_mode, static_dates, max_days_cap, missing_timestamp_behavior, buffer_days, current_fallback_date, already_done=0):
    """Prints how large the job is, taking CIDR ranges and every date option into account. Returns the minimum request count."""
    static_days = len(static_dates) if static_dates else 0
    total_requests = estimate_requests(scan, date_mode, static_days, max_days_cap, missing_timestamp_behavior)
    days_by_ip = scan['ips_by_days']

    if date_mode == 'static':
        dates_desc = static_dates[0] if static_days == 1 else f"{static_dates[0]} to {static_dates[-1]} ({static_days} days)"
    elif date_mode == 'file':
        usable = [d for d in days_by_ip if d > 0]
        if usable:
            lo, hi = min(usable), max(usable)
            dates_desc = f"from the file's timestamps ({lo} day(s))" if lo == hi else f"from the file's timestamps ({lo} to {hi} days)"
            if max_days_cap and hi > max_days_cap:
                dates_desc += f", capped at {max_days_cap}"
        else:
            dates_desc = "none of the file's timestamps are usable"
    else:
        dates_desc = "none (current, real-time lookups)"

    print("\n" + "=" * 50)
    print("WORKLOAD SUMMARY".center(50))
    print("=" * 50)
    print(f"Rows in input file:        {scan['rows']:,}")
    if scan['cidr_rows']:
        name, size = scan['largest_cidr']
        print(f"  CIDR ranges:             {scan['cidr_rows']:,} (largest: {name} = {size:,} addresses)")
    if scan['blank_ip_rows']:
        print(f"  Blank IP (skipped):      {scan['blank_ip_rows']:,}")
    if scan['invalid_cidr_rows']:
        print(f"  Invalid CIDR (failed):   {scan['invalid_cidr_rows']:,}")
    print(f"Individual IPs to enrich:  {scan['ips']:,}" + (" (after expanding CIDR ranges)" if scan['cidr_rows'] else ""))
    print(f"Dates per IP:              {dates_desc}")

    unusable = days_by_ip.get(0, 0) if date_mode == 'file' else 0
    if unusable:
        action = "current lookup" if missing_timestamp_behavior == 'current_lookup' else "no lookup, returned as no data"
        print(f"IPs with no usable date:   {unusable:,} ({action})")

    print(f"Minimum API requests:      {total_requests:,}")

    # Retries only ever apply to single-date lookups
    if date_mode == 'static':
        single_date_ips = scan['ips'] if static_days == 1 else 0
    else:
        single_date_ips = days_by_ip.get(1, 0) if date_mode == 'file' else 0
    if buffer_days and single_date_ips:
        print(f"  + up to {single_date_ips * 2 * buffer_days:,} more if no IP has data for its date (+/-{buffer_days} day buffer)")
    if current_fallback_date and single_date_ips:
        print(f"  + up to {single_date_ips:,} more current lookups if no IP has data for its date")
    if already_done:
        print(f"Already processed:         {already_done:,} records from the previous files (these are skipped)")
    print("=" * 50)

    return total_requests

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

def plan_row_dates(row_dict, ip_val, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior, run_date_str, static_dates):
    """Removes the date column(s) from row_dict and returns the per-lookup field overrides for the row (one dict per day).
    Days dropped by the cap are logged to the skipped file."""
    if static_dates:
        # Same static date(s) for every IP; the file's own timestamps (if any) are kept for reference only.
        s_val = row_dict.pop(start_col, None) if start_col else None
        e_val = row_dict.pop(end_col, None) if end_col and end_col != start_col else s_val
        original = format_original_timestamp(s_val, e_val)
        if original:
            row_dict['Original_Timestamp'] = original
        variants = [{'Timestamp': date_str} for date_str in static_dates]
        if len(variants) == 1:
            variants[0]['_AllowNoDataRetry'] = True
        return variants

    if not perform_historic_lookup or not start_col:
        row_dict.pop(start_col, None)
        row_dict.pop(end_col, None)
        return [{}]

    s_val = row_dict.pop(start_col, None)
    e_val = row_dict.pop(end_col, None) if end_col != start_col else s_val
    original = format_original_timestamp(s_val, e_val)
    if original:
        row_dict['Original_Timestamp'] = original

    date_range = resolve_date_range(s_val, e_val)

    # Fallback if parsing completely fails
    if date_range is None:
        if missing_timestamp_behavior == 'no_data':
            return [{'Timestamp': None, '_NoDataMissingTimestamp': True}]
        # 'current_lookup' (or unset default): drop the dt param and be
        # truthful in the output about what date the data reflects.
        return [{'Timestamp': run_date_str, '_ForceCurrentLookup': True}]

    s_dt, e_dt = date_range
    total_days = (e_dt.date() - s_dt.date()).days + 1
    days_to_process = min(total_days, max_days_cap) if max_days_cap else total_days

    # Write to audit trail if we are dropping days
    if days_to_process < total_days and skipped_file_path:
        actual_end_dt = s_dt + timedelta(days=days_to_process - 1)
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

    # One lookup per day in the processed range
    variants = [{'Timestamp': (s_dt + timedelta(days=i)).strftime('%Y%m%d')} for i in range(days_to_process)]
    if total_days == 1:
        variants[0]['_AllowNoDataRetry'] = True
    return variants

def process_chunk(df_chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior=None, run_date_str=None, static_dates=None):
    """Lazily yields one record per IP (CIDR ranges expanded to every address) per day (date ranges expanded).
    A generator so that a huge CIDR x date expansion never has to sit in memory at once."""
    for row_idx, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        ip_val = row_dict.pop(ip_col)
        row_dict['IP'] = ip_val
        row_dict['Input_Row'] = int(row_idx) + 2  # row number in the input file (header is row 1)

        # Clean up blanks (NaN is not valid JSON) and pandas timestamps for JSON serialization
        for key, value in row_dict.items():
            if pd.isna(value):
                row_dict[key] = None
            elif isinstance(value, pd.Timestamp):
                row_dict[key] = str(value)

        ip_val = row_dict['IP']
        ip_text = '' if ip_val is None else str(ip_val).strip()
        network = None

        if '/' in ip_text:
            network = parse_network(ip_text)
            if network is None:
                row_dict.pop(start_col, None)
                row_dict.pop(end_col, None)
                row_dict['_InvalidCIDR'] = True
                yield row_dict
                continue
            row_dict['Input_CIDR'] = ip_text

        variants = plan_row_dates(row_dict, ip_val, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior, run_date_str, static_dates)

        if network is None:
            for variant in variants:
                yield {**row_dict, **variant}
        else:
            for address in network:
                for variant in variants:
                    yield {**row_dict, 'IP': str(address), **variant}

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
    allow_no_data_retry = row_data.pop('_AllowNoDataRetry', False)
    invalid_cidr = row_data.pop('_InvalidCIDR', False)

    if SHUTDOWN_EVENT.is_set():
        row_data['Error_Reason'] = "Canceled (Graceful Shutdown)"
        return (False, row_data)

    ip_address = row_data.get('IP')

    if not ip_address or str(ip_address).lower() == 'nan':
        return (False, {**row_data, 'Error_Reason': 'Missing or Invalid IP'})

    if invalid_cidr:
        return (False, {**row_data, 'Error_Reason': 'Invalid CIDR notation'})

    if no_data_missing_ts:
        return (False, {**row_data, 'Error_Reason': 'Missing Timestamp (No Data per user selection)'})

    timestamp = row_data.get('Timestamp') if (perform_historic_lookup and not force_current_lookup) else None

    status, payload = query_api(ip_address, timestamp, api_token, use_maxmind_geo)

    # No data for the requested date: try the surrounding dates, stopping at the first hit.
    if status == 'nodata' and timestamp and allow_no_data_retry and buffer_days > 0:
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
    elif status == 'nodata' and timestamp and allow_no_data_retry and current_fallback_date:
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
                if not success and str(result_data.get('Error_Reason', '')).startswith('Canceled'):
                    # Not saved: resume treats everything in the output files as done, so these must be retried instead.
                    stats_ref['canceled'] += 1
                    continue

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
                                    processed_keys.add(get_composite_key(ip, ts, record.get('Input_Row')))
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
            print("Error: File not found.")

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
        
    # Normalize column names before detection so they match what the later passes see
    first_chunk.columns = first_chunk.columns.str.lower().str.strip()
    ip_col, start_col, end_col = find_and_map_columns(first_chunk)
    
    perform_historic_lookup = False
    max_days_cap = None
    missing_timestamp_behavior = None
    buffer_days = 0
    static_dates = None
    current_fallback_date = None
    scan = None  # whole-file counts from scan_input_file, filled in lazily

    print("-" * 50)
    if resume_mode:
        print("⚠️  RESUME NOTICE")
        print("Already-processed records are matched on IP + queried date + input row. Choose the SAME date")
        print("option as the original session, otherwise those IPs will be queried again. (Resume also expects")
        print("the same input file; if rows were added, removed or reordered, affected rows are queried again.)")
        if prev_dates:
            print(f"Dates found in the previous output: {min(prev_dates)} to {max(prev_dates)}")
        if start_col:
            scan = ensure_scan(scan, input_file_path, is_csv_file, ip_col, start_col, end_col)
            if scan['earliest'] is not None:
                print(f"Timestamps in the input file span {scan['earliest']:%Y%m%d} to {scan['latest']:%Y%m%d}.")
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
        scan = ensure_scan(scan, input_file_path, is_csv_file, ip_col, start_col, end_col)
        span = propose_span(scan)

        while True:
            start_dt, end_dt = prompt_static_dates(span)
            num_days = (end_dt - start_dt).days + 1
            print(f"\n{start_dt:%Y%m%d} to {end_dt:%Y%m%d} = {num_days} day(s) per IP.")
            print(f"{scan['ips']:,} IP(s) x {num_days} day(s) = ~{scan['ips'] * num_days:,} API requests.")
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
                scan = ensure_scan(scan, input_file_path, is_csv_file, ip_col, start_col, end_col)
                found_max = max(scan['ips_by_days'], default=0)
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

    geo_input =input("Use MaxMind for geolocation (mmgeo=1)? (yes/no): ").strip().lower()
    use_maxmind_geo = geo_input in ['yes', 'y']
    print("-" * 50)

    # Size the whole job (CIDR ranges x dates) and let the user back out before any API calls or output files
    scan = ensure_scan(scan, input_file_path, is_csv_file, ip_col, start_col, end_col)
    total_requests = print_workload_summary(scan, date_mode, static_dates, max_days_cap, missing_timestamp_behavior, buffer_days, current_fallback_date, len(processed_keys))

    if scan['ips'] == 0:
        print("\nNo IP addresses to enrich were found in the input file. Exiting.")
        sys.exit(0)

    if total_requests > scan['rows']:
        if input("\nThis run makes more than one API request per input row. Proceed? (yes/no): ").strip().lower() not in ['yes', 'y']:
            print("Cancelled. Nothing was run.")
            sys.exit(0)
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

    stats = {'processed': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'buffered': 0, 'canceled': 0}

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
        def flush(batch):
            results = executor.map(lambda r: enrich_ip(r, api_token, perform_historic_lookup, use_maxmind_geo, buffer_days, current_fallback_date), batch)
            write_to_json_stream(results, output_file_path, failed_file_path, stats, start_main_time)

        # Records are generated lazily (CIDR x date expansion can be huge) and sent in bounded batches
        pending = []
        for chunk in reader:
            chunk.columns = chunk.columns.str.lower().str.strip()

            for r in process_chunk(chunk, ip_col, start_col, end_col, perform_historic_lookup, max_days_cap, skipped_file_path, missing_timestamp_behavior, run_date_str, static_dates):
                if SHUTDOWN_EVENT.is_set():
                    break

                ip_addr = r.get('IP')
                if not ip_addr or str(ip_addr).lower() == 'nan':
                    continue

                ts_val = r.get('Timestamp') if perform_historic_lookup else None
                row_key = get_composite_key(ip_addr, ts_val, r.get('Input_Row'))
                legacy_key = get_composite_key(ip_addr, ts_val)  # outputs from older runs have no Input_Row

                if resume_mode and (row_key in processed_keys or legacy_key in processed_keys):
                    stats['skipped'] += 1
                    stats['processed'] += 1
                    continue

                pending.append(r)
                if len(pending) >= CHUNK_SIZE:
                    flush(pending)
                    pending = []

            if SHUTDOWN_EVENT.is_set():
                break

        if pending and not SHUTDOWN_EVENT.is_set():
            flush(pending)

    print("\n" + "="*50)
    print("COMPLETED / SAVED".center(50))
    print("="*50)
    print(f"Total API Hits Evaluated: {stats['processed']}")
    print(f"Already Processed (Skip): {stats['skipped']}")
    print(f"Successfully Enriched:    {stats['success']}")
    if buffer_days or current_fallback_date:
        print(f"  (via buffer/fallback):  {stats['buffered']}")
    print(f"Failed Lookups:           {stats['failed']}")
    if stats['canceled']:
        print(f"Canceled (not saved):     {stats['canceled']}  (these will be retried if you Resume)")
    print("-" * 50)
    print(f"Successes saved to: {output_file_path}")
    if stats['failed'] > 0:
        print(f"Failures saved to:  {failed_file_path}")
    if max_days_cap:
        print(f"Audit Log saved to: {skipped_file_path}")

    print(f"\nTotal runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
