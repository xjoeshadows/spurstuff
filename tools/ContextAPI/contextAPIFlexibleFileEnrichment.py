#!/usr/bin/env python3
import requests
import json
import sys
import os
import pandas as pd
from datetime import datetime
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
MAX_WORKERS = 32
CHUNK_SIZE = 10000
REQUEST_TIMEOUT = 10

RETRY_STRATEGY = Retry(
    total=8,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Functions ---
def enrich_ip(row_data, api_token, perform_historic_lookup, use_maxmind_geo):
    ip_address = row_data.get('IP')
    timestamp = row_data.get('Timestamp') if perform_historic_lookup else None

    if not ip_address or str(ip_address).lower() == 'nan':
        return None

    url = f"{api_url_base}{ip_address}"
    query_params = []
    
    if timestamp:
        query_params.append(f"dt={timestamp}")
    if use_maxmind_geo:
        query_params.append("mmgeo=1")

    if query_params:
        url += "?" + "&".join(query_params)
        
    headers = {'TOKEN': api_token}

    try:
        response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        json_response = response.json()
        return {**row_data, **json_response}
    except Exception as e:
        print(f"Error enriching {ip_address}: {e}", file=sys.stderr)
        return None

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
                # 1. Try Epoch conversion
                try:
                    epoch_val = float(val)
                    if epoch_val > 100000000:
                        dt_obj = datetime.fromtimestamp(epoch_val)
                        formatted_timestamp = dt_obj.strftime('%Y%m%d')
                except (ValueError, OverflowError):
                    # 2. Try Standard String Formats
                    ts_str = str(val).strip()
                    formats = [
                        '%a, %b %d, %Y %I:%M %p %Z',
                        '%m/%d/%Y',
                        '%m/%d/%Y %H:%M',
                        '%Y%m%d'
                    ]
                    for fmt in formats:
                        try:
                            dt_obj = datetime.strptime(ts_str, fmt)
                            formatted_timestamp = dt_obj.strftime('%Y%m%d')
                            break
                        except ValueError:
                            continue
                    
                    if not formatted_timestamp:
                        # 3. Try ISO as last resort
                        try:
                            dt_obj = datetime.fromisoformat(ts_str.replace('Z', ''))
                            formatted_timestamp = dt_obj.strftime('%Y%m%d')
                        except ValueError:
                            formatted_timestamp = None

            row_dict['Timestamp'] = formatted_timestamp
            if not perform_historic_lookup:
                row_dict.pop('Timestamp', None)

        # Sanitize any existing pandas Timestamps to strings
        for key, value in row_dict.items():
            if isinstance(value, pd.Timestamp):
                row_dict[key] = str(value)

        all_rows_data.append(row_dict)
    return all_rows_data

def write_to_json_stream(results_iterator, output_path, total_processed_count_ref, start_time):
    last_update_time = time.time()
    try:
        with open(output_path, 'a', encoding='utf-8') as outfile:
            for result in results_iterator:
                if result:
                    outfile.write(json.dumps(result, ensure_ascii=False) + '\n')
                    total_processed_count_ref[0] += 1
                
                current_time = time.time()
                if current_time - last_update_time >= 5:
                    elapsed = current_time - start_time
                    rps = total_processed_count_ref[0] / elapsed if elapsed > 0 else 0
                    print(f"  Processed {total_processed_count_ref[0]} records ({rps:.2f} r/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed))} elapsed")
                    last_update_time = current_time
    except Exception as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
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
    
    input_file_path = sys.argv[1] if len(sys.argv) == 2 else None
    
    if input_file_path is None:
        print("\n--- Input File Required ---")
        print("Accepted Timestamp Formats:")
        print("  - Epoch (e.g., 1766060380)")
        print("  - M/D/YYYY (e.g., 8/15/2025)")
        print("  - Day, Mon DD, YYYY HH:MM PM/AM TZ (e.g., Wed, Sep 10, 2025 10:29 PM UTC)")
        print("  - ISO 8601 (e.g., 2025-08-15T00:00:00.000Z)")
        print("-" * 70)

        while True:
            file_input = input("Enter the path to your CSV or XLSX file: ").strip()
            if os.path.exists(file_input):
                input_file_path = file_input
                break
            print(f"Error: File not found.")

    output_dir = os.path.dirname(input_file_path)
    input_file_basename = os.path.basename(input_file_path)
    input_file_name_without_ext = os.path.splitext(input_file_basename)[0]

    # --- Initial Read for Detection ---
    print(f"Reading data from {input_file_path}...")
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
    
    # --- Configuration Prompts ---
    perform_historic_lookup = False
    if ts_col:
        print("-" * 35)
        lookup_input = input(f"A Timestamp column ('{ts_col}') was detected. Perform historical lookups? (yes/no): ").strip().lower()
        perform_historic_lookup = lookup_input in ['yes', 'y']
        status = "✅ YES" if perform_historic_lookup else "❌ NO (Timestamp will be removed from output)"
        print(f"Historical lookups: {status}")
        print("-" * 35)

    use_maxmind_geo = False
    print("-" * 35)
    geo_input = input("Use MaxMind for geolocation (mmgeo=1)? (yes/no): ").strip().lower()
    use_maxmind_geo = geo_input in ['yes', 'y']
    print(f"MaxMind Geo: {'✅ Enabled' if use_maxmind_geo else '❌ Disabled'}")
    print("-" * 35)

    # --- Filename Logic ---
    if perform_historic_lookup:
        default_out = f"{input_file_name_without_ext}_SpurHistoricEnrichment.json"
    else:
        default_out = f"{input_file_name_without_ext}_SpurEnrichment.json"

    out_input = input(f"Enter output file name, or press Enter for default ({default_out}): ").strip()
    output_file_path = os.path.join(output_dir, out_input if out_input else default_out)

    if os.path.exists(output_file_path):
        open(output_file_path, 'w').close()

    # --- Main Loop ---
    total_ips = 0
    total_processed = [0]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in reader:
            chunk.columns = chunk.columns.str.lower().str.strip()
            total_ips += len(chunk)
            chunk_data = process_chunk(chunk, ip_col, ts_col, perform_historic_lookup)
            valid = [r for r in chunk_data if r.get('IP') and str(r['IP']).lower() != 'nan']
            
            results = executor.map(lambda r: enrich_ip(r, api_token, perform_historic_lookup, use_maxmind_geo), valid)
            write_to_json_stream(results, output_file_path, total_processed, start_main_time)
            
    print(f"\nAll enrichment tasks completed.")
    print(f"Total IPs: {total_ips} | Saved to: {output_file_path}")
    print(f"Total runtime: {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_main_time))}")
