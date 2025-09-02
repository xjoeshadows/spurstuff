#!/usr/bin/env python3
import requests
import json
import sys
import os
import pandas as pd
from datetime import datetime
import concurrent.futures
from requests.adapters import HTTPAdapter # For retries
from urllib3.util.retry import Retry # For retry strategy
import time # For progress indicator

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"

# Set the maximum number of concurrent workers (threads) for API calls
# Be mindful of API rate limits when setting this value.
MAX_WORKERS = 32 # Set to a more conservative number for stability

# Chunk size for reading large files to prevent memory issues
CHUNK_SIZE = 10000

# API request timeout in seconds. Crucial for preventing indefinite hangs.
REQUEST_TIMEOUT = 10

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=8,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

# Create a session with the retry strategy
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Functions ---
def enrich_ip(row_data, api_token):
    """
    Enriches a single IP address using the Spur API, optionally with a timestamp,
    and merges additional row data into the JSON response.

    Args:
        row_data (dict): A dictionary representing a row from the input file,
                         expected to contain 'IP', 'Timestamp' (formatted), and other columns.
        api_token (str): The API authentication token.

    Returns:
        dict: The combined JSON response from the API and the input row data,
              or None if an error occurs or IP is invalid.
    """
    ip_address = row_data.get('IP')
    timestamp = row_data.get('Timestamp')

    if not ip_address or str(ip_address).lower() == 'nan':
        return None

    url = f"{api_url_base}{ip_address}"
    if timestamp:
        url += f"?dt={timestamp}"
    headers = {'TOKEN': api_token}

    try:
        response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        json_response = response.json()
        merged_response = {**row_data, **json_response}

        return merged_response
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"No Enrichment Data for {ip_address} on {timestamp}", file=sys.stderr)
        else:
            print(f"Error enriching {ip_address} (timestamp={timestamp}): {e}", file=sys.stderr)
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address} (timestamp={timestamp}): {e}", file=sys.stderr)
        return None

def write_to_json_stream(results_iterator, output_path, total_processed_count_ref, start_time):
    """
    Writes a stream of JSON objects to a JSON Lines file.
    Each JSON object is written on a new line.
    """
    last_update_time = time.time()
    try:
        # Open the file in append mode. It's handled in main to be truncated on the first run.
        with open(output_path, 'a', encoding='utf-8') as outfile:
            for result in results_iterator:
                if result:
                    outfile.write(json.dumps(result, ensure_ascii=False) + '\n')
                    total_processed_count_ref[0] += 1
                
                current_time = time.time()
                if current_time - last_update_time >= 5:
                    elapsed_time = current_time - start_time
                    records_per_second = total_processed_count_ref[0] / elapsed_time if elapsed_time > 0 else 0
                    print(f"  Processed {total_processed_count_ref[0]} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")
                    last_update_time = current_time

    except Exception as e:
        print(f"Error writing to JSON Lines file: {e}", file=sys.stderr)
        sys.exit(1)


def find_and_map_columns(df):
    """
    Finds and maps the IP and Timestamp columns from a DataFrame.
    """
    normalized_columns = df.columns.str.lower().str.strip()
    
    ip_col = None
    ts_col = None

    for col in normalized_columns:
        if ('ip address' in col or 'ips' in col or 'ip' in col) and ip_col is None:
            ip_col = col
        if 'timestamp' in col and ts_col is None:
            ts_col = col

    if ip_col is None or ts_col is None:
        raise ValueError("Input file must contain a column for IP (e.g., 'ip address', 'ips') and a column for 'timestamp'.")
    
    return ip_col, ts_col


def process_chunk(df_chunk, ip_col, ts_col):
    """
    Processes a single pandas DataFrame chunk.
    """
    all_rows_data = []
    for index, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        
        # Map the identified columns to the standardized keys
        row_dict['IP'] = row_dict.pop(ip_col)
        row_dict['Timestamp'] = row_dict.pop(ts_col)
        
        # Process timestamp to YYYYMMDD format
        timestamp_str = str(row_dict.get('Timestamp')) if pd.notna(row_dict.get('Timestamp')) else None
        formatted_timestamp = None
        if timestamp_str and timestamp_str.lower() != 'nan':
            try:
                dt_obj = datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M')
                formatted_timestamp = dt_obj.strftime('%Y%m%d')
            except ValueError:
                try:
                    # Handle ISO 8601 format with or without 'Z'
                    if timestamp_str.endswith('Z'):
                        dt_obj = datetime.fromisoformat(timestamp_str.replace('Z', ''))
                    else:
                        dt_obj = datetime.fromisoformat(timestamp_str)
                    formatted_timestamp = dt_obj.strftime('%Y%m%d')
                except ValueError:
                    try:
                        datetime.strptime(timestamp_str, '%Y%m%d')
                        formatted_timestamp = timestamp_str
                    except ValueError:
                        formatted_timestamp = None
        
        row_dict['Timestamp'] = formatted_timestamp

        # Clean up any other unexpected Timestamp objects that might be present
        for key, value in row_dict.items():
            if isinstance(value, pd.Timestamp):
                row_dict[key] = str(value)

        all_rows_data.append(row_dict)
    return all_rows_data


# --- Main Script ---
if __name__ == "__main__":
    start_main_time = time.time()

    api_token = os.environ.get("TOKEN")
    if not api_token:
        print("Error: TOKEN environment variable not set.")
        api_token = input("Please enter your Spur API token: ").strip()
        if not api_token:
            print("No API token provided. Exiting.", file=sys.stderr)
            sys.exit(1)
        os.environ['TOKEN'] = api_token
    
    input_file_path = None
    if len(sys.argv) == 2:
        input_file_path = sys.argv[1]
    
    if input_file_path is None:
        print("\n--- Input File Required ---")
        while True:
            file_input = input("Enter the path to your CSV or XLSX file: ").strip()
            if not file_input:
                print("File path cannot be empty. Please try again.")
                continue
            if not os.path.exists(file_input):
                print(f"Error: Input file not found at {file_input}. Please check the path and try again.")
                continue
            input_file_path = file_input
            break

    output_dir = os.path.dirname(input_file_path)
    input_file_basename = os.path.basename(input_file_path)
    input_file_name_without_ext = os.path.splitext(input_file_basename)[0]
    default_output_file_name = f"{input_file_name_without_ext}_SpurEnrichment.json"

    output_file_name_input = input(f"Enter the desired output file name (e.g., ip_data_enriched.json), or press Enter for default ({default_output_file_name}): ").strip()
    
    if not output_file_name_input:
        output_file_path = os.path.join(output_dir, default_output_file_name)
        print(f"Using default output file name: {default_output_file_name}")
    else:
        output_file_name_input = "".join(x for x in output_file_name_input if x.isalnum() or x in "._-")
        if not output_file_name_input.endswith(".json"):
            output_file_name_input += ".json"
        output_file_path = os.path.join(output_dir, output_file_name_input)

    # Truncate the output file at the start of a new run
    if os.path.exists(output_file_path):
        with open(output_file_path, 'w') as f:
            f.truncate(0)

    print(f"Reading data from {input_file_path} in chunks of {CHUNK_SIZE}...")
    total_ips = 0
    total_processed_count = [0] # Use a list to pass by reference

    # Set up the appropriate pandas reader for chunking
    if input_file_path.lower().endswith('.csv'):
        reader = pd.read_csv(input_file_path, chunksize=CHUNK_SIZE)
    elif input_file_path.lower().endswith(('.xls', '.xlsx')):
        reader = pd.read_excel(input_file_path, chunksize=CHUNK_SIZE)
    else:
        print("Error: Unsupported file format.", file=sys.stderr)
        sys.exit(1)
        
    # Process the file in chunks
    first_chunk = True
    ip_col, ts_col = None, None
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in reader:
            if first_chunk:
                ip_col, ts_col = find_and_map_columns(chunk)
                first_chunk = False
            
            # Count the number of IP records in the chunk
            total_ips += len(chunk)

            # Process the chunk and get a list of dictionaries
            chunk_data = process_chunk(chunk, ip_col, ts_col)

            # Submit tasks to the executor
            results_iterator = executor.map(lambda row: enrich_ip(row, api_token), chunk_data)

            # Stream the results to the file
            write_to_json_stream(results_iterator, output_file_path, total_processed_count, start_main_time)
            
    print("All enrichment tasks completed and results written to file.")
    print(f"Total IPs read for enrichment: {total_ips}")

    end_main_time = time.time()
    total_runtime = end_main_time - start_main_time
    print(f"Total script runtime: {time.strftime('%H:%M:%S', time.gmtime(total_runtime))}")
