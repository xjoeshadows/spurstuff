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
MAX_WORKERS = 500

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

def write_to_json_stream(results_iterator, output_path):
    """
    Writes a stream of JSON objects to a JSON Lines file.
    Each JSON object is written on a new line.
    """
    processed_count = 0
    start_time = time.time()
    try:
        print(f"Writing enriched records to {output_path} (JSON Lines format)...")
        with open(output_path, 'w', encoding='utf-8') as outfile:
            for result in results_iterator:
                if result:
                    outfile.write(json.dumps(result, ensure_ascii=False) + '\n')
                    processed_count += 1
                    if processed_count % 1000 == 0:
                        elapsed_time = time.time() - start_time
                        records_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                        print(f"  Written {processed_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")
        print(f"Successfully wrote {processed_count} enriched records to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON Lines file: {e}", file=sys.stderr)
        sys.exit(1)


def read_ip_timestamp_and_all_data(input_file_path):
    """
    Reads all data from a CSV or XLSX file, processes the 'Timestamp' column,
    and returns a list of dictionaries, where each dict is a row.
    """
    try:
        if input_file_path.lower().endswith('.csv'):
            df = pd.read_csv(input_file_path)
        elif input_file_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_file_path)
        else:
            raise ValueError("Unsupported file format. Please use CSV or XLSX.")

        # Normalize column names to lowercase for case-insensitive checking and access
        df.columns = df.columns.str.lower()
        
        ip_col = None
        ts_col = None

        # Find the columns based on more specific terms
        for col in df.columns:
            if ('ip address' in col or '_ip_' in col) and ip_col is None:
                ip_col = col
            if 'timestamp' in col and ts_col is None:
                ts_col = col
        
        # Fallback to a simple 'ip' check if no specific match is found, just in case
        if ip_col is None:
            for col in df.columns:
                if 'ip' in col:
                    ip_col = col
                    break
        
        # Check if required columns were found
        if ip_col is None or ts_col is None:
            raise ValueError("Input file must contain a column for IP (e.g., 'ip address', '_ip_') and a column for 'timestamp'.")

        all_rows_data = []
        for index, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Map the identified columns to the standardized keys
            # and remove the original key to prevent the original Timestamp object from persisting
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

    except Exception as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Script ---
if __name__ == "__main__":
    # Record the start time of the entire script
    start_main_time = time.time()

    api_token = os.environ.get("TOKEN")
    if not api_token:
        print("Error: TOKEN environment variable not set.", file=sys.stderr)
        print("Please set it using: export TOKEN='YOUR_API_TOKEN'", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) != 2:
        print("Usage: python enrich_ip_api.py <input_file>", file=sys.stderr)
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)

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

    print(f"Reading data from {input_file_path}...")
    all_input_rows = read_ip_timestamp_and_all_data(input_file_path)
    total_ips = len(all_input_rows)
    print(f"Read {total_ips} records for enrichment.")

    valid_records_for_enrichment = [
        row for row in all_input_rows 
        if row.get('IP') and str(row['IP']).lower() != 'nan'
    ]
    
    if not valid_records_for_enrichment:
        print("No valid IP addresses found for enrichment. Exiting.", file=sys.stderr)
        sys.exit(0)

    print(f"Starting enrichment for {len(valid_records_for_enrichment)} valid records in parallel...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results_iterator = executor.map(lambda row: enrich_ip(row, api_token), valid_records_for_enrichment)
        write_to_json_stream(results_iterator, output_file_path)

    print("All enrichment tasks completed and results written to file.")

    end_main_time = time.time()
    total_runtime = end_main_time - start_main_time
    print(f"Total script runtime: {time.strftime('%H:%M:%S', time.gmtime(total_runtime))}")
