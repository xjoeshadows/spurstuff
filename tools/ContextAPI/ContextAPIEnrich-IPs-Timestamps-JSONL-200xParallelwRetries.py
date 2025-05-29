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
api_token = "HNRDfEWuYGCvuD3I1hCPsK"  # Replace with your actual API token
default_output_file = "ip_data.jsonl" # Changed default output to JSONL

# Set the maximum number of concurrent workers (threads) for API calls
# Be mindful of API rate limits when setting this value.
MAX_WORKERS = 200 # Keeping MAX_WORKERS as per your uploaded script

# API request timeout in seconds. Crucial for preventing indefinite hangs.
REQUEST_TIMEOUT = 30 # Added/Increased timeout for robustness

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=5, # Increased total retries (from 3)
    backoff_factor=2, # Increased backoff factor (from 1) for more robust delays
    status_forcelist=[429, 500, 502, 503, 504], # HTTP status codes to retry on
    allowed_methods=["HEAD", "GET", "OPTIONS"] # Methods to retry
)

# Create a session with the retry strategy
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Functions ---
def enrich_ip(ip_address, timestamp=None):
    """Enriches a single IP address using the Spur API, optionally with a timestamp."""
    # Ensure IP is valid before constructing URL
    if not ip_address or str(ip_address).lower() == 'nan':
        # print(f"Skipping invalid IP address: {ip_address}", file=sys.stderr) # Suppress for large files
        return None

    url = f"{api_url_base}{ip_address}"
    if timestamp:
        url += f"?dt={timestamp}"
    headers = {'TOKEN': api_token}
    try:
        # Use the session for the request and add a timeout
        response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Raise an exception for bad status codes (e.g., 401, 400)
        
        json_response = response.json()
        
        # Removed the 'datetime' field addition here
        # if timestamp:
        #     json_response['datetime'] = timestamp
        # else:
        #     json_response['datetime'] = None

        return json_response
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address} (timestamp={timestamp}): {e}", file=sys.stderr)
        return None

def write_to_jsonl_stream(results_iterator, output_path):
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
                if result: # Only write if result is not None (i.e., enrichment was successful)
                    outfile.write(json.dumps(result, ensure_ascii=False) + '\n')
                    processed_count += 1
                    # Progress indicator for writing phase
                    if processed_count % 1000 == 0:
                        elapsed_time = time.time() - start_time
                        records_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                        print(f"  Written {processed_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")
        print(f"Successfully wrote {processed_count} enriched records to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON Lines file: {e}", file=sys.stderr)
        sys.exit(1)


def read_ip_timestamp_data(input_file_path):
    """Reads IP address and timestamp data from a CSV or XLSX file using pandas."""
    try:
        if input_file_path.lower().endswith('.csv'):
            df = pd.read_csv(input_file_path)
        elif input_file_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_file_path)
        else:
            raise ValueError("Unsupported file format. Please use CSV or XLSX.")

        # Check for required columns
        if not all(col in df.columns for col in ['IP', 'Timestamp']):
            raise ValueError("Input file must contain columns named 'IP' and 'Timestamp'.")

        ip_timestamp_list = []
        for index, row in df.iterrows():
            ip = row['IP']
            timestamp_str = str(row['Timestamp']) if pd.notna(row['Timestamp']) else None
            
            # Process timestamp toYYYYMMDD format
            formatted_timestamp = None
            if timestamp_str and timestamp_str.lower() != 'nan':
                try:
                    # Attempt to parse as a full datetime string
                    dt_obj = datetime.fromisoformat(timestamp_str)
                    formatted_timestamp = dt_obj.strftime('%Y%m%d')
                except ValueError:
                    # If not a full datetime, check if it's already inYYYYMMDD
                    try:
                        datetime.strptime(timestamp_str, '%Y%m%d')
                        formatted_timestamp = timestamp_str
                    except ValueError:
                        # If neither, treat as invalid timestamp
                        # print(f"Warning: Invalid timestamp format '{timestamp_str}' for IP {ip}. Skipping timestamp.", file=sys.stderr) # Suppress for large files
                        formatted_timestamp = None # Explicitly set to None if format is bad

            ip_timestamp_list.append((ip, formatted_timestamp))
        return ip_timestamp_list

    except Exception as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python enrich_ip_api.py <input_file>", file=sys.stderr)
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)

    # Extract the directory from the input file path
    output_dir = os.path.dirname(input_file_path)

    # Get the output file name from the user
    output_file_name = input("Enter the desired output file name (e.g., ip_data.jsonl): ").strip()
    if not output_file_name:
        output_file_path = os.path.join(output_dir, default_output_file)
        print(f"Using default output file name: {default_output_file}")
    else:
        # Sanitize the filename
        output_file_name = "".join(x for x in output_file_name if x.isalnum() or x in "._-")
        if not output_file_name.endswith(".jsonl"): # Ensure .jsonl extension
            output_file_name += ".jsonl"
        output_file_path = os.path.join(output_dir, output_file_name)

    print(f"Reading IP and timestamp data from {input_file_path}...")
    ip_timestamp_data = read_ip_timestamp_data(input_file_path)
    total_ips = len(ip_timestamp_data)
    print(f"Read {total_ips} IP addresses for enrichment.")

    # Use ThreadPoolExecutor for parallel processing
    # We will iterate over the results as they complete and stream them to the file.
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks using map, which returns an iterator of results
        # The lambda function unpacks the (ip, timestamp) tuple for enrich_ip
        results_iterator = executor.map(lambda p: enrich_ip(p[0], p[1]), ip_timestamp_data)
        
        # Stream the results directly to the JSONL file
        write_to_jsonl_stream(results_iterator, output_file_path)

    print("All enrichment tasks completed and results written to file.")
