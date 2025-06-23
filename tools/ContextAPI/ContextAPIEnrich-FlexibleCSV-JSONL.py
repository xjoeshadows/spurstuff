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
# api_token = "YOUR_API_TOKEN_HERE"  # Removed hardcoded token
default_output_file = "ip_data.jsonl" # Default output extension

# Set the maximum number of concurrent workers (threads) for API calls
# Be mindful of API rate limits when setting this value.
MAX_WORKERS = 200 # Keep at a realistic starting point

# API request timeout in seconds. Crucial for preventing indefinite hangs.
REQUEST_TIMEOUT = 30 # Increased to 30 seconds for more robustness

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=5, # Increased total retries from 3 to 5
    backoff_factor=2, # Increased backoff factor from 1 to 2 (delays: 2s, 4s, 8s, 16s, 32s)
    status_forcelist=[429, 500, 502, 503, 504], # HTTP status codes to retry on (429 is Too Many Requests)
    allowed_methods=["HEAD", "GET", "OPTIONS"] # Methods to retry
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
    timestamp = row_data.get('Timestamp') # This will be the already formattedYYYYMMDD timestamp

    # Ensure IP is valid before constructing URL
    if not ip_address or str(ip_address).lower() == 'nan':
        # print(f"Skipping invalid IP address: {ip_address}", file=sys.stderr) # Suppress this for large files
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
        
        # Merge all data from the input row into the API response
        # This will add new fields and potentially overwrite existing ones
        # if column names conflict with API response keys.
        merged_response = {**row_data, **json_response}

        return merged_response
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

        # Check for required columns
        if not all(col in df.columns for col in ['ip', 'timestamp']): # Changed to lowercase
            raise ValueError("Input file must contain columns named 'ip' and 'timestamp' (case-insensitive).")

        all_rows_data = []
        for index, row in df.iterrows():
            row_dict = row.to_dict() # Convert pandas Series (row) to a Python dictionary

            # Rename 'ip' and 'timestamp' keys to 'IP' and 'Timestamp' (if they are not already)
            # This ensures consistency for downstream functions expecting 'IP' and 'Timestamp'
            if 'ip' in row_dict and 'IP' not in row_dict:
                row_dict['IP'] = row_dict.pop('ip')
            if 'timestamp' in row_dict and 'Timestamp' not in row_dict:
                row_dict['Timestamp'] = row_dict.pop('timestamp')

            # Process timestamp toYYYYMMDD format
            timestamp_str = str(row_dict.get('Timestamp')) if pd.notna(row_dict.get('Timestamp')) else None
            formatted_timestamp = None
            if timestamp_str and timestamp_str.lower() != 'nan':
                try:
                    dt_obj = datetime.fromisoformat(timestamp_str)
                    formatted_timestamp = dt_obj.strftime('%Y%m%d')
                except ValueError:
                    try:
                        datetime.strptime(timestamp_str, '%Y%m%d')
                        formatted_timestamp = timestamp_str
                    except ValueError:
                        # print(f"Warning: Invalid timestamp format '{timestamp_str}' for IP {row_dict.get('IP')}. Skipping timestamp.", file=sys.stderr)
                        formatted_timestamp = None
            
            row_dict['Timestamp'] = formatted_timestamp # Update the dictionary with formatted timestamp
            all_rows_data.append(row_dict)
        return all_rows_data

    except Exception as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Script ---
if __name__ == "__main__":
    # Retrieve API token from environment variable
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
        if not output_file_name.endswith(".jsonl"):
            output_file_name += ".jsonl"
        output_file_path = os.path.join(output_dir, output_file_name)

    print(f"Reading data from {input_file_path}...")
    # Read all rows including IP, Timestamp, and any other columns
    all_input_rows = read_ip_timestamp_and_all_data(input_file_path)
    total_ips = len(all_input_rows)
    print(f"Read {total_ips} records for enrichment.")

    # Filter out invalid IPs before submission to executor
    valid_records_for_enrichment = [
        row for row in all_input_rows 
        if row.get('IP') and str(row['IP']).lower() != 'nan'
    ]
    
    if not valid_records_for_enrichment:
        print("No valid IP addresses found for enrichment. Exiting.", file=sys.stderr)
        sys.exit(0)

    print(f"Starting enrichment for {len(valid_records_for_enrichment)} valid records in parallel...")

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit each full row dictionary to enrich_ip, passing the api_token
        # functools.partial could also be used here for clarity if needed
        # lambda is used to pass the api_token to the enrich_ip function
        results_iterator = executor.map(lambda row: enrich_ip(row, api_token), valid_records_for_enrichment)
        
        # Stream the results directly to the JSONL file
        write_to_jsonl_stream(results_iterator, output_file_path)

    print("All enrichment tasks completed and results written to file.")
