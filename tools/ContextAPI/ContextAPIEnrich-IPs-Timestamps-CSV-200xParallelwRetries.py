import requests
import json
import sys
import os
import pandas as pd
from datetime import datetime
import concurrent.futures
from requests.adapters import HTTPAdapter # For retries
from urllib3.util.retry import Retry # For retry strategy
import csv # Import for CSV handling

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
api_token = "YOUR_API_TOKEN_HERE"  # Replace with your actual API token
default_output_file = "ip_data.csv" # Changed default output to CSV

# Set the maximum number of concurrent workers (threads) for API calls
# Be mindful of API rate limits when setting this value.
MAX_WORKERS = 200 # Reduced from 500 to a more realistic starting point

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=3, # Total number of retries
    backoff_factor=1, # Backoff factor for exponential delay (1s, 2s, 4s, etc.)
    status_forcelist=[429, 500, 502, 503, 504], # HTTP status codes to retry on (429 is Too Many Requests)
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
        print(f"Skipping invalid IP address: {ip_address}", file=sys.stderr)
        return None

    url = f"{api_url_base}{ip_address}"
    if timestamp:
        url += f"?dt={timestamp}"
    headers = {'TOKEN': api_token}
    try:
        # Use the session for the request
        response = HTTP.get(url, headers=headers)
        response.raise_for_status() # Raise an exception for bad status codes (e.g., 401, 400)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address} (timestamp={timestamp}): {e}", file=sys.stderr)
        return None

def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary."""
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_json(item, new_key + sep + str(i), sep=sep).items())
                else:
                    items.append((new_key + sep + str(i), item))
        else:
            items.append((new_key, v))
    return dict(items)

def write_to_csv(data, output_path):
    """Writes a list of dictionaries to a CSV file, ensuring all fields are included."""
    if not data:
        print("No data to write to CSV.")
        return

    # Collect all unique fieldnames from all dictionaries
    fieldnames = set()
    for row in data:
        fieldnames.update(row.keys())
    fieldnames = sorted(list(fieldnames))  # Sort for consistent column order

    try:
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='') # Added restval
            writer.writeheader()
            writer.writerows(data)
        print(f"Enriched data written to {output_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}", file=sys.stderr)

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
        for _, row in df.iterrows():
            ip = row['IP']
            timestamp_str = str(row['Timestamp']) if pd.notna(row['Timestamp']) else None
            
            # Process timestamp to YYYYMMDD format
            formatted_timestamp = None
            if timestamp_str and timestamp_str.lower() != 'nan':
                try:
                    # Attempt to parse as a full datetime string
                    dt_obj = datetime.fromisoformat(timestamp_str)
                    formatted_timestamp = dt_obj.strftime('%Y%m%d')
                except ValueError:
                    # If not a full datetime, check if it's already in YYYYMMDD
                    try:
                        datetime.strptime(timestamp_str, '%Y%m%d')
                        formatted_timestamp = timestamp_str
                    except ValueError:
                        # If neither, treat as invalid timestamp
                        print(f"Warning: Invalid timestamp format '{timestamp_str}' for IP {ip}. Skipping timestamp.", file=sys.stderr)
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
    output_file_name = input("Enter the desired output file name (e.g., ip_data.csv): ").strip() # Changed prompt
    if not output_file_name:
        output_file_path = os.path.join(output_dir, default_output_file)
        print(f"Using default output file name: {default_output_file}")
    else:
        # Sanitize the filename
        output_file_name = "".join(x for x in output_file_name if x.isalnum() or x in "._-")
        if not output_file_name.endswith(".csv"): # Ensure .csv extension
            output_file_name += ".csv"
        output_file_path = os.path.join(output_dir, output_file_name)

    # Read IP and timestamp data
    ip_timestamp_data = read_ip_timestamp_data(input_file_path)

    enriched_data = []

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit enrichment tasks for each IP-timestamp pair
        futures = {executor.submit(enrich_ip, ip, timestamp): (ip, timestamp) for ip, timestamp in ip_timestamp_data}

        # Iterate over the completed futures as they finish
        for future in concurrent.futures.as_completed(futures):
            ip, timestamp = futures[future] # Retrieve the original IP and timestamp for context
            try:
                enrichment_result = future.result() # Get the result of the API call
                if enrichment_result:
                    # Flatten the JSON result before appending to enriched_data
                    flattened_result = flatten_json(enrichment_result)
                    enriched_data.append(flattened_result)
            except Exception as exc:
                # The enrich_ip function already prints errors, so this catches unexpected exceptions
                print(f"Enrichment for IP {ip} (timestamp={timestamp}) failed unexpectedly: {exc}", file=sys.stderr)

    write_to_csv(enriched_data, output_file_path) # Call write_to_csv
