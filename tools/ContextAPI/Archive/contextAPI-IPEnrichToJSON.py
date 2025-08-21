#!/usr/bin/env python3
import requests
import json
import sys
import os
import concurrent.futures
from requests.adapters import HTTPAdapter # For retries
from urllib3.util.retry import Retry # For retry strategy
import time # For progress indicator
import datetime # For current date

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
# Retrieve API token from environment variable
API_TOKEN = os.environ.get("TOKEN")
if not API_TOKEN:
    print("Error: TOKEN environment variable not set.", file=sys.stderr)
    print("Please set it using: export TOKEN='YOUR_API_TOKEN'", file=sys.stderr)
    sys.exit(1)

# Set the maximum number of concurrent workers (threads) for API calls
# Be mindful of API rate limits when setting this value.
MAX_WORKERS = 50 # A more realistic starting point for parallel requests

# API request timeout in seconds. Crucial for preventing indefinite hangs.
REQUEST_TIMEOUT = 5 # Default timeout for API calls

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=5, # Total number of retries
    backoff_factor=2, # Backoff factor for exponential delay (2s, 4s, 8s, 16s, 32s)
    status_forcelist=[429, 500, 502, 503, 504], # HTTP status codes to retry on
    allowed_methods=["GET"] # Methods to retry
)

# Create a session with the retry strategy
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Functions ---
def enrich_ip(ip_address):
    """Enriches a single IP address using the Spur API."""
    # Ensure IP is valid before constructing URL
    if not ip_address or str(ip_address).lower() == 'nan':
        # print(f"Skipping invalid IP address: {ip_address}", file=sys.stderr) # Suppress for large files
        return None

    url = f"{api_url_base}{ip_address}"
    headers = {'TOKEN': API_TOKEN} # Use the global API_TOKEN
    try:
        # Use the session for the request and add a timeout
        response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address}: {e}", file=sys.stderr)
        return None

def write_to_jsonl_stream(results_iterator, output_path, total_records):
    """
    Writes a stream of JSON objects to a JSON Lines file.
    Each JSON object is written on a new line.
    Includes progress indicators.
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
                if processed_count % 1000 == 0 or processed_count == total_records:
                    elapsed_time = time.time() - start_time
                    records_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                    print(f"  Exported {processed_count}/{total_records} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")
        print(f"Successfully wrote {processed_count} enriched records to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON Lines file: {e}", file=sys.stderr)
        sys.exit(1) # Exit if writing fails

# --- Main Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python enrich_ip_api.py <input_file>", file=sys.stderr)
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)

    # Extract the directory and base filename from the input file path
    output_dir = os.path.dirname(input_file_path)
    base_input_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    
    # Construct the default output filename: YYYYMMDDContextAPEnrichment-[input file name here].jsonl
    current_date_ymd = datetime.date.today().strftime("%Y%m%d")
    default_output_file_name = f"{current_date_ymd}ContextAPEnrichment-{base_input_filename}.jsonl"

    # Get the output file name from the user
    output_file_name_prompt = f"Enter the desired output file name (e.g., {default_output_file_name}): "
    user_output_filename = input(output_file_name_prompt).strip()
    
    if not user_output_filename:
        output_file_path = os.path.join(output_dir, default_output_file_name)
        print(f"Using default output file name: {default_output_file_name}")
    else:
        # Sanitize the filename
        output_file_name = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not output_file_name.lower().endswith(".jsonl"): # Ensure .jsonl extension
            output_file_name += ".jsonl"
        output_file_path = os.path.join(output_dir, output_file_name)

    print(f"Reading IP addresses from {input_file_path}...")
    ip_addresses = []
    try:
        with open(input_file_path, 'r') as f:
            for line in f:
                ip = line.strip()
                if ip:
                    ip_addresses.append(ip)
    except FileNotFoundError: # This block is redundant due to os.path.exists check, but kept for consistency
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading input IP file: {e}", file=sys.stderr)
        sys.exit(1)

    total_ips = len(ip_addresses)
    if total_ips == 0:
        print("No IP addresses found in the input file. Exiting.", file=sys.stderr)
        sys.exit(0)
    print(f"Read {total_ips} IP addresses for enrichment.")

    print(f"Starting enrichment for {total_ips} IP addresses in parallel...")

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit enrichment tasks for each IP
        results_iterator = executor.map(enrich_ip, ip_addresses)

        # Stream the results directly to the JSONL file
        write_to_jsonl_stream(results_iterator, output_file_path, total_ips)

    print("All enrichment tasks completed and results written to file.")
