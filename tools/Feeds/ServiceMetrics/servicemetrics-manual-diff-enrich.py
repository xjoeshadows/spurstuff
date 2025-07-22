#!/usr/bin/env python3
import sys
import requests
import json
import os
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import datetime # Import datetime for date handling
import re     # Import re for regex operations

# --- Configuration ---
API_URL_BASE = "https://api.spur.us/v2/metadata/tags/"
# Use TOKEN from environment variable
API_TOKEN = os.environ.get('TOKEN')
# DEFAULT_OUTPUT_FILE = "added_tags_enrichment.json" # This will be dynamically generated now

MAX_WORKERS = 10  # Number of concurrent API requests. Adjust based on API rate limits.
REQUEST_TIMEOUT = 15 # Timeout for each API request in seconds

# Retry strategy for transient network errors and potential soft rate limits
RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=1, # Delays: 1s, 2s, 4s, 8s, 16s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)

# Create a session with the retry strategy
ADAPTER = HTTPAdapter(max_retries=RETRY_STRATEGY)
HTTP = requests.Session()
HTTP.mount("https://", ADAPTER)
HTTP.mount("http://", ADAPTER)

# --- Functions ---
def read_tags_from_file(filename):
    """
    Reads tags from a file, one tag per line.

    Args:
        filename (str): The name of the file to read from.

    Returns:
        list: A list of tags, or None on error.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            tags = [line.strip() for line in f if line.strip()] # Read lines and remove empty ones
        return tags
    except FileNotFoundError:
        print(f"Error: File not found: {filename}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error reading file {filename}: {e}", file=sys.stderr)
        return None

def compare_tag_lists(tags1, tags2):
    """
    Compares two lists of tags and returns the differences.

    Args:
        tags1 (list): The first list of tags.
        tags2 (list): The second list of tags.

    Returns:
        dict: A dictionary containing the differences:
            {
                'added': tags present in tags2 but not in tags1,
                'removed': tags present in tags1 but not in tags2
            }
        Returns None if there is an error
    """
    if tags1 is None or tags2 is None:
        return None

    added = sorted(list(set(tags2) - set(tags1))) # Sort for consistent output
    removed = sorted(list(set(tags1) - set(tags2))) # Sort for consistent output
    return {'added': added, 'removed': removed}

def enrich_tag_metadata(tag):
    """
    Enriches a single tag using the Spur Metadata Tags API.

    Args:
        tag (str): The tag string to enrich.

    Returns:
        dict: The JSON response from the API, or None if an error occurs.
    """
    url = f"{API_URL_BASE}{tag}"
    headers = {'Token': API_TOKEN}
    try:
        response = HTTP.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error enriching tag '{tag}': {e}", file=sys.stderr)
        return None

def write_to_json_stream(results_iterator, output_path):
    """
    Writes a stream of JSON objects to a JSON Lines file.
    Each JSON object is written on a new line.
    """
    processed_count = 0
    start_time = time.time()
    try:
        print(f"Writing enriched tag data to {output_path} (JSON Lines format)...")
        with open(output_path, 'w', encoding='utf-8') as outfile:
            for result in results_iterator:
                if result: # Only write if result is not None (i.e., enrichment was successful)
                    outfile.write(json.dumps(result, ensure_ascii=False) + '\n')
                    processed_count += 1
                    # Progress indicator for writing phase
                    if processed_count % 100 == 0: # Print progress every 100 records
                        elapsed_time = time.time() - start_time
                        records_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                        print(f"  Written {processed_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")
        print(f"Successfully wrote {processed_count} enriched records to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON Lines file: {e}", file=sys.stderr)
        sys.exit(1)

def get_date_from_filename_or_creation(file_path):
    """
    Attempts to extract YYYYMMDD from filename. If not found, uses file modification date.
    """
    filename = os.path.basename(file_path)
    # Check for YYYYMMDD pattern at the beginning of the filename (e.g., 20240101-...)
    match = re.match(r'^(\d{8})', filename)
    if match:
        return match.group(1)
    else:
        try:
            # Fallback to file modification time (st_mtime) as it's more consistently available
            mod_timestamp = os.path.getmtime(file_path)
            dt_object = datetime.datetime.fromtimestamp(mod_timestamp)
            return dt_object.strftime("%Y%m%d")
        except Exception as e:
            print(f"Warning: Could not extract date from filename or file modification time for {filename}. Using current date.", file=sys.stderr)
            return datetime.datetime.now().strftime("%Y%m%d")


def main():
    """
    Main function to compare tag lists from two files and enrich added tags.
    """
    if len(sys.argv) != 3:
        print("Usage: python servicetagsdiff.py <file1.txt> <file2.txt>", file=sys.stderr)
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    tags1 = read_tags_from_file(file1)
    tags2 = read_tags_from_file(file2)

    if tags1 is None or tags2 is None:
        print("Error: Failed to read tag lists from files. Exiting.", file=sys.stderr)
        sys.exit(1)

    differences = compare_tag_lists(tags1, tags2)
    if differences is None:
        print("Error: Could not compare tag lists. Exiting.", file=sys.stderr)
        sys.exit(1)

    print("--- Tag Differences ---")
    print(f"Tags added ({len(differences['added'])}):")
    for tag in differences['added']:
        print(f"  + {tag}")
    print(f"Tags removed ({len(differences['removed'])}):")
    for tag in differences['removed']:
        print(f"  - {tag}")
    print("-----------------------")

    tags_to_enrich = differences['added']

    if not tags_to_enrich:
        print("No new tags found to enrich. Exiting.")
        sys.exit(0)

    # Get dates from input files for default output filename
    file1_date = get_date_from_filename_or_creation(file1)
    file2_date = get_date_from_filename_or_creation(file2)

    # Default output filename: file1_date-file2_dateSMDiffEnriched.json
    # Assuming file1 is the 'old' and file2 is the 'new' for diff context
    default_output_filename = f"{file1_date}-{file2_date}SMDiffEnriched.json"

    # Prompt for output filename
    output_file_name_prompt = f"Enter the desired output JSON file name (e.g., {default_output_filename}): ".strip()
    output_file_name = input(output_file_name_prompt).strip()

    if not output_file_name:
        output_path = os.path.join(os.getcwd(), default_output_filename)
        print(f"Using default output file name: {default_output_filename}")
    else:
        output_file_name = "".join(x for x in output_file_name if x.isalnum() or x in "._-")
        if not output_file_name.lower().endswith(".json"):
            output_file_name += ".json"
        # Place output file in the current working directory
        output_path = os.path.join(os.getcwd(), output_file_name) 

    print(f"\nStarting enrichment for {len(tags_to_enrich)} added tags in parallel...")
    
    # Use ThreadPoolExecutor for parallel API calls
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit enrichment tasks for each added tag
        results_iterator = executor.map(enrich_tag_metadata, tags_to_enrich)
        
        # Stream the results directly to the JSON file
        write_to_json_stream(results_iterator, output_path)

    print("Tag enrichment process completed.")

if __name__ == "__main__":
    main()
