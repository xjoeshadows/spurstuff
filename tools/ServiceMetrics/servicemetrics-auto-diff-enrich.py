import sys
import requests
import json
import os
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import datetime
import re
import gzip # Import the gzip module

# --- Configuration ---
API_URL_BASE = "https://api.spur.us/v2/metadata/tags/"
# Use TOKEN from environment variable
API_TOKEN = os.environ.get('TOKEN')

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

# --- Functions from servicemetrics-listall.py (adapted) ---
def download_file(url, token, output_path):
    """
    Downloads a file from the specified URL and saves it to output_path.

    Args:
        url (str): The URL of the file to download.
        token (str): The API authentication token.
        output_path (str): The path to save the downloaded file (e.g., 'YYYYMMDD-ServiceMetricsList.json.gz').

    Returns:
        str: The path to the downloaded file, or None on error.
    """
    headers = {'Token': token}
    try:
        print(f"Downloading file from {url} to {output_path}...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Successfully downloaded {output_path}")
        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {url}: {e}", file=sys.stderr)
        return None

def decompress_gzip(gz_file_path, output_dir=None):
    """
    Decompresses a gzip file.

    Args:
        gz_file_path (str): The path to the gzip compressed file.
        output_dir (str, optional): Directory to save the decompressed file.
                                   Defaults to the same directory as gz_file_path.

    Returns:
        str: The path to the decompressed file, or None on error.
    """
    if output_dir is None:
        output_dir = os.path.dirname(gz_file_path)
    
    # Construct the output filename by removing .gz and potentially adding .json if not present
    base_name = os.path.basename(gz_file_path)
    if base_name.endswith('.gz'):
        decompressed_name = base_name[:-3] # Remove .gz
        if not decompressed_name.endswith('.json'): # Ensure .json extension
            decompressed_name += '.json'
    else: # Should not happen if gz_file_path is correctly named, but as a fallback
        decompressed_name = base_name + '.json' # Assume it should be json if no gz
        
    decompressed_path = os.path.join(output_dir, decompressed_name)

    try:
        print(f"Decompressing {gz_file_path} to {decompressed_path}...")
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(decompressed_path, 'wb') as f_out:
                f_out.write(f_in.read())
        print(f"Successfully decompressed to {decompressed_path}")
        return decompressed_path
    except FileNotFoundError:
        print(f"Error: Gzip file not found at {gz_file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error decompressing gzip data from {gz_file_path}: {e}", file=sys.stderr)
        return None

def extract_tag_values_from_json_file(file_path):
    """
    Extracts the values from a JSON file.
    This function is specifically adapted for Service Metrics feed, which is a list of strings.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        list: A list of tag values (strings), or None on error.
    """
    tags = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Read the entire file content as it's a single JSON array or JSON Lines
            content = f.read().strip()
            
            # Attempt to load as a single JSON array (expected for Service Metrics)
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    # Filter to ensure all items in the list are strings (tags)
                    tags = [item for item in data if isinstance(item, str)]
                    if len(tags) != len(data):
                        print(f"Warning: Some non-string items found in JSON array in {file_path}. Skipping them.", file=sys.stderr)
                else:
                    print(f"Warning: Expected a JSON array of strings in {file_path}, but got type {type(data)}. Attempting line-by-line parsing.", file=sys.stderr)
                    # Fallback to line-by-line for potential JSON Lines with simple strings if primary parse fails
                    for line_num, line in enumerate(content.split('\n'), 1):
                        line = line.strip()
                        if line:
                            # Try to parse each line as a string, or simple JSON value
                            try:
                                parsed_line = json.loads(line)
                                if isinstance(parsed_line, str):
                                    tags.append(parsed_line)
                                else:
                                    print(f"Warning: Line {line_num} in {file_path} is not a simple string or string JSON value. Skipping: {line[:80]}...", file=sys.stderr)
                            except json.JSONDecodeError:
                                # If it's not valid JSON, treat it as a raw string if it's not empty
                                tags.append(line)
            except json.JSONDecodeError:
                # If the entire file is not a single JSON array, try parsing line by line
                print(f"Warning: File {file_path} is not a single valid JSON array. Attempting line-by-line parsing for potential JSON Lines.", file=sys.stderr)
                for line_num, line in enumerate(content.split('\n'), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        # Try to load each line as a JSON object, specifically looking for 'tag' field
                        json_data = json.loads(line)
                        if isinstance(json_data, dict) and 'tag' in json_data:
                            tags.append(json_data['tag'])
                        elif isinstance(json_data, str): # Handle cases where lines might just be plain strings in quotes
                            tags.append(json_data)
                        else:
                            print(f"Warning: Line {line_num} in {file_path} contains valid JSON but not a 'tag' field or is not a string. Skipping: {line[:80]}...", file=sys.stderr)
                    except json.JSONDecodeError:
                        # If a line is not valid JSON, assume it's a plain string tag
                        tags.append(line)

        print(f"Successfully extracted {len(tags)} tags from {file_path}")
        return tags
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error reading and processing {file_path}: {e}", file=sys.stderr)
        return None

# --- Functions from servicemetricsdiff-enriched.py ---
# Note: read_tags_from_file is now effectively replaced by extract_tag_values_from_json_file
# but kept here if other parts of the script were to call it expecting a .txt file of tags.
# For this specific script, we're always dealing with JSON files that get their tags extracted.
def read_tags_from_file(filename):
    """
    Reads tags from a file, one tag per line. This is a placeholder/legacy from original,
    main logic uses extract_tag_values_from_json_file for Service Metrics JSON.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            tags = [line.strip() for line in f if line.strip()]
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

def write_to_jsonl_stream(results_iterator, output_path):
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
    Main function to download the latest Service Metrics feed,
    compare it with a user-supplied file, and enrich the added tags.
    """
    if API_TOKEN is None:
        print("Error: TOKEN environment variable not set. Please set it to your Spur API token.", file=sys.stderr)
        sys.exit(1)

    # 1. Download the latest Service Metrics feed
    current_date_ymd = datetime.datetime.now().strftime("%Y%m%d")
    latest_gz_filename = f"{current_date_ymd}-ServiceMetricsList.json.gz"
    # Ensure the decompressed file name correctly matches what gzip produces (e.g., ends in .json)
    latest_decompressed_filename = f"{current_date_ymd}-ServiceMetricsList.json"
    service_metrics_url = 'https://feeds.spur.us/v2/service-metrics/latest.json.gz'

    downloaded_gz_path = download_file(service_metrics_url, API_TOKEN, latest_gz_filename)
    if not downloaded_gz_path:
        print("Failed to download the latest Service Metrics feed. Exiting.", file=sys.stderr)
        sys.exit(1)

    latest_tags_file = decompress_gzip(downloaded_gz_path)
    if not latest_tags_file:
        print("Failed to decompress the latest Service Metrics feed. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Load tags from the newly downloaded file
    tags_latest = extract_tag_values_from_json_file(latest_tags_file)
    if tags_latest is None:
        print("Failed to extract tags from the latest Service Metrics feed. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Latest Service Metrics file: {latest_tags_file}")

    # 2. Ask the user for another file to compare with
    comparison_file_path = input("\nPlease enter the full path to the comparison Service Metrics file (e.g., '/path/to/20240501-ServiceMetricsList.json'): ").strip()

    if not os.path.exists(comparison_file_path):
        print(f"Error: Comparison file '{comparison_file_path}' not found. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # Load tags from the comparison file
    tags_comparison = extract_tag_values_from_json_file(comparison_file_path)
    if tags_comparison is None:
        print("Failed to extract tags from the comparison file. Exiting.", file=sys.stderr)
        sys.exit(1)

    print(f"Comparison Service Metrics file: {comparison_file_path}")

    # 3. Compare the tag lists
    differences = compare_tag_lists(tags_comparison, tags_latest) # Comparison is (old, new)
    if differences is None:
        print("Error: Could not compare tag lists. Exiting.", file=sys.stderr)
        sys.exit(1)

    print("\n--- Tag Differences ---")
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

    # 4. Determine output filename based on both dates
    latest_file_date = get_date_from_filename_or_creation(latest_tags_file)
    comparison_file_date = get_date_from_filename_or_creation(comparison_file_path)

    # Output filename: YYYYMMDD-YYYYMMDDServiceMetricsDiff.jsonl
    output_filename = f"{latest_file_date}-{comparison_file_date}ServiceMetricsDiff.jsonl"
    output_path = os.path.join(os.getcwd(), output_filename) 

    print(f"\nStarting enrichment for {len(tags_to_enrich)} added tags in parallel...")
    
    # Use ThreadPoolExecutor for parallel API calls
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results_iterator = executor.map(enrich_tag_metadata, tags_to_enrich)
        write_to_jsonl_stream(results_iterator, output_path)

    print("Tag enrichment process completed.")

if __name__ == "__main__":
    main()
