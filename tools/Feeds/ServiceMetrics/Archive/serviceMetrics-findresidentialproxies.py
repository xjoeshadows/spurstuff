#!/usr/bin/env python3
import requests
import gzip
import os
import sys
from io import BytesIO
import re # Import regular expression module for egrep functionality
import datetime # Import datetime for date formatting

# --- Configuration ---
DOWNLOAD_URL = "https://feeds.spur.us/v2/service-metrics/latest.json.gz"
# Use TOKEN from environment variable
API_TOKEN = os.environ.get('TOKEN')

# Get current date in YYYYMMDD format
current_date_ymd = datetime.datetime.now().strftime("%Y%m%d")

# Dynamically set default output filenames based on current date
DEFAULT_RAW_OUTPUT_FILENAME = f"{current_date_ymd}ServiceMetricsAll.json"
DEFAULT_GREP_OUTPUT_FILENAME = f"{current_date_ymd}ServiceMetrics-Residential.json"

# Keywords for egrep-like filtering
# The re.IGNORECASE flag makes the search case-insensitive
FILTER_KEYWORDS = ['residential']
FILTER_PATTERN = re.compile(r'|'.join(FILTER_KEYWORDS), re.IGNORECASE)

# --- Functions ---
def download_and_decompress_gz(url, token):
    """
    Downloads a .gz file from the given URL, decompresses it, and returns the content.
    """
    headers = {"Token": token}
    try:
        print(f"Downloading from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        # Read content in chunks to handle potentially large files
        compressed_data = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            compressed_data.write(chunk)
        compressed_data.seek(0) # Rewind to the beginning of the BytesIO object

        with gzip.GzipFile(fileobj=compressed_data, mode='rb') as gz_file:
            decompressed_content = gz_file.read().decode('utf-8') # Decode to string
        print("Download and decompression successful.")
        return decompressed_content
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}", file=sys.stderr)
        return None
    except gzip.BadGzipFile:
        print("Error: Downloaded file is not a valid gzip file.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during download/decompression: {e}", file=sys.stderr)
        return None

def write_content_to_file(content, output_path):
    """
    Writes string content to a specified file.
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            outfile.write(content)
        print(f"Content written to: {output_path}")
        return True
    except Exception as e:
        print(f"Error writing content to '{output_path}': {e}", file=sys.stderr)
        return False

def filter_content_and_write(input_content, keywords_pattern, output_path):
    """
    Filters content line by line using a regex pattern and writes matching lines to a file.
    """
    matching_lines_count = 0
    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            for line in input_content.splitlines():
                if keywords_pattern.search(line): # Use search to find pattern anywhere in line
                    outfile.write(line + '\n')
                    matching_lines_count += 1
        print(f"Filtered content written to: {output_path}")
        print(f"Found {matching_lines_count} matching lines.")
        return True
    except Exception as e:
        print(f"Error filtering and writing to '{output_path}': {e}", file=sys.stderr)
        return False

# --- Main Script ---
if __name__ == "__main__":
    if API_TOKEN is None:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    # --- Step 1: Download and Decompress ---
    print("--- Starting Download and Decompression ---")
    decompressed_data = download_and_decompress_gz(DOWNLOAD_URL, API_TOKEN)
    if decompressed_data is None:
        sys.exit(1)

    # --- Step 2: Write raw downloaded content ---
    raw_output_path = os.path.join(os.getcwd(), DEFAULT_RAW_OUTPUT_FILENAME) # Save in current directory

    if not write_content_to_file(decompressed_data, raw_output_path):
        sys.exit(1)

    # --- Step 3: Filter content and write it ---
    grep_output_path = os.path.join(os.getcwd(), DEFAULT_GREP_OUTPUT_FILENAME) # Save in current directory

    print(f"\n--- Starting Content Filtering for keywords: {', '.join(FILTER_KEYWORDS)} ---")
    if not filter_content_and_write(decompressed_data, FILTER_PATTERN, grep_output_path):
        sys.exit(1)

    print("\nScript execution completed successfully.")
