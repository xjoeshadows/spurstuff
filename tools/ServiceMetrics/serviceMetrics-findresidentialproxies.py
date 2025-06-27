#!/usr/bin/env python3
import requests
import gzip
import os
import sys
from io import BytesIO
import re # Import regular expression module for egrep functionality

# --- Configuration ---
DOWNLOAD_URL = "https://feeds.spur.us/v2/service-metrics/latest.json.gz"
# Use TOKEN from environment variable
API_TOKEN = os.environ.get('TOKEN')
DEFAULT_RAW_OUTPUT_FILENAME = "service_metrics_latest.json"
DEFAULT_GREP_OUTPUT_FILENAME = "filtered_metrics.txt"

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
        print(f"Raw content written to: {output_path}")
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

def get_output_filename(prompt_message, default_filename, expected_extension):
    """
    Prompts the user for an output filename, sanitizes it, and ensures correct extension.
    """
    filename = input(prompt_message).strip()
    if not filename:
        print(f"No filename provided. Using default: {default_filename}")
        return default_filename
    else:
        sanitized_filename = "".join(x for x in filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(expected_extension):
            sanitized_filename += expected_extension
        return sanitized_filename

# --- Main Script ---
if __name__ == "__main__":
    # --- Step 1: Download and Decompress ---
    print("--- Starting Download and Decompression ---")
    decompressed_data = download_and_decompress_gz(DOWNLOAD_URL, API_TOKEN)
    if decompressed_data is None:
        sys.exit(1)

    # --- Step 2: Get filename for raw downloaded content and write it ---
    raw_output_filename = get_output_filename(
        f"Enter filename for the raw downloaded content (e.g., {DEFAULT_RAW_OUTPUT_FILENAME}): ",
        DEFAULT_RAW_OUTPUT_FILENAME,
        ".json" # Assuming the decompressed content is JSON
    )
    raw_output_path = os.path.join(os.getcwd(), raw_output_filename) # Save in current directory

    if not write_content_to_file(decompressed_data, raw_output_path):
        sys.exit(1)

    # --- Step 3: Get filename for filtered content and write it ---
    grep_output_filename = get_output_filename(
        f"Enter filename for the filtered (grep) output (e.g., {DEFAULT_GREP_OUTPUT_FILENAME}): ",
        DEFAULT_GREP_OUTPUT_FILENAME,
        ".txt"
    )
    grep_output_path = os.path.join(os.getcwd(), grep_output_filename) # Save in current directory

    print(f"\n--- Starting Content Filtering for keywords: {', '.join(FILTER_KEYWORDS)} ---")
    if not filter_content_and_write(decompressed_data, FILTER_PATTERN, grep_output_path):
        sys.exit(1)

    print("\nScript execution completed successfully.")
