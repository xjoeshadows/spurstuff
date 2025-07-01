#!/usr/bin/env python3
import requests
import gzip
import os
import sys
from io import BytesIO
import re # Import regular expression module
import json # Import json for parsing
import concurrent.futures # For parallel processing
import time # For progress indicators
import datetime # For current date

# --- Configuration ---
DOWNLOAD_URL = "https://feeds.spur.us/v2/service-metrics/latest.json.gz"
API_TOKEN = os.environ.get('TOKEN') # Use TOKEN from environment variable

# Default filename for the raw decompressed service metrics feed
# This will be YYYYMMDDServiceMetricsFull.json
DEFAULT_RAW_OUTPUT_FILENAME_TEMPLATE = "{}ServiceMetricsAll.json" 

# Default prefix for the filtered output filename (e.g., YYYYMMDDServiceMetrics)
DEFAULT_FILTERED_OUTPUT_FILENAME_PREFIX_TEMPLATE = "{}_ServiceMetrics" 

# --- Functions ---
def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary.
    Handles nested dictionaries and lists of dictionaries/simple values.
    - Lists of dictionaries: items are flattened with indexed keys (e.g., 'list_key_0_sub_key').
    - Lists of simple values: values are joined into a comma-separated string under a single key
      (e.g., 'list_key': 'value1,value2').
    """
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Check if all items in the list are simple values (not dictionaries)
            if all(not isinstance(item, dict) for item in v):
                # If so, join them into a single comma-separated string
                items.append((new_key, ','.join(map(str, v))))
            else:
                # If the list contains dictionaries, flatten each dictionary with indexed keys
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_json(item, new_key + sep + str(i), sep=sep).items())
                    else:
                        # For mixed lists, if a simple value appears, still index it
                        items.append((new_key + sep + str(i), item))
        else:
            items.append((new_key, v))
    return dict(items)

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

def process_chunk_for_filtering(chunk_content, filter_column, keywords):
    """
    Processes a chunk of content (string), filters JSON objects, and returns matching ones.
    This is designed to be run in parallel.
    """
    matching_objects = []
    for line in chunk_content.splitlines():
        line_stripped = line.strip()
        if not line_stripped:
            continue
        
        try:
            json_obj = json.loads(line_stripped)
            
            should_export = True
            if keywords: # Only filter if keywords are provided
                if filter_column: # Column-specific filter
                    flattened_obj = flatten_json(json_obj) # Flatten to access nested keys
                    column_value = str(flattened_obj.get(filter_column, '')).lower() # Get value, convert to string, lowercase
                    if not any(kw in column_value for kw in keywords):
                        should_export = False # Does not match filter criteria
                else: # General line-based filter (no specific column)
                    if not any(kw in line_stripped.lower() for kw in keywords):
                        should_export = False
            
            if should_export:
                matching_objects.append(json_obj)
        except json.JSONDecodeError:
            # print(f"Warning: Skipping malformed JSON line in chunk: {e_line} in '{line_stripped[:80]}...'", file=sys.stderr)
            pass # Suppress for performance/clean output in parallel processing
        except Exception:
            # print(f"Warning: An unexpected error occurred in chunk: {e_other} in '{line_stripped[:80]}...'", file=sys.stderr)
            pass # Suppress for performance/clean output in parallel processing
    return matching_objects


def get_output_filename_with_defaults(default_prefix, filter_column_name=None, filter_keywords_input=None):
    """
    Prompts the user for an output filename, offering a default based on filter criteria.
    Capitalizes the first letter of each word for the column name and keywords in the default filename.
    """
    default_name_parts = [default_prefix]
    
    # Only include column name in default filename if a specific column was chosen for filtering
    if filter_column_name:
        sanitized_col_name = re.sub(r'[^a-zA-Z0-9]', '', filter_column_name).title()
        if sanitized_col_name:
            default_name_parts.append(sanitized_col_name)
            
    if filter_keywords_input:
        # Take first few keywords for filename, sanitize, then capitalize the first letter of each word
        sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in filter_keywords_input.split(',') if kw.strip()][:3]
        if sanitized_keywords:
            default_name_parts.append("".join(sanitized_keywords))
    
    default_filename = "".join(default_name_parts) + ".jsonl" # Output is JSONL
    
    prompt_message = f"Enter the desired output file name (e.g., {default_filename}): "
    user_output_filename = input(prompt_message).strip()

    if not user_output_filename:
        print(f"No filename provided. Using default: {default_filename}")
        return default_filename
    else:
        sanitized_filename = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".jsonl"): # Ensure .jsonl extension
            sanitized_filename += ".jsonl"
        return sanitized_filename

# --- Main Script ---
if __name__ == "__main__":
    if API_TOKEN is None:
        print("Error: TOKEN environment variable not set. Please set it to your Spur API token.", file=sys.stderr)
        sys.exit(1)

    current_date_ymd = datetime.date.today().strftime("%Y%m%d")

    # --- Step 1: Download and Decompress ---
    print("--- Starting Download and Decompression ---")
    
    # Default raw output filename is YYYYMMDDServiceMetricsAll.json
    raw_output_filename = DEFAULT_RAW_OUTPUT_FILENAME_TEMPLATE.format(current_date_ymd)
    raw_output_path = os.path.join(os.getcwd(), raw_output_filename) 

    decompressed_data = download_and_decompress_gz(DOWNLOAD_URL, API_TOKEN)
    if decompressed_data is None:
        sys.exit(1)

    # --- Step 2: Write raw downloaded content to a default file ---
    if not write_content_to_file(decompressed_data, raw_output_path):
        sys.exit(1)

    # --- Step 3: Get user input for filtering ---
    filter_column = None
    filter_keywords_input = None
    keywords = [] # Initialize keywords list

    # Always ask for keywords, regardless of column filtering choice
    filter_keywords_input = input("\nEnter keywords to search for across the file (comma-separated, e.g., 'malicious,trojan'): ").strip()
    if filter_keywords_input: # Only proceed with filter options if keywords are provided
        keywords = [kw.strip().lower() for kw in filter_keywords_input.split(',') if kw.strip()]
        if not keywords: # If keywords input was just spaces or commas
            print("No valid keywords provided. Proceeding without filtering.", file=sys.stderr)
            perform_filter = 'N' # Effectively no filter
        else:
            # If keywords are provided, then ask about column-specific filtering
            perform_column_specific_filter = input("Do you want to filter by a specific column (Y/N)? ").strip().upper()
            if perform_column_specific_filter == 'Y':
                # Sample data to suggest column names for filtering
                print("\n--- Analyzing sample data for filterable columns ---")
                sample_lines = []
                for line in decompressed_data.splitlines()[:100]: # Read first 100 lines to sample
                    line_stripped = line.strip()
                    if line_stripped:
                        sample_lines.append(line_stripped)

                suggested_keys = set()
                for line in sample_lines:
                    try:
                        obj = json.loads(line)
                        flattened_obj = flatten_json(obj)
                        suggested_keys.update(flattened_obj.keys())
                    except json.JSONDecodeError:
                        pass

                if suggested_keys:
                    print("\nAvailable columns for filtering (flattened names, sampled from first 100 lines):")
                    for key in sorted(list(suggested_keys)):
                        print(f"  - {key}")
                    print("\n")
                else:
                    print("\nCould not determine column names from sample. Please enter column name carefully.")

                filter_column = input("Enter the exact column name to filter (e.g., 'client_behaviors', 'ip', 'organization'): ").strip()
                if not filter_column:
                    print("No column name provided. Reverting to general keyword search.", file=sys.stderr)
                    # If column not provided, revert to general search (filter_column remains None)
            elif perform_column_specific_filter == 'N':
                print("Proceeding with general keyword search across entire lines.", file=sys.stderr)
                # filter_column remains None
            else:
                print("Invalid response for column filtering. Reverting to general keyword search.", file=sys.stderr)
                # filter_column remains None
    else: # If no keywords were provided at all
        print("No keywords provided. No filtering will be performed. All records will be exported.", file=sys.stderr)
        # filter_column remains None, keywords remains empty, so no filtering will happen in process_chunk_for_filtering

    # --- Step 4: Get filename for filtered content (JSONL) ---
    # Default filename now includes current date, "ServiceMetrics", and keywords if filtering is performed
    filtered_output_filename = get_output_filename_with_defaults(
        DEFAULT_FILTERED_OUTPUT_FILENAME_PREFIX_TEMPLATE.format(current_date_ymd),
        filter_column if filter_column else None, # Pass column only if it's explicitly chosen
        filter_keywords_input if keywords else None # Pass keywords input only if actual keywords exist
    )
    filtered_output_path = os.path.join(os.getcwd(), filtered_output_filename)

    print(f"\n--- Starting Content Filtering ---")
    if keywords: # Check if there are actual keywords to filter by
        if filter_column:
            print(f"Filtering content from '{raw_output_path}' based on column '{filter_column}' and keywords '{filter_keywords_input}'...")
        else:
            print(f"Filtering content from '{raw_output_path}' based on general keywords '{filter_keywords_input}' across entire lines...")
    else:
        print(f"No filtering requested. All records from '{raw_output_path}' will be written to '{filtered_output_path}'.")

    # --- Step 5: Perform Parallel Streaming Filtering and Writing ---
    records_exported_count = 0
    start_time = time.time()
    
    # Determine the number of parallel workers for file processing
    NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4 # Default to 4 if cannot detect
    print(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors for filtering.")

    content_lines = decompressed_data.splitlines()
    total_lines = len(content_lines)
    chunk_size_lines = (total_lines + NUM_PARALLEL_PROCESSORS - 1) // NUM_PARALLEL_PROCESSORS
    
    chunks_data = []
    for i in range(NUM_PARALLEL_PROCESSORS):
        start_line_idx = i * chunk_size_lines
        end_line_idx = min((i + 1) * chunk_size_lines, total_lines)
        if start_line_idx < end_line_idx:
            chunks_data.append("\n".join(content_lines[start_line_idx:end_line_idx]))

    try:
        with open(filtered_output_path, 'w', encoding='utf-8') as outfile:
            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_PARALLEL_PROCESSORS) as executor:
                # Submit tasks for each chunk of data
                futures = [executor.submit(process_chunk_for_filtering, chunk, filter_column, keywords) for chunk in chunks_data]
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        matching_objects_in_chunk = future.result()
                        for obj in matching_objects_in_chunk:
                            outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')
                            records_exported_count += 1
                        
                        # Progress indicator for writing phase
                        if records_exported_count % 1000 == 0:
                            elapsed_time = time.time() - start_time
                            records_per_second = records_exported_count / elapsed_time if elapsed_time > 0 else 0
                            print(f"  Exported {records_exported_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")

                    except Exception as exc:
                        print(f"Error processing chunk: {exc}", file=sys.stderr)
        
        print(f"Successfully exported {records_exported_count} records to {filtered_output_path}.")

    except Exception as e:
        print(f"Error during streaming export to {filtered_output_path}: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nScript finished.")
