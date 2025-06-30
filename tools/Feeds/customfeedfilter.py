#!/usr/bin/env python3
import os
import subprocess
import datetime
import sys
import json
import re
import concurrent.futures
import time

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

def get_output_filename(default_prefix, filter_column_name=None, filter_keywords=None):
    """
    Prompts the user for an output filename, offering a default based on filter criteria.
    Capitalizes the first letter of each word for the column name and keywords in the default filename.
    """
    default_name_parts = [default_prefix]
    if filter_column_name:
        # Sanitize column name for filename, then capitalize the first letter of each word
        sanitized_col_name = re.sub(r'[^a-zA-Z0-9]', '', filter_column_name).title()
        if sanitized_col_name:
            default_name_parts.append(sanitized_col_name)
    if filter_keywords:
        # Take first few keywords for filename, sanitize, then capitalize the first letter of each word
        sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in filter_keywords.split(',') if kw.strip()][:3]
        if sanitized_keywords:
            default_name_parts.append("".join(sanitized_keywords))
    
    default_filename = "".join(default_name_parts) + ".jsonl"
    
    prompt_message = f"Enter the desired output file name (e.g., {default_filename}): "
    user_output_filename = input(prompt_message).strip()

    if not user_output_filename:
        print(f"No output filename provided. Using default: {default_filename}")
        return default_filename
    else:
        sanitized_filename = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".jsonl"):
            sanitized_filename += ".jsonl"
        return sanitized_filename

def count_lines_in_file(filepath):
    """Counts the number of lines in a file."""
    count = 0
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            count += 1
    return count

def get_file_chunks(filepath, num_chunks):
    """
    Determines byte offsets for splitting a file into roughly equal chunks.
    This is useful for parallel processing.
    """
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    
    chunks = []
    with open(filepath, 'rb') as f: # Open in binary mode for seeking
        for i in range(num_chunks):
            start_byte = i * chunk_size
            end_byte = (i + 1) * chunk_size if i < num_chunks - 1 else file_size
            
            # Adjust start_byte to the beginning of a line if not the very first chunk
            if start_byte > 0:
                f.seek(start_byte - 1) # Go back one byte
                # Read until newline to find the start of the next full line
                f.readline() 
                start_byte = f.tell() # Current position is the start of a line
            
            chunks.append((start_byte, end_byte))
    return chunks

def process_file_chunk(filepath, start_byte, end_byte, filter_column, keywords):
    """
    Processes a specific byte range (chunk) of a file,
    filters JSON objects, and returns matching ones.
    """
    matching_objects = []
    # Open with 'errors=ignore' for robustness against corrupted parts in large files
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(start_byte)
        
        # Read until end_byte or EOF
        current_byte = start_byte
        for line in f:
            line_stripped = line.strip()
            current_byte += len(line) # Account for newline char
            
            if not line_stripped:
                if current_byte >= end_byte: # Stop if we've passed the end_byte
                    break
                continue
            
            try:
                json_obj = json.loads(line_stripped)
                
                should_export = True
                if filter_column and keywords: # Only filter if criteria are provided
                    flattened_obj = flatten_json(json_obj)
                    column_value = str(flattened_obj.get(filter_column, '')).lower()
                    if not any(kw in column_value for kw in keywords):
                        should_export = False
                
                if should_export:
                    matching_objects.append(json_obj)
            except json.JSONDecodeError as e_line:
                # Suppress warnings for malformed JSON lines in chunks for cleaner output during parallel processing
                # print(f"Warning: Skipping malformed JSON line in chunk (byte {current_byte-len(line)}): {e_line} in '{line_stripped[:80]}...'", file=sys.stderr)
                pass
            except Exception as e_other:
                # Suppress warnings for other unexpected errors in chunks
                # print(f"Warning: An unexpected error occurred in chunk (byte {current_byte-len(line)}): {e_other} in '{line_stripped[:80]}...'", file=sys.stderr)
                pass
            
            if current_byte >= end_byte: # Stop if we've passed the end_byte
                break
    return matching_objects

# --- Main Script Logic ---
if __name__ == "__main__":
    current_date = datetime.datetime.now().strftime("%Y%m%d") 
    decompressed_source_file = None
    base_feed_name = "UnknownFeed" 
    
    # Prompt user for input method
    use_existing_file_input = input("Do you want to use an existing Spur Feed file? (Y/N): ").strip().upper()

    if use_existing_file_input == 'Y':
        provided_file_path = input("Please enter the full path to your Spur Feed file (e.g., '/path/to/20240610AnonRes.json'): ").strip()
        if not os.path.exists(provided_file_path):
            print(f"Error: Provided input file '{provided_file_path}' not found. Exiting.", file=sys.stderr)
            sys.exit(1)
        
        decompressed_source_file = provided_file_path
        print(f"Using provided file: {decompressed_source_file}")

        match = re.search(r'(\d{8})(AnonRes|AnonResRT|Anonymous)\.json$', os.path.basename(provided_file_path))
        if match:
            current_date = match.group(1)
            base_feed_name = match.group(2)
            print(f"Extracted date '{current_date}' and feed '{base_feed_name}' from the provided filename for output files.")
        else:
            name_without_ext = os.path.splitext(os.path.basename(provided_file_path))[0]
            base_feed_name_candidate = re.sub(r'^\d{8}', '', name_without_ext)
            if base_feed_name_candidate:
                base_feed_name = base_feed_name_candidate
            else:
                base_feed_name = "CustomFeed"

            print(f"Warning: Could not extract date and standard FeedName from provided filename '{os.path.basename(provided_file_path)}'. Using current system date '{current_date}' and derived feed name '{base_feed_name}' for output files. Please ensure your filename follows the YYYYMMDD<FeedName>.json format for consistent results.", file=sys.stderr)

    elif use_existing_file_input == 'N':
        TOKEN = os.environ.get('TOKEN')
        if not TOKEN:
            print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable to download the file.", file=sys.stderr)
            sys.exit(1)

        feed_options = {
            "1": {"name": "AnonRes", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_feed_name": "AnonRes"},
            "2": {"name": "AnonRes Realtime", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_feed_name": "AnonResRT"},
            "3": {"name": "Anonymous", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_feed_name": "Anonymous"},
        }

        selected_feed = None
        while selected_feed is None:
            print("\nPlease select a feed to download:")
            for key, value in feed_options.items():
                print(f"  {key}: {value['name']}")
            
            choice = input("Enter the number corresponding to your choice: ").strip()
            selected_feed = feed_options.get(choice)
            if selected_feed is None:
                print("Invalid choice. Please enter a number from the list.")

        api_url = selected_feed["url"]
        base_feed_name = selected_feed["base_feed_name"]

        gz_filename = f"{current_date}{base_feed_name}.json.gz"
        decompressed_source_file = f"{current_date}{base_feed_name}.json" 

        print(f"Fetching {api_url} to {gz_filename}...")
        try:
            curl_command = [
                "curl",
                "--location", api_url,
                "--header", f"Token: {TOKEN}",
                "--output", gz_filename
            ]
            subprocess.run(curl_command, check=True)
            print(f"Successfully downloaded {gz_filename}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading file: {e}", file=sys.stderr)
            sys.exit(1)

        print(f"Decompressing {gz_filename}...")
        try:
            subprocess.run(["gunzip", "-df", gz_filename], check=True)
            print(f"Successfully decompressed {gz_filename} to {decompressed_source_file}")
            try:
                os.remove(gz_filename)
                print(f"Deleted temporary gzipped file: {gz_filename}")
            except OSError as e:
                print(f"Error deleting gzipped file {gz_filename}: {e}", file=sys.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error decompressing file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Invalid response. Please answer 'Y' or 'N'. Exiting.", file=sys.stderr)
        sys.exit(1)

    if not decompressed_source_file or not os.path.exists(decompressed_source_file):
        print(f"Critical Error: Source data file '{decompressed_source_file}' could not be located or created. Exiting.", file=sys.stderr)
        sys.exit(1)

    # --- User-defined Filtering ---
    filter_column = None
    filter_keywords_input = None
    keywords = [] # Initialize keywords list

    perform_filter = input("\nDo you want to filter the data by a specific column and keywords? (Y/N): ").strip().upper()

    if perform_filter == 'Y':
        # Read the first few lines to suggest column names without loading the whole file
        sample_lines = []
        try:
            with open(decompressed_source_file, 'r', encoding='utf-8') as f_sample:
                for _ in range(10): # Read first 10 lines to sample
                    line = f_sample.readline()
                    if not line: break
                    sample_lines.append(line)
        except Exception as e:
            print(f"Error reading sample lines from {decompressed_source_file}: {e}", file=sys.stderr)
            sample_lines = []

        suggested_keys = set()
        for line in sample_lines:
            try:
                obj = json.loads(line.strip())
                flattened_obj = flatten_json(obj)
                suggested_keys.update(flattened_obj.keys())
            except json.JSONDecodeError:
                pass

        if suggested_keys:
            print("\nAvailable columns for filtering (flattened names, sampled from first few lines):")
            for key in sorted(list(suggested_keys)):
                print(f"  - {key}")
            print("\n")
        else:
            print("\nCould not determine column names from sample. Please enter column name carefully.")

        filter_column = input("Enter the exact column name to filter (e.g., 'client_behaviors', 'ip', 'organization'): ").strip()
        if not filter_column:
            print("No column name provided. Proceeding without filtering.", file=sys.stderr)
            perform_filter = 'N' # Revert to 'N' if no column given
        else:
            filter_keywords_input = input(f"Enter keywords to filter by (comma-separated, e.g., 'malicious,trojan'): ").strip()
            if not filter_keywords_input:
                print("No keywords provided. Proceeding without filtering.", file=sys.stderr)
                perform_filter = 'N' # Revert to 'N' if no keywords given
            else:
                keywords = [kw.strip().lower() for kw in filter_keywords_input.split(',') if kw.strip()]
                if not keywords:
                    print("No valid keywords after parsing. Proceeding without filtering.", file=sys.stderr)
                    perform_filter = 'N' # Revert to 'N' if no valid keywords
    
    # --- Determine Output Filename BEFORE processing ---
    output_prefix = f"{current_date}{base_feed_name}"
    # Pass actual filter_column and keywords if filtering is active, else None
    output_file_path = os.path.join(os.getcwd(), get_output_filename(output_prefix, filter_column if perform_filter == 'Y' else None, filter_keywords_input if perform_filter == 'Y' else None))
    
    print(f"\nExporting records to {output_file_path}...")
    
    # --- Perform Streaming Filtering and Writing ---
    records_exported_count = 0
    start_time = time.time()
    
    # Determine the number of parallel workers for file processing
    # Using os.cpu_count() for a reasonable default, can be adjusted
    NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4 # Default to 4 if cannot detect
    print(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors for filtering.")

    # Get file chunks for parallel processing
    chunks = get_file_chunks(decompressed_source_file, NUM_PARALLEL_PROCESSORS)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_PARALLEL_PROCESSORS) as executor:
        # Submit tasks for each chunk
        # Pass filter_column and keywords (which will be empty if no filtering is performed)
        future_to_chunk = {executor.submit(process_file_chunk, decompressed_source_file, start, end, filter_column if perform_filter == 'Y' else None, keywords if perform_filter == 'Y' else None): (start, end) for start, end in chunks}
        
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_start, chunk_end = future_to_chunk[future]
                try:
                    matching_objects_in_chunk = future.result()
                    for obj in matching_objects_in_chunk:
                        outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')
                        records_exported_count += 1
                    
                    # Progress indicator for writing phase
                    if records_exported_count % 1000 == 0: # Print progress every 1000 records
                        elapsed_time = time.time() - start_time
                        records_per_second = records_exported_count / elapsed_time if elapsed_time > 0 else 0
                        print(f"  Exported {records_exported_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")

                except Exception as exc:
                    print(f"Error processing chunk (bytes {chunk_start}-{chunk_end}): {exc}", file=sys.stderr)
        
        print(f"Successfully exported {records_exported_count} records to {output_file_path}.")

    print("\nScript finished.")
