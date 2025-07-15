#!/usr/bin/env python3
import os
import subprocess
import datetime
import sys
import json
import re
import concurrent.futures # Still useful for as_completed
import multiprocessing # For true CPU parallelism
import time
import requests # For downloading feeds
import gzip # For decompressing feeds
from io import BytesIO # For handling decompressed data in memory

# --- Configuration ---
# API Token for downloading feeds (if chosen by user)
API_TOKEN = os.environ.get('TOKEN') 

# Default filename templates
DEFAULT_RAW_OUTPUT_FILENAME_TEMPLATE = "{}_FeedRaw.json" # For raw decompressed content

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

def get_output_filename(current_date_ymd, base_feed_name, filter_criteria):
    """
    Prompts the user for an output filename, offering a default based on filter criteria.
    The format is YYYYMMDD[inputfilename]Key1Keyword1Key2Keyword2.jsonl
    """
    filename_parts = [current_date_ymd, base_feed_name]
    
    if filter_criteria: # Only append filter parts if filtering is active
        for key_name, kws in filter_criteria:
            if key_name: # If it's a key-specific filter
                sanitized_key_name = re.sub(r'[^a-zA-Z0-9]', '', key_name).title()
                if sanitized_key_name:
                    filename_parts.append(sanitized_key_name)
            
            # Keywords are always present in the filter_criteria tuple if the condition was added
            if kws:
                # Sanitize and title-case each keyword, then join them
                sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in kws][:3] # Take up to 3 keywords
                if sanitized_keywords:
                    filename_parts.append("".join(sanitized_keywords))
    
    default_filename = "".join(filename_parts) + ".jsonl"
    
    prompt_message = f"Enter the desired output file name (e.g., {default_filename}): "
    user_output_filename = input(prompt_message).strip()

    if not user_output_filename:
        print(f"No filename provided. Using default: {default_filename}")
        return default_filename
    else:
        sanitized_filename = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".jsonl"):
            sanitized_filename += ".jsonl"
        return sanitized_filename

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

def process_file_chunk(args_tuple): # Modified to accept a single tuple argument
    """
    Processes a specific byte range (chunk) of a file,
    filters JSON objects based on multiple criteria (logical AND),
    and returns matching ones.
    This function is designed to be run in a separate process.
    """
    filepath, start_byte, end_byte, filter_criteria = args_tuple # Unpack arguments
    
    matching_objects = []
    # Open with 'errors=ignore' for robustness against corrupted parts in large files
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(start_byte)
        
        # Read until end_byte or EOF
        current_byte = start_byte
        for line in f:
            line_stripped = line.strip()
            current_byte += len(line.encode('utf-8')) # Account for newline char and multi-byte chars
            
            if not line_stripped:
                if current_byte >= end_byte: # Stop if we've passed the end_byte
                    break
                continue
            
            try:
                json_obj = json.loads(line_stripped)
                
                # Apply all filter criteria (logical AND)
                all_conditions_met = True
                for key_name, kws in filter_criteria:
                    if key_name: # Key-specific filter
                        flattened_obj = flatten_json(json_obj) # Flatten to access nested keys
                        key_value = str(flattened_obj.get(key_name, '')).lower() # Get value, convert to string, lowercase
                        if not all(kw in key_value for kw in kws): # Logical AND for keywords within this key
                            all_conditions_met = False
                            break # No need to check other conditions for this object
                    else: # General line-based filter (no specific key)
                        if not all(kw in line_stripped.lower() for kw in kws): # Logical AND for keywords within this line
                            all_conditions_met = False
                            break # No need to check other conditions for this object
                
                if all_conditions_met:
                    matching_objects.append(json_obj)
            except json.JSONDecodeError as e_line:
                # Suppress detailed error for performance/clean output in parallel processing
                pass 
            except Exception as e_other:
                # Suppress detailed error for performance/clean output in parallel processing
                pass
            
            if current_byte >= end_byte: # Stop if we've passed the end_byte
                break
    return matching_objects

def download_and_decompress_gz_to_file(url, token, output_path):
    """
    Downloads a .gz file from the given URL, decompresses it, and saves it to output_path.
    """
    headers = {"Token": token}
    try:
        print(f"Downloading from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        with open(output_path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        print(f"Successfully downloaded gzipped file to: {output_path}")

        decompressed_file_path = os.path.splitext(output_path)[0] # Remove .gz extension
        if not decompressed_file_path.lower().endswith('.json'):
            decompressed_file_path += '.json'

        print(f"Decompressing {output_path} to {decompressed_file_path}...")
        with gzip.open(output_path, 'rb') as f_in:
            with open(decompressed_file_path, 'wb') as f_out:
                f_out.write(f_in.read())
        print(f"Successfully decompressed to {decompressed_file_path}")
        
        # Delete the .gz file after successful decompression
        try:
            os.remove(output_path)
            print(f"Deleted temporary gzipped file: {output_path}")
        except OSError as e:
            print(f"Error deleting gzipped file {output_path}: {e}", file=sys.stderr)

        return decompressed_file_path # Return path to decompressed file
    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}", file=sys.stderr)
        return None
    except gzip.BadGzipFile:
        print("Error: Downloaded file is not a valid gzip file.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during download/decompression: {e}", file=sys.stderr)
        return None

def download_raw_file_to_disk(url, token, output_path):
    """
    Downloads a raw file (e.g., .mmdb) from the given URL and saves it to output_path.
    """
    headers = {"Token": token}
    try:
        print(f"Downloading raw file from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        with open(output_path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        print(f"Successfully downloaded raw file to: {output_path}")
        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Error during raw file download: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during raw file download: {e}", file=sys.stderr)
        return None

# --- Main Script Logic ---
if __name__ == "__main__":
    script_start_time = time.time() # Record script start time

    if os.environ.get('TOKEN') is None: # Use os.environ.get for API_TOKEN check
        print("Error: TOKEN environment variable not set. Please set it to your Spur API token.", file=sys.stderr)
        sys.exit(1)

    current_date_ymd = datetime.date.today().strftime("%Y%m%d")
    decompressed_source_file_path = None # Path to the raw decompressed JSON file (on disk)
    base_feed_name = "UnknownFeed" 
    
    # --- Step 1: Get or Download Feed ---
    use_existing_file_input = input("Do you want to use an existing Spur Feed file? (Y/N): ").strip().upper()

    if use_existing_file_input == 'Y':
        provided_file_path = input("Please enter the full path to your Spur Feed file (e.g., '/path/to/20240610AnonRes.json'): ").strip()
        if not os.path.exists(provided_file_path):
            print(f"Error: Provided input file '{provided_file_path}' not found. Exiting.", file=sys.stderr)
            sys.exit(1)
        
        decompressed_source_file_path = provided_file_path
        print(f"Using provided file: {decompressed_source_file_path}")

        # Attempt to extract date and feed name from the provided filename
        # Expanded regex to include IPGeoMMDB and IPGeoJSON for existing file naming
        match = re.search(r'(\d{8})(AnonRes|AnonResRT|Anonymous|IPGeoMMDB|IPGeoJSON|ServiceMetrics)\.(json|mmdb|json\.gz)$', os.path.basename(provided_file_path), re.IGNORECASE)
        if match:
            current_date_ymd = match.group(1)
            base_feed_name = match.group(2)
        else:
            name_without_ext = os.path.splitext(os.path.basename(provided_file_path))[0]
            base_feed_name_candidate = re.sub(r'^\d{8}', '', name_without_ext)
            if base_feed_name_candidate:
                base_feed_name = name_without_ext # Use full name if no date prefix removed
            else:
                base_feed_name = "CustomFeed"
            print(f"Warning: Could not extract date and standard FeedName from provided filename. Using derived name '{base_feed_name}'.", file=sys.stderr)

    elif use_existing_file_input == 'N':
        feed_options = {
            "1": {"name": "AnonRes (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_feed_name": "AnonRes", "needs_decompression": True, "output_ext": ".json", "is_historical": False},
            "2": {"name": "AnonRes Realtime (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_feed_name": "AnonResRT", "needs_decompression": True, "output_ext": ".json", "is_historical": False},
            "3": {"name": "Anonymous (Latest)", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": False},
            "4": {"name": "IPGeo (MMDB - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.mmdb", "base_feed_name": "IPGeoMMDB", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False},
            "5": {"name": "IPGeo (JSON - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.json.gz", "base_feed_name": "IPGeoJSON", "needs_decompression": True, "output_ext": ".json", "is_historical": False},
            "6": {"name": "Service Metrics (Latest)", "url": "https://feeds.spur.us/v2/service-metrics/latest.json.gz", "base_feed_name": "ServiceMetrics", "needs_decompression": True, "output_ext": ".json", "is_historical": False},
            "7": {"name": "Anonymous (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous/{}/feed.json.gz", "base_feed_name": "AnonymousHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True},
            "8": {"name": "AnonRes (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/realtime/{}/0000.json.gz", "base_feed_name": "AnonResHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True},
            "9": {"name": "Service Metrics (Historical)", "url_template": "https://feeds.spur.us/v2/service-metrics/{}/feed.json.gz", "base_feed_name": "ServiceMetricsHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True},
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

        api_url = selected_feed.get("url") # Use .get for non-historical feeds
        base_feed_name = selected_feed["base_feed_name"]
        needs_decompression = selected_feed["needs_decompression"]
        output_ext = selected_feed["output_ext"]
        is_historical = selected_feed["is_historical"]

        if is_historical:
            date_input_valid = False
            while not date_input_valid:
                historical_date_ymd = input("Enter the date for the historical feed in YYYYMMDD format (e.g., 20231231): ").strip()
                if re.fullmatch(r'\d{8}', historical_date_ymd):
                    try:
                        # Validate if the date is a real date
                        datetime.datetime.strptime(historical_date_ymd, "%Y%m%d")
                        api_url = selected_feed["url_template"].format(historical_date_ymd)
                        current_date_ymd = historical_date_ymd # Use historical date for filename
                        date_input_valid = True
                    except ValueError:
                        print("Invalid date. Please enter a real date in YYYYMMDD format.")
                else:
                    print("Invalid format. Please enter the date in YYYYMMDD format (e.g., 20231231).")


        download_filename = f"{current_date_ymd}{base_feed_name}"
        if needs_decompression:
            download_filename += ".json.gz" # Original gzipped name
            decompressed_source_file_path = download_and_decompress_gz_to_file(api_url, os.environ.get('TOKEN'), download_filename)
        else:
            download_filename += output_ext # For MMDB, this is the final file name
            decompressed_source_file_path = download_raw_file_to_disk(api_url, os.environ.get('TOKEN'), download_filename)
        
        if decompressed_source_file_path is None:
            print("Failed to download or decompress the feed. Exiting.", file=sys.stderr)
            sys.exit(1)

    else:
        print("Invalid response. Please answer 'Y' or 'N'. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Validate that the source file is now available and is a JSON file for parsing
    if not decompressed_source_file_path or not os.path.exists(decompressed_source_file_path):
        print(f"Critical Error: Source data file '{decompressed_source_file_path}' could not be located or created. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # Check if the file is a JSON file, as the rest of the script assumes JSON parsing
    if not decompressed_source_file_path.lower().endswith('.json'):
        print(f"Error: The selected feed '{os.path.basename(decompressed_source_file_path)}' is not a JSON file. This script can only filter JSON feeds.", file=sys.stderr)
        print("Please select a JSON feed or provide an existing JSON file.", file=sys.stderr)
        sys.exit(1)

    # --- Step 2: Get user input for filtering ---
    filter_criteria = [] # List to store (key_name, [keywords]) tuples
    
    perform_initial_filter_choice = input("\nDo you want to filter the data? (Y/N): ").strip().upper()

    if perform_initial_filter_choice == 'Y':
        while True:
            current_filter_key = None 
            current_keywords_input = None
            current_keywords = []

            # Ask if they want to filter by a specific key for THIS filter condition
            perform_key_specific_filter_choice = input("  Filter by a specific key (Y/N)? ").strip().upper() 

            if perform_key_specific_filter_choice == 'Y':
                # Sample data to suggest key names for filtering
                print("\n--- Analyzing sample data for filterable keys ---") 
                sample_lines = []
                try:
                    with open(decompressed_source_file_path, 'r', encoding='utf-8') as f_sample:
                        for _ in range(10): # Read first 10 lines to sample
                            line = f_sample.readline()
                            if not line: break
                            sample_lines.append(line)
                except Exception as e:
                    print(f"Error reading sample lines from {decompressed_source_file_path}: {e}", file=sys.stderr)
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
                    print("\nAvailable keys for filtering (flattened names, sampled from first few lines):") 
                    for key in sorted(list(suggested_keys)):
                        print(f"  - {key}")
                    print("\n")
                else:
                    print("\nCould not determine key names from sample. Please enter key name carefully.") 

                current_filter_key = input("  Enter the exact key name for this filter (e.g., 'client_behaviors', 'ip', 'organization'): ").strip()
                if not current_filter_key:
                    print("  No key name provided for this filter. Skipping this filter condition.", file=sys.stderr)
                    continue # Skip to next iteration of while loop

                current_keywords_input = input(f"  Enter keywords for key '{current_filter_key}' (comma-separated, e.g., 'malicious,trojan'): ").strip()
            
            elif perform_key_specific_filter_choice == 'N':
                current_keywords_input = input("  Enter keywords for general search across lines (comma-separated, e.g., 'malicious,trojan'): ").strip()
                # current_filter_key remains None for general search
            else:
                print("  Invalid response. Skipping this filter condition.", file=sys.stderr)
                continue # Skip to next iteration of while loop
            
            if current_keywords_input:
                current_keywords = [kw.strip().lower() for kw in current_keywords_input.split(',') if kw.strip()]
                if current_keywords:
                    filter_criteria.append((current_filter_key, current_keywords))
                    print(f"  Added filter: Key='{current_filter_key if current_filter_key else 'Any'}', Keywords='{', '.join(current_keywords)}'")
                else:
                    print("  No valid keywords provided for this filter condition. Skipping.", file=sys.stderr)
            else:
                print("  No keywords provided for this filter condition. Skipping.", file=sys.stderr)

            add_another = input("Add another filter condition (Y/N)? ").strip().upper()
            if add_another != 'Y':
                break
    
    if not filter_criteria:
        print("No filter criteria provided. All records will be exported.", file=sys.stderr)
        perform_filter = 'N' # No filtering will be done
    else:
        perform_filter = 'Y' # Filtering will be done

    # --- Step 3: Get filename for filtered content (JSONL) ---
    filtered_output_filename = get_output_filename(
        current_date_ymd, 
        base_feed_name,   
        filter_criteria if perform_filter == 'Y' else []
    )
    output_file_path = os.path.join(os.getcwd(), filtered_output_filename)

    print(f"\n--- Starting Content Filtering ---")
    if perform_filter == 'Y':
        print(f"Filtering content from '{decompressed_source_file_path}' based on {len(filter_criteria)} criteria (logical AND)...")
    else:
        # If no filtering, the decompressed_source_file_path is the final output.
        # No new file creation or copying is needed.
        print(f"No filtering requested. The output file is: '{decompressed_source_file_path}'.")
        print("Script finished.")
        # Calculate and print total completion time (for download/decompression only)
        script_end_time = time.time()
        total_elapsed_seconds = script_end_time - script_start_time
        minutes = int(total_elapsed_seconds // 60)
        seconds = int(total_elapsed_seconds % 60)
        print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
        sys.exit(0) # Exit gracefully as no further processing is needed

    # --- Step 4: Perform Parallel Streaming Filtering and Writing ---
    records_exported_count = 0
    start_time = time.time()
    
    NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4
    print(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors for filtering.")

    try:
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            chunks = get_file_chunks(decompressed_source_file_path, NUM_PARALLEL_PROCESSORS)
            
            # Use multiprocessing.Pool for true CPU parallelism
            with multiprocessing.Pool(processes=NUM_PARALLEL_PROCESSORS) as pool:
                # Map the process_file_chunk function to each chunk
                # pool.imap_unordered is used to get results as they complete, which is good for streaming
                results_iterator = pool.imap_unordered(
                    process_file_chunk, # Pass the function directly
                    [(decompressed_source_file_path, start, end, filter_criteria) for start, end in chunks] # Pass arguments as tuples
                )
                
                for matching_objects_in_chunk in results_iterator:
                    # The 'try' block for processing chunk results should be here,
                    # and its 'except' should be at the same indentation level.
                    try: 
                        for obj in matching_objects_in_chunk:
                            outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')
                            records_exported_count += 1
                        
                        if records_exported_count % 1000 == 0:
                            elapsed_time = time.time() - start_time
                            records_per_second = records_exported_count / elapsed_time if elapsed_time > 0 else 0
                            print(f"  Exported {records_exported_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")

                    except Exception as exc: # This is the correct indentation for this except
                        print(f"Error processing chunk result: {exc}", file=sys.stderr)
            
            print(f"Successfully exported {records_exported_count} records to {output_file_path}.")

    except Exception as e: # This outer try/except is for file operations or pool creation
            print(f"Error during streaming export to {output_file_path}: {e}", file=sys.stderr)
            sys.exit(1)

    print("\nScript finished.")

    # Calculate and print total completion time
    script_end_time = time.time()
    total_elapsed_seconds = script_end_time - script_start_time
    minutes = int(total_elapsed_seconds // 60)
    seconds = int(total_elapsed_seconds % 60)
    print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
