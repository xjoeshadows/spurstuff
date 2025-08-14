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

def get_output_filename(current_date_ymd, current_time_hms, base_feed_name, filter_criteria, overall_match_type):
    """
    Prompts the user for an output filename, offering a default based on filter criteria.
    The format is YYYYMMDD[HHMMSS][inputfilename]Key1Keyword1Key2Keyword2.json
    """
    filename_parts = [current_date_ymd]
    
    # Add timestamp only for AnonResRT and AnonymousResidentialRT feeds
    if (base_feed_name == "AnonResRT" or base_feed_name == "AnonymousResidentialRT") and current_time_hms:
        filename_parts.append(current_time_hms)
        
    filename_parts.append(base_feed_name)

    if filter_criteria: # Only append filter parts if filtering is active
        for i, criterion in enumerate(filter_criteria):
            key_name = criterion['key']
            kws = criterion['keywords']
            # match_type_keywords = criterion['match_type_keywords'] # Not directly used in filename anymore

            if key_name: # If it's a key-specific filter
                sanitized_key_name = re.sub(r'[^a-zA-Z0-9]', '', key_name).title()
                if sanitized_key_name:
                    filename_parts.append(sanitized_key_name)
            
            if kws:
                sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in kws][:3] # Take up to 3 keywords
                if sanitized_keywords:
                    filename_parts.append("".join(sanitized_keywords))
            
            # Add overall AND/OR indicator if multiple criteria and not the last one
            # This logic is now handled by the overall_match_type for the whole filename
            # if len(filter_criteria) > 1 and i < len(filter_criteria) - 1:
            # filename_parts.append(overall_match_type.upper())

    # Add overall match type to filename if there are multiple criteria
    if len(filter_criteria) > 1:
        filename_parts.append(overall_match_type.upper())

    default_filename = "".join(filename_parts) + ".json"
    
    prompt_message = f"Enter the desired output file name (e.g., {default_filename}): "
    user_output_filename = input(prompt_message).strip()

    if not user_output_filename:
        print(f"No filename provided. Using default: {default_filename}")
        return default_filename
    else:
        sanitized_filename = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".json"):
            sanitized_filename += ".json"
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

def process_file_chunk(args_tuple):
    """
    Processes a specific byte range (chunk) of a file,
    filters JSON objects based on multiple criteria (logical AND/OR),
    and returns matching ones.
    This function is designed to be run in a separate process.
    """
    filepath, start_byte, end_byte, filter_criteria, overall_match_type = args_tuple
    
    matching_objects = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(start_byte)
        
        current_byte = start_byte
        for line in f:
            line_stripped = line.strip()
            current_byte += len(line.encode('utf-8'))
            
            if not line_stripped:
                if current_byte >= end_byte:
                    break
                continue
            
            try:
                json_obj = json.loads(line_stripped)
                
                # Evaluate each filter criterion
                criterion_results = [] # Stores True/False for each filter condition
                for criterion in filter_criteria:
                    key_name = criterion['key']
                    kws = criterion['keywords']
                    match_type_keywords = criterion['match_type_keywords']

                    source_value_raw = ""
                    
                    # Logic to handle searching across flattened keys
                    flattened_obj = flatten_json(json_obj)
                    
                    # New logic to handle simplified key names
                    if key_name.startswith('tunnels_'):
                        sub_key = key_name.split('_', 1)[1]
                        # Find all flattened keys that match the simplified key
                        relevant_keys = [k for k in flattened_obj.keys() if k.startswith('tunnels_') and k.endswith(f'_{sub_key}')]
                        
                        source_values = [str(flattened_obj[k]) for k in relevant_keys if k in flattened_obj]
                        source_value_raw = ','.join(source_values)
                    else:
                        source_value_raw = str(flattened_obj.get(key_name, ''))

                    # Evaluate keywords within this single criterion
                    current_kws_match_status = False # Default for OR, will be True for AND
                    if match_type_keywords == 'AND':
                        current_kws_match_status = True # Assume true, set false if any fails
                        for kw in kws:
                            num_match = re.match(r'([<>]?=?)\s*(\-?\d+(\.\d+)?)$', kw, re.IGNORECASE)
                            if num_match:
                                operator = num_match.group(1)
                                target_num_str = num_match.group(2)
                                try:
                                    target_num = float(target_num_str)
                                    actual_num = float(source_value_raw)
                                    if operator == '>':
                                        if not (actual_num > target_num): current_kws_match_status = False; break
                                    elif operator == '<':
                                        if not (actual_num < target_num): current_kws_match_status = False; break
                                    elif operator == '>=':
                                        if not (actual_num >= target_num): current_kws_match_status = False; break
                                    elif operator == '<=':
                                        if not (actual_num <= target_num): current_kws_match_status = False; break
                                    elif operator == '=' or operator == '':
                                        if not (actual_num == target_num): current_kws_match_status = False; break
                                except ValueError:
                                    current_kws_match_status = False; break
                            else: # Not numerical, perform substring match
                                if kw.lower() not in source_value_raw.lower(): # Case-insensitive substring match
                                    current_kws_match_status = False; break
                    elif match_type_keywords == 'OR':
                        current_kws_match_status = False # Assume false, set true if any passes
                        for kw in kws:
                            num_match = re.match(r'([<>]?=?)\s*(\-?\d+(\.\d+)?)$', kw, re.IGNORECASE)
                            if num_match:
                                operator = num_match.group(1)
                                target_num_str = num_match.group(2)
                                try:
                                    target_num = float(target_num_str)
                                    actual_num = float(source_value_raw)
                                    if operator == '>':
                                        if (actual_num > target_num): current_kws_match_status = True; break
                                    elif operator == '<':
                                        if (actual_num < target_num): current_kws_match_status = True; break
                                    elif operator == '>=':
                                        if (actual_num >= target_num): current_kws_match_status = True; break
                                    elif operator == '<=':
                                        if (actual_num <= target_num): current_kws_match_status = True; break
                                    elif operator == '=' or operator == '':
                                        if (actual_num == target_num): current_kws_match_status = True; break
                                except ValueError:
                                    pass # Continue to next keyword if numerical comparison fails
                            else: # Not numerical, perform substring match
                                if kw.lower() in source_value_raw.lower(): # Case-insensitive substring match
                                    current_kws_match_status = True; break
                    
                    criterion_results.append(current_kws_match_status)

                # Combine results of all criteria based on overall_match_type
                final_match = False
                if overall_match_type == 'AND':
                    final_match = all(criterion_results)
                elif overall_match_type == 'OR':
                    final_match = any(criterion_results)
                
                if final_match:
                    matching_objects.append(json_obj)
            except json.JSONDecodeError:
                pass 
            except Exception:
                pass
            
            if current_byte >= end_byte:
                break
    return matching_objects

def download_and_decompress_gz_to_file(url, token, output_path):
    """
    Downloads a .gz file from the given URL, decompresses it in chunks, and saves it to output_path.
    This approach is more memory-efficient for very large files.
    """
    headers = {"Token": token}
    try:
        print(f"Downloading from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        with open(output_path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        print(f"Successfully downloaded gzipped file to: {output_path}")

        decompressed_file_path = os.path.splitext(output_path)[0]
        if not decompressed_file_path.lower().endswith('.json'):
            decompressed_file_path += '.json'

        print(f"Decompressing {output_path} to {decompressed_file_path}...")
        
        # Decompress in chunks to be memory-efficient
        with gzip.open(output_path, 'rb') as f_in:
            with open(decompressed_file_path, 'wb') as f_out:
                buffer_size = 1024 * 1024  # 1MB buffer
                while True:
                    chunk = f_in.read(buffer_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
        
        print(f"Successfully decompressed to {decompressed_file_path}")
        
        try:
            os.remove(output_path)
            print(f"Deleted temporary gzipped file: {output_path}")
        except OSError as e:
            print(f"Error deleting gzipped file {output_path}: {e}", file=sys.stderr)

        return decompressed_file_path
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
        response.raise_for_status()

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
    script_start_time = time.time()

    if os.environ.get('TOKEN') is None:
        print("Error: TOKEN environment variable not set. Please set it to your Spur API token.", file=sys.stderr)
        sys.exit(1)

    while True:
        current_date_ymd = datetime.date.today().strftime("%Y%m%d")
        current_time_hms = None # Initialize timestamp for non-realtime feeds
        decompressed_source_file_path = None
        base_feed_name = "UnknownFeed" 
        is_feed_json = True # Assume JSON by default
        
        # --- Step 1: Get or Download Feed ---
        use_existing_file_input = input("Do you want to use an existing Spur Feed file? (Y/N): ").strip().upper()

        if use_existing_file_input == 'Y':
            provided_file_path = input("Please enter the full path to your Spur Feed file (e.g., '/path/to/20240610Anonymous-Residential.json' or '/path/to/20240610123000AnonResRT.json'): ").strip()
            if not os.path.exists(provided_file_path):
                print(f"Error: Provided input file '{provided_file_path}' not found. Exiting.", file=sys.stderr)
                sys.exit(1)
            
            decompressed_source_file_path = provided_file_path
            print(f"Using provided file: {decompressed_source_file_path}")

            # Updated regex to explicitly include "ServiceMetricsAll"
            match = re.search(r'(\d{8})(\d{6})?(AnonRes|AnonResRT|Anonymous|IPGeoMMDB|IPGeoJSON|ServiceMetricsAll|DCH|AnonymousIPv6|AnonymousResidentialIPv6|AnonymousResidential|AnonymousResidentialRT|IPSummary|SimilarIPs)\.(json|mmdb|json\.gz)$', os.path.basename(provided_file_path), re.IGNORECASE)
            if match:
                current_date_ymd = match.group(1)
                # Check if the optional timestamp group exists and is not None
                if match.group(2):
                    current_time_hms = match.group(2)
                base_feed_name = match.group(3)
                # Determine if the existing file is JSON based on its detected feed type
                if base_feed_name in ["IPGeoMMDB"]: # Add other non-JSON feeds here if necessary
                    is_feed_json = False
            else:
                name_without_ext = os.path.splitext(os.path.basename(provided_file_path))[0]
                # Try to extract base feed name, excluding potential date and timestamp
                base_feed_name_candidate = re.sub(r'^\d{8}(\d{6})?', '', name_without_ext)
                if base_feed_name_candidate:
                    base_feed_name = base_feed_name_candidate
                    # Attempt to normalize the base_feed_name if it was parsed as 'AnonRes' variants
                    if "AnonRes" in base_feed_name and "AnonResRT" not in base_feed_name: # Exclude AnonResRT from this replacement
                        base_feed_name = base_feed_name.replace("AnonRes", "AnonymousResidential")
                    
                else:
                    base_feed_name = "CustomFeed"
                print(f"Warning: Could not extract date and standard FeedName from provided filename. Using derived name '{base_feed_name}'.", file=sys.stderr)
                # If we couldn't parse it, assume it's JSON unless its extension is .mmdb
                if not provided_file_path.lower().endswith('.json') and not provided_file_path.lower().endswith('.json.gz'):
                    is_feed_json = False


        elif use_existing_file_input == 'N':
            feed_options = {
                "1": {"name": "Anonymous (Latest)", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "2": {"name": "Anonymous IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-ipv6/latest.json.gz", "base_feed_name": "AnonymousIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "3": {"name": "Anonymous (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous/{}/feed.json.gz", "base_feed_name": "AnonymousHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "4": {"name": "Anonymous-Residential (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_feed_name": "AnonymousResidential", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "5": {"name": "Anonymous-Residential IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential-ipv6/latest.json.gz", "base_feed_name": "AnonymousResidentialIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "6": {"name": "Anonymous-Residential (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/realtime/{}/0000.json.gz", "base_feed_name": "AnonymousResidentialHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "7": {"name": "Anonymous-Residential Realtime (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_feed_name": "AnonResRT", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True}, # Base name kept as AnonResRT for filename consistency with timestamp
                "8": {"name": "Anonymous-Residential Realtime (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/realtime/{}/{}.json.gz", "base_feed_name": "AnonymousResidentialRT", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "9": {"name": "IPGeo (MMDB - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.mmdb", "base_feed_name": "IPGeoMMDB", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False, "is_json": False},
                "10": {"name": "IPGeo (JSON - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.json.gz", "base_feed_name": "IPGeoJSON", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "11": {"name": "Data Center Hosting (DCH) (Latest)", "url": "https://feeds.spur.us/v2/dch/latest.json.gz", "base_feed_name": "DCH", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "12": {"name": "Service Metrics (Latest)", "url": "https://feeds.spur.us/v2/service-metrics/latest.json.gz", "base_feed_name": "ServiceMetricsAll", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "13": {"name": "Service Metrics (Historical)", "url_template": "https://feeds.spur.us/v2/service-metrics/{}/feed.json.gz", "base_feed_name": "ServiceMetricsAllHist", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "14": {"name": "IPSummary (Latest)", "url": "https://feeds.spur.us/v2/ipsummary/latest.json.gz", "base_feed_name": "IPSummary", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "15": {"name": "Similar IPs (Latest)", "url": "https://feeds.spur.us/v1/similar-ips/latest.json.gz", "base_feed_name": "SimilarIPs", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
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

            api_url = selected_feed.get("url")
            base_feed_name = selected_feed["base_feed_name"]
            needs_decompression = selected_feed["needs_decompression"]
            output_ext = selected_feed["output_ext"]
            is_historical = selected_feed["is_historical"]
            is_feed_json = selected_feed["is_json"] # Set is_feed_json based on selection

            if is_historical:
                date_input_valid = False
                while not date_input_valid:
                    historical_date_ymd = input("Enter the date for the historical feed in YYYYMMDD format (e.g., 20231231): ").strip()
                    if re.fullmatch(r'\d{8}', historical_date_ymd):
                        try:
                            datetime.datetime.strptime(historical_date_ymd, "%Y%m%d")
                            
                            if base_feed_name == "AnonymousResidentialRT":
                                historical_time_hhmm = input("Enter the time in HHMM format (e.g., 1430): ").strip()
                                if re.fullmatch(r'\d{4}', historical_time_hhmm):
                                    api_url = selected_feed["url_template"].format(historical_date_ymd, historical_time_hhmm)
                                    current_time_hms = historical_time_hhmm + '00' # Add seconds for filename consistency
                                    date_input_valid = True
                                else:
                                    print("Invalid time format. Please enter HHMM format.")
                            else:
                                api_url = selected_feed["url_template"].format(historical_date_ymd)
                                date_input_valid = True
                                
                            if date_input_valid:
                                current_date_ymd = historical_date_ymd
                                
                        except ValueError:
                            print("Invalid date. Please enter a real date in YYYYMMDD format.")
                    else:
                        print("Invalid format. Please enter the date in YYYYMMDD format (e.g., 20231231).")


            download_filename = f"{current_date_ymd}"
            if base_feed_name == "AnonResRT" or base_feed_name == "AnonymousResidentialRT":
                # The current_time_hms variable is already set for historical RT
                if current_time_hms:
                    download_filename += f"{current_time_hms}"
                else:
                    current_time_hms = datetime.datetime.now().strftime("%H%M%S")
                    download_filename += f"{current_time_hms}"
            
            # Adjust base_feed_name for consistent filename generation if it's the old 'AnonRes'
            if base_feed_name == "AnonRes": # Original base name for AnonRes (Latest)
                download_filename += "AnonymousResidential"
            elif base_feed_name == "AnonResHist": # Original base name for Anonymous-Residential (Historical)
                download_filename += "AnonymousResidentialHist"
            else: # For all other feeds including the new IPv6 ones and AnonResRT
                download_filename += base_feed_name


            if needs_decompression:
                download_filename += ".json.gz"
                decompressed_source_file_path = download_and_decompress_gz_to_file(api_url, os.environ.get('TOKEN'), download_filename)
            else:
                download_filename += output_ext
                decompressed_source_file_path = download_raw_file_to_disk(api_url, os.environ.get('TOKEN'), download_filename)
            
            if decompressed_source_file_path is None:
                retry_input = input("Failed to download or decompress the feed. Would you like to try a different feed? (Y/N): ").strip().upper()
                if retry_input == 'Y':
                    continue # Restart the loop for feed selection
                else:
                    print("Exiting.")
                    sys.exit(1)

        else:
            print("Invalid response. Please answer 'Y' or 'N'. Exiting.", file=sys.stderr)
            sys.exit(1)

        if not decompressed_source_file_path or not os.path.exists(decompressed_source_file_path):
            print(f"Critical Error: Source data file '{decompressed_source_file_path}' could not be located or created. Exiting.", file=sys.stderr)
            sys.exit(1)
        
        # Check if the feed is JSON and if filtering can proceed
        if not is_feed_json:
            print(f"The selected feed '{os.path.basename(decompressed_source_file_path)}' is not a JSON file. No filtering will be performed.")
            print(f"The downloaded file is located at: {decompressed_source_file_path}")
            print("Script finished.")
            script_end_time = time.time()
            total_elapsed_seconds = script_end_time - script_start_time
            minutes = int(total_elapsed_seconds // 60)
            seconds = int(total_elapsed_seconds % 60)
            print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
            sys.exit(0)
        
        # Original JSON check (can be simplified if is_feed_json handles all cases)
        if not decompressed_source_file_path.lower().endswith('.json') and is_feed_json:
            print(f"Error: The selected feed '{os.path.basename(decompressed_source_file_path)}' is expected to be a JSON file but its extension is not .json.", file=sys.stderr)
            print("Please ensure you selected a JSON feed or provided an existing JSON file.", file=sys.stderr)
            sys.exit(1)
        break # Break the outer while loop if file is successfully handled

    filter_criteria = []
    
    perform_initial_filter_choice = input("\nDo you want to filter the data? (Y/N): ").strip().upper()

    if perform_initial_filter_choice == 'Y':
        while True:
            current_filter_key = None 
            current_keywords_input = None
            current_keywords = []
            current_match_type_keywords = 'AND' # Default to AND for keywords within a single criterion

            perform_key_specific_filter_choice = input("  Filter by a specific key (Y/N)? ").strip().upper() 

            if perform_key_specific_filter_choice == 'Y':
                print("\n--- Analyzing sample data for filterable keys ---") 
                sample_lines = []
                try:
                    with open(decompressed_source_file_path, 'r', encoding='utf-8') as f_sample:
                        for _ in range(500000): # Sample up to 500,000 lines
                            line = f_sample.readline()
                            if not line: break
                            sample_lines.append(line)
                except Exception as e:
                    print(f"Error reading sample lines from {decompressed_source_file_path}: {e}", file=sys.stderr)
                    sample_lines = []

                suggested_keys = set()
                flattened_keys = set()
                for line in sample_lines:
                    try:
                        obj = json.loads(line.strip())
                        temp_flattened = flatten_json(obj)
                        flattened_keys.update(temp_flattened.keys())
                        for key in temp_flattened.keys():
                            match = re.match(r'(.+?)_\d+_(.+)', key)
                            if match:
                                simplified_key = f"{match.group(1)}_{match.group(2)}"
                                suggested_keys.add(simplified_key)
                            else:
                                suggested_keys.add(key)
                    except json.JSONDecodeError:
                        pass
                
                if suggested_keys:
                    print("\nAvailable keys for filtering (simplified names, sampled from first 500,000 lines):") 
                    for key in sorted(list(suggested_keys)):
                        print(f"  - {key}")
                    print("\n")
                else:
                    print("\nCould not determine key names from sample. Please enter key name carefully.") 

                current_filter_key = input("  Enter the exact key name for this filter (e.g., 'client_behaviors', 'tunnels_operator', 'ip'): ").strip()
                if not current_filter_key:
                    print("  No key name provided for this filter. Skipping this filter condition.", file=sys.stderr)
                    continue

                # Logic to handle sampling for both standard and simplified keys
                see_sample_values = input(f"  Would you like to see a sample of values for the key '{current_filter_key}'? (Y/N): ").strip().upper()
                if see_sample_values == 'Y':
                    print(f"\n--- Sampling values for key '{current_filter_key}' (from first 500,000 lines) ---")
                    unique_values = set()
                    
                    # Determine which flattened keys correspond to the user's input key
                    target_flattened_keys = []
                    match = re.match(r'(.+?)_(.+)', current_filter_key)
                    if match:
                        root_key, sub_key = match.groups()
                        for f_key in flattened_keys:
                            if f_key.startswith(f"{root_key}_") and f_key.endswith(f"_{sub_key}"):
                                target_flattened_keys.append(f_key)
                    else:
                        if current_filter_key in flattened_keys:
                            target_flattened_keys.append(current_filter_key)
                    
                    if target_flattened_keys:
                        for line in sample_lines:
                            try:
                                obj = json.loads(line.strip())
                                flattened_obj = flatten_json(obj)
                                for f_key in target_flattened_keys:
                                    value = flattened_obj.get(f_key)
                                    if value is not None:
                                        if isinstance(value, str):
                                            individual_values = value.split(',')
                                            for individual_value in individual_values:
                                                unique_values.add(individual_value.strip())
                                        else:
                                            unique_values.add(str(value).strip())
                            except json.JSONDecodeError:
                                pass

                    if unique_values:
                        print("Unique values found:")
                        for value in sorted(list(unique_values)):
                            print(f"  - {value}")
                        print("\n")
                    else:
                        print("No values found for this key in the sample data.\n")
                    
                    # New prompt after sampling values
                    proceed_with_key_filter = input(f"  Would you like to proceed filtering for this key? (Y/N): ").strip().upper()
                    if proceed_with_key_filter != 'Y':
                        # This breaks out of the inner loop and restarts the outer one
                        continue 
                    # If user says Yes, continue to the keyword input part below
                        
                current_keywords_input = input(f"  Enter keywords for key '{current_filter_key}' (comma-separated, e.g., 'malicious,trojan'): ").strip()
            
            elif perform_key_specific_filter_choice == 'N':
                current_keywords_input = input("  Enter keywords for general search across lines (comma-separated, e.g., 'malicious,trojan'): ").strip()
            else:
                print("  Invalid response. Skipping this filter condition.", file=sys.stderr)
                continue
            
            if current_keywords_input:
                current_keywords = [kw.strip().lower() for kw in current_keywords_input.split(',') if kw.strip()]
                if current_keywords:
                    # Only ask for match type if there's more than one keyword
                    if len(current_keywords) > 1:
                        match_type_kws_choice = input("  Match ALL keywords (AND) or ANY keyword (OR) for this condition? (AND/OR): ").strip().upper()
                        if match_type_kws_choice in ['AND', 'OR']:
                            current_match_type_keywords = match_type_kws_choice
                        else:
                            print("  Invalid choice for keyword matching type. Defaulting to AND.", file=sys.stderr)
                            current_match_type_keywords = 'AND'
                    else:
                        current_match_type_keywords = 'AND' # Single keyword, AND/OR is irrelevant

                    filter_criteria.append({
                        'key': current_filter_key,
                        'keywords': current_keywords,
                        'match_type_keywords': current_match_type_keywords
                    })
                    print(f"  Added filter: Key='{current_filter_key if current_filter_key else 'Any'}', Keywords='{', '.join(current_keywords)}', MatchType='{current_match_type_keywords}'")
                else:
                    print("  No valid keywords provided for this filter condition. Skipping.", file=sys.stderr)
            else:
                print("  No keywords provided for this filter condition. Skipping.", file=sys.stderr)

            add_another = input("Add another filter condition (Y/N)? ").strip().upper()
            if add_another != 'Y':
                break
    
    overall_match_type = 'AND' # Default overall match type
    if len(filter_criteria) > 1:
        overall_match_type_choice = input("Apply ALL filter conditions (AND) or ANY filter condition (OR)? (AND/OR): ").strip().upper()
        if overall_match_type_choice in ['AND', 'OR']:
            overall_match_type = overall_match_type_choice
        else:
            print("Invalid choice for overall filter matching type. Defaulting to AND.", file=sys.stderr)
            overall_match_type = 'AND'

    if not filter_criteria:
        print("No filter criteria provided. All records will be exported.", file=sys.stderr)
        perform_filter = 'N'
    else:
        perform_filter = 'Y'

    # --- Step 3: Get filename for filtered content (JSONL) ---
    # The get_output_filename function now generates .json files
    filtered_output_filename = get_output_filename(
        current_date_ymd, 
        current_time_hms, # Pass the timestamp
        base_feed_name,   
        filter_criteria if perform_filter == 'Y' else [],
        overall_match_type # Pass overall match type for filename suggestion
    )
    output_file_path = os.path.join(os.getcwd(), filtered_output_filename)

    print(f"\n--- Starting Content Filtering ---")
    if perform_filter == 'Y':
        print(f"Filtering content from '{decompressed_source_file_path}' based on {len(filter_criteria)} criteria (overall '{overall_match_type}')...")
    else:
        print(f"No filtering requested. The output file is: '{decompressed_source_file_path}'.")
        print("Script finished.")
        script_end_time = time.time()
        total_elapsed_seconds = script_end_time - script_start_time
        minutes = int(total_elapsed_seconds // 60)
        seconds = int(total_elapsed_seconds % 60)
        print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
        sys.exit(0)

    # --- Step 4: Perform Parallel Streaming Filtering and Writing ---
    records_exported_count = 0
    start_time = time.time()
    
    NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4
    print(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors for filtering.")

    try:
        # Output file opened with .json extension
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            chunks = get_file_chunks(decompressed_source_file_path, NUM_PARALLEL_PROCESSORS)
            
            with multiprocessing.Pool(processes=NUM_PARALLEL_PROCESSORS) as pool:
                results_iterator = pool.imap_unordered(
                    process_file_chunk,
                    [(decompressed_source_file_path, start, end, filter_criteria, overall_match_type) for start, end in chunks]
                )
                
                for matching_objects_in_chunk in results_iterator:
                    try: 
                        for obj in matching_objects_in_chunk:
                            outfile.write(json.dumps(obj, ensure_ascii=False) + '\n')
                            records_exported_count += 1
                        
                        if records_exported_count % 1000 == 0:
                            elapsed_time = time.time() - start_time
                            records_per_second = records_exported_count / elapsed_time if elapsed_time > 0 else 0
                            print(f"  Exported {records_exported_count} records ({records_per_second:.2f} records/s) - {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))} elapsed")

                    except Exception as exc:
                        print(f"Error processing chunk result: {exc}", file=sys.stderr)
            
            print(f"Successfully exported {records_exported_count} records to {output_file_path}.")

    except Exception as e:
        print(f"Error during streaming export to {output_file_path}: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nScript finished.")

    script_end_time = time.time()
    total_elapsed_seconds = script_end_time - script_start_time
    minutes = int(total_elapsed_seconds // 60)
    seconds = int(total_elapsed_seconds % 60)
    print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
