#!/usr/bin/env python3
import os
import datetime
import sys
import json
import re
import multiprocessing # For true CPU parallelism
import time
import requests # For downloading feeds
import gzip # For decompressing feeds
import shutil # For moving files safely

# --- Configuration ---
# API Token for downloading feeds (if chosen by user)
API_TOKEN = os.environ.get('TOKEN') 

# --- Functions ---
def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary."""
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(not isinstance(item, dict) for item in v):
                items.append((new_key, ','.join(map(str, v))))
            else:
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_json(item, new_key + sep + str(i), sep=sep).items())
                    else:
                        items.append((new_key + sep + str(i), item))
        else:
            items.append((new_key, v))
    return dict(items)

def get_output_filename(current_date_ymd, current_time_hms, base_feed_name, user_filename, filter_criteria, overall_match_type):
    """Determines the output filename."""
    if user_filename:
        sanitized_filename = "".join(x for x in user_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".json"):
            sanitized_filename += ".json"
        return sanitized_filename

    filename_parts = [current_date_ymd]
    
    if (base_feed_name == "AnonResRT" or base_feed_name == "AnonymousResidentialRT") and current_time_hms:
        filename_parts.append(current_time_hms)
        
    if base_feed_name.endswith('Hist'):
        base_feed_name = base_feed_name[:-4]

    filename_parts.append(base_feed_name)

    if filter_criteria:
        for i, criterion in enumerate(filter_criteria):
            key_name = criterion['key']
            kws = criterion['keywords']

            if key_name:
                sanitized_key_name = re.sub(r'[^a-zA-Z0-9]', '', key_name).title()
                if sanitized_key_name:
                    filename_parts.append(sanitized_key_name)
            
            if kws:
                sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in kws][:3]
                if sanitized_keywords:
                    filename_parts.append("".join(sanitized_keywords))
            
    if len(filter_criteria) > 1:
        filename_parts.append(overall_match_type.upper())

    default_filename = "".join(filename_parts) + ".json"
    return default_filename

def get_file_chunks(filepath, min_chunks):
    """
    Determines byte offsets for splitting a file.
    OPTIMIZATION: Targets ~64MB chunks to prevent memory bloat in workers.
    """
    file_size = os.path.getsize(filepath)
    
    # Target chunk size: 64MB (safe for memory even with 100% match rate)
    TARGET_CHUNK_SIZE = 64 * 1024 * 1024
    
    # Calculate required chunks based on size, but ensure at least 'min_chunks' (CPU count)
    size_based_chunks = (file_size // TARGET_CHUNK_SIZE) + 1
    num_chunks = max(min_chunks, size_based_chunks)
    
    chunk_size = file_size // num_chunks
    
    chunks = []
    with open(filepath, 'rb') as f:
        for i in range(num_chunks):
            start_byte = i * chunk_size
            end_byte = (i + 1) * chunk_size if i < num_chunks - 1 else file_size
            
            if start_byte > 0:
                f.seek(start_byte - 1)
                f.readline() 
                start_byte = f.tell()
            
            if start_byte < end_byte:
                chunks.append((start_byte, end_byte))
                
    return chunks

def process_file_chunk(args_tuple):
    """
    Processes a specific byte range (chunk) of a file.
    OPTIMIZATION: Returns raw strings (line_stripped) instead of dict objects.
    """
    filepath, start_byte, end_byte, filter_criteria, overall_match_type = args_tuple
    
    matching_lines = [] 
    
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
                # We still parse to check logic, but we won't store the result
                json_obj = json.loads(line_stripped)
                
                # Evaluate each filter criterion
                criterion_results = [] 
                for criterion in filter_criteria:
                    key_name = criterion['key']
                    kws = criterion['keywords']
                    match_type_keywords = criterion['match_type_keywords']

                    flattened_obj = flatten_json(json_obj)
                    
                    source_value_raw = None 
                    source_key_found = False
                    
                    # --- Determine Source Value ---
                    if key_name.startswith('tunnels_'):
                        sub_key = key_name.split('_', 1)[1]
                        relevant_keys = [k for k in flattened_obj.keys() if k.startswith('tunnels_') and k.endswith(f'_{sub_key}')]
                        
                        if relevant_keys:
                            source_key_found = True
                            source_values = [str(flattened_obj[k]) for k in relevant_keys if k in flattened_obj and str(flattened_obj[k]).strip().lower() not in ('none', 'null')]
                            source_value_raw = ','.join(source_values)
                        
                    elif key_name:
                        if key_name in flattened_obj:
                            source_key_found = True
                            value = flattened_obj.get(key_name)
                            if value is not None:
                                str_value = str(value).strip().lower()
                                if str_value not in ('none', 'null'):
                                    source_value_raw = str_value
                        
                    else: # General search
                        source_key_found = True
                        source_value_raw = line_stripped.lower()

                    # --- Evaluate Keywords ---
                    current_kws_match_status = True if match_type_keywords == 'AND' else False
                    
                    for kw in kws:
                        individual_match = False
                        
                        # Handle Negation Logic (!)
                        is_negation = False
                        clean_kw = kw
                        
                        if kw.startswith('!') and kw != '!=empty':
                            is_negation = True
                            clean_kw = kw[1:]

                        # 1. Special case: EMPTY / NOT EMPTY
                        if clean_kw == '=empty':
                            if not source_value_raw or not source_value_raw.strip():
                                individual_match = True
                        elif clean_kw == '!=empty':
                            if source_value_raw and source_value_raw.strip():
                                individual_match = True
                        
                        # 2. Standard substring/numerical filtering
                        elif source_value_raw:
                            val_str = str(source_value_raw).lower()
                            
                            # Numerical Check
                            num_match = re.match(r'([<>]?=?)\s*(\-?\d+(\.\d+)?)$', clean_kw, re.IGNORECASE)
                            
                            if num_match:
                                operator = num_match.group(1)
                                target_num_str = num_match.group(2)
                                try:
                                    target_num = float(target_num_str)
                                    actual_num = float(val_str)
                                    
                                    if operator == '>' and actual_num > target_num: individual_match = True
                                    elif operator == '<' and actual_num < target_num: individual_match = True
                                    elif operator == '>=' and actual_num >= target_num: individual_match = True
                                    elif operator == '<=' and actual_num <= target_num: individual_match = True
                                    elif (operator == '=' or operator == '') and actual_num == target_num: individual_match = True
                                except ValueError:
                                    pass 
                            else:
                                if clean_kw in val_str:
                                    individual_match = True
                        
                        if is_negation:
                            individual_match = not individual_match

                        if match_type_keywords == 'AND':
                            if not individual_match:
                                current_kws_match_status = False
                                break
                        elif match_type_keywords == 'OR':
                            if individual_match:
                                current_kws_match_status = True
                                break
                    
                    criterion_results.append(current_kws_match_status)

                final_match = False
                if overall_match_type == 'AND':
                    final_match = all(criterion_results)
                elif overall_match_type == 'OR':
                    final_match = any(criterion_results)
                
                if final_match:
                    # OPTIMIZATION: Store the raw string, not the dict object
                    matching_lines.append(line_stripped)

            except json.JSONDecodeError:
                pass 
            except Exception:
                pass
            
            if current_byte >= end_byte:
                break
    return matching_lines

def download_and_decompress_gz_to_file(url, token, output_path):
    """Downloads and decompresses a .gz file."""
    headers = {"Token": token}
    try:
        print(f"Downloading from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        start_time = time.time()
        last_update_time = start_time

        with open(output_path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
                downloaded_size += len(chunk)
                current_time = time.time()
                if current_time - last_update_time >= 5:
                    percentage = (downloaded_size / total_size) * 100 if total_size else 0
                    elapsed_time = current_time - start_time
                    rate = (downloaded_size / (1024 * 1024)) / elapsed_time if elapsed_time else 0
                    sys.stdout.write(f"\rDownloading... {percentage:.2f}% ({downloaded_size / (1024 * 1024):.2f} MB / {total_size / (1024 * 1024):.2f} MB) at {rate:.2f} MB/s")
                    sys.stdout.flush()
                    last_update_time = current_time
            sys.stdout.write("\n")
            sys.stdout.flush()
            
        print(f"Successfully downloaded gzipped file to: {output_path}")

        decompressed_file_path = os.path.splitext(output_path)[0]
        if not decompressed_file_path.lower().endswith('.json'):
            decompressed_file_path += '.json'

        print(f"Decompressing {output_path} to {decompressed_file_path}...")
        
        with gzip.open(output_path, 'rb') as f_in:
            with open(decompressed_file_path, 'wb') as f_out:
                buffer_size = 1024 * 1024 
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
    except Exception as e:
        print(f"Error during download/decompression: {e}", file=sys.stderr)
        return None

def download_raw_file_to_disk(url, token, output_path):
    """Downloads a raw file."""
    headers = {"Token": token}
    try:
        print(f"Downloading raw file from: {url}")
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        start_time = time.time()
        last_update_time = start_time

        with open(output_path, 'wb') as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
                downloaded_size += len(chunk)
                current_time = time.time()
                if current_time - last_update_time >= 5:
                    percentage = (downloaded_size / total_size) * 100 if total_size else 0
                    elapsed_time = current_time - start_time
                    rate = (downloaded_size / (1024 * 1024)) / elapsed_time if elapsed_time else 0
                    sys.stdout.write(f"\rDownloading... {percentage:.2f}% ({downloaded_size / (1024 * 1024):.2f} MB / {total_size / (1024 * 1024):.2f} MB) at {rate:.2f} MB/s")
                    sys.stdout.flush()
                    last_update_time = current_time
            sys.stdout.write("\n")
            sys.stdout.flush()

        print(f"Successfully downloaded raw file to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error during raw file download: {e}", file=sys.stderr)
        return None

def print_keyword_tips():
    """Prints helpful tips for keyword entry."""
    print("\n  --- KEYWORD SEARCH TIPS ---")
    print("  1. Text Search: Enter words to find (e.g., 'malicious'). Case-insensitive.")
    print("  2. Negation:    Prefix with '!' to exclude a term (e.g., '!clean').")
    print("  3. Existence:   Use '=EMPTY' to find missing values, '!=EMPTY' for existing values.")
    print("  4. Numerical:   Use operators for numbers (e.g., '>50', '<=100', '=443').")
    print("  5. Combined:    Separate multiple keywords with commas (e.g., '>50, !clean, !=EMPTY').")
    print("  ---------------------------")

# --- Main Script Logic ---
if __name__ == "__main__":
    script_start_time = time.time()

    if os.environ.get('TOKEN') is None:
        user_token = input("No TOKEN environment variable found. Please enter your Spur API token: ").strip()
        os.environ['TOKEN'] = user_token
        API_TOKEN = user_token

    while True:
        current_date_ymd = datetime.date.today().strftime("%Y%m%d")
        current_time_hms = None 
        decompressed_source_file_path = None
        base_feed_name = "UnknownFeed" 
        is_feed_json = True 
        
        use_existing_file_input = input("Do you want to use an existing Spur Feed file? (Y/N): ").strip().upper()

        if use_existing_file_input == 'Y':
            provided_file_path = input("Please enter the full path to your Spur Feed file: ").strip()
            if not os.path.exists(provided_file_path):
                print(f"Error: Provided input file '{provided_file_path}' not found. Exiting.", file=sys.stderr)
                sys.exit(1)
            
            decompressed_source_file_path = provided_file_path
            print(f"Using provided file: {decompressed_source_file_path}")

            match = re.search(r'(\d{8})(\d{6})?(AnonRes|AnonResRT|Anonymous|IPGeoMMDB|IPGeoJSON|ServiceMetricsAll|DCH|AnonymousIPv6|AnonymousResidentialIPv6|AnonymousResidential|AnonymousResidentialRT|IPSummary|SimilarIPs)\.(json|mmdb|json\.gz)$', os.path.basename(provided_file_path), re.IGNORECASE)
            if match:
                current_date_ymd = match.group(1)
                if match.group(2):
                    current_time_hms = match.group(2)
                base_feed_name = match.group(3)
                if base_feed_name in ["IPGeoMMDB"]:
                    is_feed_json = False
            else:
                name_without_ext = os.path.splitext(os.path.basename(provided_file_path))[0]
                base_feed_name_candidate = re.sub(r'^\d{8}(\d{6})?', '', name_without_ext)
                if base_feed_name_candidate:
                    base_feed_name = base_feed_name_candidate
                    if "AnonRes" in base_feed_name and "AnonResRT" not in base_feed_name:
                        base_feed_name = base_feed_name.replace("AnonRes", "AnonymousResidential")
                else:
                    base_feed_name = "CustomFeed"
                print(f"Warning: Could not extract standard FeedName. Using derived name '{base_feed_name}'.", file=sys.stderr)
                if not provided_file_path.lower().endswith('.json') and not provided_file_path.lower().endswith('.json.gz'):
                    is_feed_json = False

            if not is_feed_json:
                print(f"The selected feed is not a JSON file. Exiting.", file=sys.stderr)
                sys.exit(1)
            
            break 

        elif use_existing_file_input == 'N':
            feed_options = {
                "1": {"name": "Anonymous (Latest)", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "2": {"name": "Anonymous IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-ipv6/latest.json.gz", "base_feed_name": "AnonymousIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "3": {"name": "Anonymous (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous/{}/feed.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "4": {"name": "Anonymous-Residential (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_feed_name": "AnonymousResidential", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "5": {"name": "Anonymous-Residential IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential-ipv6/latest.json.gz", "base_feed_name": "AnonymousResidentialIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "6": {"name": "Anonymous-Residential (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/{}/feed.json.gz", "base_feed_name": "AnonymousResidential", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "7": {"name": "Anonymous-Residential Realtime (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_feed_name": "AnonResRT", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "8": {"name": "Anonymous-Residential Realtime (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/realtime/{}/{}.json.gz", "base_feed_name": "AnonymousResidentialRT", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
                "9": {"name": "IPGeo (MMDB - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.mmdb", "base_feed_name": "IPGeoMMDB", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False, "is_json": False},
                "10": {"name": "IPGeo (JSON - Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.json.gz", "base_feed_name": "IPGeoJSON", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "11": {"name": "Data Center Hosting (DCH) (Latest)", "url": "https://feeds.spur.us/v2/dch/latest.json.gz", "base_feed_name": "DCH", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "12": {"name": "Service Metrics (Latest)", "url": "https://feeds.spur.us/v2/service-metrics/latest.json.gz", "base_feed_name": "ServiceMetricsAll", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
                "13": {"name": "Service Metrics (Historical)", "url_template": "https://feeds.spur.us/v2/service-metrics/{}/feed.json.gz", "base_feed_name": "ServiceMetricsAll", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
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
                    print("Invalid choice.")

            api_url = selected_feed.get("url")
            base_feed_name = selected_feed["base_feed_name"]
            needs_decompression = selected_feed["needs_decompression"]
            output_ext = selected_feed["output_ext"]
            is_historical = selected_feed["is_historical"]
            is_feed_json = selected_feed["is_json"]

            if is_historical:
                date_input_valid = False
                while not date_input_valid:
                    historical_date_ymd = input("Enter date (YYYYMMDD): ").strip()
                    if re.fullmatch(r'\d{8}', historical_date_ymd):
                        try:
                            datetime.datetime.strptime(historical_date_ymd, "%Y%m%d")
                            if base_feed_name == "AnonymousResidentialRT":
                                historical_time_hhmm = input("Enter time (HHMM): ").strip()
                                if re.fullmatch(r'\d{4}', historical_time_hhmm):
                                    api_url = selected_feed["url_template"].format(historical_date_ymd, historical_time_hhmm)
                                    current_time_hms = historical_time_hhmm + '00'
                                    date_input_valid = True
                                else:
                                    print("Invalid time format.")
                            else:
                                api_url = selected_feed["url_template"].format(historical_date_ymd)
                                date_input_valid = True
                            
                            if date_input_valid:
                                current_date_ymd = historical_date_ymd
                        except ValueError:
                            print("Invalid date.")
                    else:
                        print("Invalid format.")
            
            download_successful = False
            download_filename_temp = f"{current_date_ymd}"
            
            temp_base_feed_name = base_feed_name
            if base_feed_name in ["AnonResRT", "AnonymousResidentialRT"]:
                if not current_time_hms:
                    current_time_hms = datetime.datetime.now().strftime("%H%M%S")
                download_filename_temp += f"{current_time_hms}"
            
            if base_feed_name == "AnonymousResidentialHist": temp_base_feed_name = "AnonymousResidential"
            elif base_feed_name == "ServiceMetricsAllHist": temp_base_feed_name = "ServiceMetricsAll"
            elif base_feed_name == "AnonymousHist": temp_base_feed_name = "Anonymous"
            
            download_filename_temp += temp_base_feed_name

            if needs_decompression:
                download_filename_temp += ".json.gz"
                decompressed_source_file_path = download_and_decompress_gz_to_file(api_url, os.environ.get('TOKEN'), download_filename_temp)
                if decompressed_source_file_path is not None:
                    download_successful = True
            else:
                download_filename_temp += output_ext
                decompressed_source_file_path = download_raw_file_to_disk(api_url, os.environ.get('TOKEN'), download_filename_temp)
                if decompressed_source_file_path is not None:
                    download_successful = True
            
            if not download_successful:
                retry_input = input("Download failed. Try again? (Y/N): ").strip().upper()
                if retry_input == 'Y': continue
                else: sys.exit(1)

            if not is_feed_json:
                print(f"File at: {decompressed_source_file_path}. Script finished (non-JSON).")
                sys.exit(0)
            
            break 
        else:
            print("Invalid response. Exiting.", file=sys.stderr)
            sys.exit(1)

    if not decompressed_source_file_path or not os.path.exists(decompressed_source_file_path):
        print(f"Critical Error: File '{decompressed_source_file_path}' not found. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    if not decompressed_source_file_path.lower().endswith('.json') and is_feed_json:
        print("Error: Expected JSON file.", file=sys.stderr)
        sys.exit(1)

    filter_criteria = []
    
    perform_initial_filter_choice = input("\nDo you want to filter the data? (Y/N): ").strip().upper()

    if perform_initial_filter_choice == 'Y':
        while True:
            current_filter_key = None 
            current_keywords = []
            current_match_type_keywords = 'AND' 

            perform_key_specific_filter_choice = input("  Filter by a specific key (Y/N)? ").strip().upper() 

            if perform_key_specific_filter_choice == 'Y':
                # New prompt for key sampling size
                key_sample_size_str = input("  How many lines to sample for keys? (Default 500000): ").strip()
                key_sample_size = 500000
                if key_sample_size_str.isdigit():
                    key_sample_size = int(key_sample_size_str)
                
                print(f"\n--- Analyzing first {key_sample_size} lines for filterable keys ---") 
                sample_lines = []
                try:
                    with open(decompressed_source_file_path, 'r', encoding='utf-8') as f_sample:
                        for _ in range(key_sample_size): 
                            line = f_sample.readline()
                            if not line: break
                            sample_lines.append(line)
                except Exception:
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
                                suggested_keys.add(f"{match.group(1)}_{match.group(2)}")
                            else:
                                suggested_keys.add(key)
                    except json.JSONDecodeError:
                        pass
                
                if suggested_keys:
                    print("\nAvailable keys for filtering:") 
                    for key in sorted(list(suggested_keys)):
                        print(f"  - {key}")
                    print("\n")
                
                current_filter_key = input("  Enter the exact key name for this filter: ").strip()
                if not current_filter_key: continue

                see_sample_values = input(f"  See sample values for '{current_filter_key}'? (Y/N): ").strip().upper()
                if see_sample_values == 'Y':
                    # New prompt for value sampling size
                    val_sample_size_str = input("  How many lines to sample for values? (Default 500000): ").strip()
                    val_sample_size = 500000
                    if val_sample_size_str.isdigit():
                        val_sample_size = int(val_sample_size_str)

                    print(f"\n--- Analyzing first {val_sample_size} lines for values ---")
                    
                    # Re-read file for value sampling (sample_lines might be too small or different)
                    val_sample_lines = []
                    try:
                        with open(decompressed_source_file_path, 'r', encoding='utf-8') as f_sample:
                            for _ in range(val_sample_size):
                                line = f_sample.readline()
                                if not line: break
                                val_sample_lines.append(line)
                    except Exception:
                        val_sample_lines = []
                        
                    unique_values = set()
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
                        for line in val_sample_lines:
                            try:
                                obj = json.loads(line.strip())
                                flattened_obj = flatten_json(obj)
                                for f_key in target_flattened_keys:
                                    value = flattened_obj.get(f_key)
                                    if value is not None:
                                        if isinstance(value, str):
                                            for individual_value in value.split(','):
                                                unique_values.add(individual_value.strip())
                                        else:
                                            unique_values.add(str(value).strip())
                            except json.JSONDecodeError: pass

                    if unique_values:
                        print("Unique values found:")
                        for value in sorted(list(unique_values)):
                            print(f"  - {value}")
                        print("\n")
                    else:
                        print("No values found in sample.\n")
                    
                    if input(f"  Proceed filtering for this key? (Y/N): ").strip().upper() != 'Y':
                        continue 

                # Print the tips before asking for input
                print_keyword_tips()
                current_keywords_input = input(f"  Enter keywords for '{current_filter_key}': ").strip()
            
            elif perform_key_specific_filter_choice == 'N':
                # Print the tips before asking for input
                print_keyword_tips()
                current_keywords_input = input("  Enter keywords for general search: ").strip()
            else:
                continue
            
            if current_keywords_input:
                current_keywords = [kw.strip().lower() for kw in current_keywords_input.split(',') if kw.strip()]
                
                if current_keywords:
                    if len(current_keywords) > 1:
                        match_type_kws_choice = input("  Match ALL keywords (AND) or ANY keyword (OR)? (AND/OR): ").strip().upper()
                        if match_type_kws_choice in ['AND', 'OR']:
                            current_match_type_keywords = match_type_kws_choice
                    
                    filter_criteria.append({
                        'key': current_filter_key,
                        'keywords': current_keywords,
                        'match_type_keywords': current_match_type_keywords
                    })
                    print(f"  Added filter.")
            
            if input("Add another filter condition (Y/N)? ").strip().upper() != 'Y':
                break
    
    overall_match_type = 'AND'
    if len(filter_criteria) > 1:
        overall_match_type_choice = input("Apply ALL filter conditions (AND) or ANY filter condition (OR)? (AND/OR): ").strip().upper()
        if overall_match_type_choice in ['AND', 'OR']:
            overall_match_type = overall_match_type_choice

    if not filter_criteria:
        # --- NO FILTERING: BYPASS PROCESSING ---
        
        # Determine strict output name
        current_filename = os.path.basename(decompressed_source_file_path)
        user_output_filename = input(f"Enter output filename (Default: {current_filename}): ").strip()
        
        if not user_output_filename:
             print(f"File available at: {decompressed_source_file_path}")
        else:
            # Rename/Move source to user specific name
            final_output_path = get_output_filename(
                current_date_ymd, current_time_hms, base_feed_name, user_output_filename, [], overall_match_type
            )
            # Ensure we don't overwrite if they just typed the same name
            if os.path.abspath(final_output_path) != os.path.abspath(decompressed_source_file_path):
                shutil.move(decompressed_source_file_path, final_output_path)
                print(f"File moved to: {final_output_path}")
            else:
                print(f"File available at: {final_output_path}")

        print("\nScript finished.")
        sys.exit(0)
    else:
        perform_filter = 'Y'

    # Generate the dynamic default filename based on selected filters
    default_generated_filename = get_output_filename(
        current_date_ymd, 
        current_time_hms, 
        base_feed_name,
        "",  
        filter_criteria if perform_filter == 'Y' else [],
        overall_match_type
    )

    user_output_filename = input(f"Enter output filename (Default: {default_generated_filename}): ").strip()
    
    filtered_output_filename = get_output_filename(
        current_date_ymd, 
        current_time_hms, 
        base_feed_name,
        user_output_filename,    
        filter_criteria if perform_filter == 'Y' else [],
        overall_match_type
    )
    output_file_path = os.path.join(os.getcwd(), filtered_output_filename)

    # --- CRITICAL SAFEGUARD: Prevent overwriting source file ---
    if os.path.abspath(output_file_path) == os.path.abspath(decompressed_source_file_path):
        print("Warning: Output filename matches source filename. Prepending 'Filtered_' to prevent data loss.")
        dirname, basename = os.path.split(output_file_path)
        output_file_path = os.path.join(dirname, "Filtered_" + basename)
    
    print(f"\n--- Starting Processing ---")
    if perform_filter == 'Y':
        print(f"Filtering content...")
    
    records_exported_count = 0
    chunks_completed = 0
    start_time = time.time()
    
    NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4

    try:
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            # 1. Get the chunks (Optimized for 64MB targets)
            chunks = get_file_chunks(decompressed_source_file_path, NUM_PARALLEL_PROCESSORS)
            total_chunks = len(chunks)
            
            # 2. Update Feedback: Show the user we are using safe chunking
            print(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors to process {total_chunks} data chunks (Optimized for Memory Safety).")
            
            with multiprocessing.Pool(processes=NUM_PARALLEL_PROCESSORS) as pool:
                results_iterator = pool.imap_unordered(
                    process_file_chunk,
                    [(decompressed_source_file_path, start, end, filter_criteria, overall_match_type) for start, end in chunks]
                )
                
                for matching_lines_in_chunk in results_iterator:
                    chunks_completed += 1
                    try: 
                        # Write raw strings directly (Memory Efficient)
                        for line in matching_lines_in_chunk:
                            outfile.write(line + '\n')
                            records_exported_count += 1
                        
                        # 3. Update Feedback: Single line update with carriage return
                        if chunks_completed % 5 == 0 or records_exported_count % 1000 == 0:
                            elapsed_time = time.time() - start_time
                            records_per_second = records_exported_count / elapsed_time if elapsed_time > 0 else 0
                            progress_pct = (chunks_completed / total_chunks) * 100
                            sys.stdout.write(f"\r  Progress: {progress_pct:.1f}% ({chunks_completed}/{total_chunks} chunks) | Exported: {records_exported_count} records ({records_per_second:.2f} rec/s)")
                            sys.stdout.flush()

                    except Exception as exc:
                        print(f"\nError processing chunk: {exc}", file=sys.stderr)
            
            # Print a newline to ensure the final success message is on a new line
            print()
            print(f"Successfully exported {records_exported_count} records to {output_file_path}.")

    except Exception as e:
        print(f"\nError during export: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nScript finished.")
    total_elapsed_seconds = time.time() - script_start_time
    print(f"\nTotal execution time: {int(total_elapsed_seconds // 60)} Minutes {int(total_elapsed_seconds % 60)} Seconds")
    sys.exit(0)
