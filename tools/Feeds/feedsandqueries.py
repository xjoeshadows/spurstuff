import os
import subprocess
import datetime
import sys
import json
import csv
import re # Import re module for regular expressions

# --- Provided Functions from localjsontocsvconversion.py ---
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

def write_to_csv(data, output_path):
    """Writes a list of dictionaries to a CSV file, ensuring all fields are included.
    Automatically determines all unique fieldnames from the data for the header.
    """
    if not data:
        print("No data to write to CSV.")
        return

    # Collect all unique fieldnames from all dictionaries to ensure comprehensive header
    fieldnames = set()
    for row in data:
        fieldnames.update(row.keys())
    fieldnames = sorted(list(fieldnames))  # Sort for consistent column order in CSV

    try:
        # Open file with utf-8-sig encoding for better compatibility with Excel
        # newline='' is essential for csv module to handle line endings correctly
        # restval='' ensures empty string for fields missing in a row
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='')
            writer.writeheader()  # Write the header row
            writer.writerows(data) # Write all data rows
        print(f"Processed data written to {output_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}", file=sys.stderr)
        sys.exit(1) # Exit if writing fails

def process_json_to_csv(input_file_path, output_file_path):
    """
    Processes a JSON file (either single object, list of objects, or JSON Lines)
    and converts it to a flattened CSV format.
    """
    raw_data = []

    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            file_content = f.read().strip()

        try:
            full_json_data = json.loads(file_content)
            if isinstance(full_json_data, list):
                raw_data.extend(full_json_data)
            elif isinstance(full_json_data, dict):
                raw_data.append(full_json_data)
            else:
                print(f"Warning: Entire file is valid JSON but not a list or dictionary (type: {type(full_json_data)}). Skipping content.", file=sys.stderr)

        except json.JSONDecodeError as e_full:
            print(f"Attempt 1 (full file JSON) failed: {e_full}. Falling back to line-by-line parsing.", file=sys.stderr)
            
            for line_num, line in enumerate(file_content.splitlines(), 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    json_object = json.loads(line)
                    if isinstance(json_object, dict):
                        raw_data.append(json_object)
                    elif isinstance(json_object, list):
                        raw_data.extend(json_object)
                    else:
                        print(f"Warning: Line {line_num} contains valid JSON but is not a dictionary or list (type: {type(json_object)}). Skipping: '{line}'", file=sys.stderr)
                except json.JSONDecodeError as e_line:
                    print(f"Error decoding malformed JSON from line {line_num}: {e_line} in line: '{line}'", file=sys.stderr)
                except Exception as e_other:
                    print(f"An unexpected error occurred processing line {line_num}: {e_other} in line: '{line}'", file=sys.stderr)

    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during file reading: {e}", file=sys.stderr)
        sys.exit(1)

    if not raw_data:
        print(f"No valid JSON data found in {input_file_path} to process.", file=sys.stderr)
        return

    processed_data = []
    for item in raw_data:
        if isinstance(item, dict):
            flattened_data = flatten_json(item)
            processed_data.append(flattened_data)
        else:
            print(f"Skipping non-dictionary item during processing: {item} (Type: {type(item)})", file=sys.stderr)

    write_to_csv(processed_data, output_file_path)

# --- Main Script Logic ---
if __name__ == "__main__":
    # Default date format to YYYYMMDD (4-digit year)
    current_date = datetime.datetime.now().strftime("%Y%m%d") 
    decompressed_source_file = None
    
    # Prompt user for input method
    use_existing_file_input = input("Do you want to use an existing YYYYMMDDAnonRes file? (Y/N): ").strip().upper()

    if use_existing_file_input == 'Y':
        provided_file_path = input("Please enter the full path to your YYYYMMDDAnonRes file: ").strip()
        if not os.path.exists(provided_file_path):
            print(f"Error: Provided input file '{provided_file_path}' not found. Exiting.", file=sys.stderr)
            sys.exit(1)
        
        # Set the source file to the provided path
        decompressed_source_file = provided_file_path
        print(f"Using provided file: {decompressed_source_file}")

        # Attempt to extract YYYYMMDD from the provided filename for consistent output naming
        # This regex looks for 8 digits followed by "AnonRes" in the filename
        match = re.search(r'(\d{8})AnonRes', os.path.basename(provided_file_path))
        if match:
            current_date = match.group(1) # Use the extracted date
            print(f"Extracted date '{current_date}' from the provided filename for output files.")
        else:
            print(f"Warning: Could not extract YYYYMMDD from provided filename '{os.path.basename(provided_file_path)}'. Using current system date '{current_date}' for output files.", file=sys.stderr)

    elif use_existing_file_input == 'N':
        # No file provided, proceed with downloading a fresh one
        TOKEN = os.environ.get('TOKEN') # Get TOKEN from environment variable
        if not TOKEN:
            print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable to download the file.", file=sys.stderr)
            sys.exit(1)

        feed_options = {
            "1": {"name": "AnonRes", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_filename": "AnonRes"},
            "2": {"name": "AnonRes Realtime", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_filename": "AnonResRT"},
            "3": {"name": "Anonymous", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_filename": "Anonymous"},
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
        base_filename = selected_feed["base_filename"]

        gz_filename = f"{current_date}{base_filename}.json.gz"
        decompressed_source_file = f"{current_date}{base_filename}" 

        print(f"Fetching {api_url} to {gz_filename}...")
        try:
            curl_command = [
                "curl",
                "--location", api_url,
                "--header", f"Token: {TOKEN}", # Ensure correct header format
                "--output", gz_filename
            ]
            subprocess.run(curl_command, check=True)
            print(f"Successfully downloaded {gz_filename}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading file: {e}", file=sys.stderr)
            sys.exit(1)

        print(f"Decompressing {gz_filename}...")
        try:
            # gunzip -df will decompress in place, removing the .gz extension
            subprocess.run(["gunzip", "-df", gz_filename], check=True)
            print(f"Successfully decompressed {gz_filename} to {decompressed_source_file}")
        except subprocess.CalledProcessError as e:
            print(f"Error decompressing file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Invalid response. Please answer 'Y' or 'N'. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Validate that the source file is now available
    if not decompressed_source_file or not os.path.exists(decompressed_source_file):
        print(f"Critical Error: Source data file '{decompressed_source_file}' could not be located or created. Exiting.", file=sys.stderr)
        sys.exit(1)

    # Query 1: grep for "country":"KP"
    kp_ips_filename = f"{current_date}AnonResKPIPs.txt"
    print(f"Running query for 'country':'KP' against {decompressed_source_file}...")
    try:
        with open(kp_ips_filename, 'w') as kp_file:
            grep_kp_command = ["grep", "-i", "-E", "country\":\"KP", decompressed_source_file]
            subprocess.run(grep_kp_command, stdout=kp_file, check=True)
        print(f"Results for 'country':'KP' written to {kp_ips_filename}")
    except subprocess.CalledProcessError as e:
        # Grep returns 1 if no lines were selected, which is not an error if no matches are expected.
        if e.returncode == 1:
            print(f"No matches found for 'country':'KP' in {decompressed_source_file}. This is not necessarily an error.", file=sys.stderr)
        else:
            print(f"An error occurred during grep execution for 'country':'KP': {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred during 'country':'KP' query: {e}", file=sys.stderr)

    # Query 2: grep for "TROJAN" - Refined query
    trojan_ips_filename = f"{current_date}AnonResTrojanIPs.txt"
    print(f"Running refined query for 'services:[\"TROJAN\"]' against {decompressed_source_file}...")
    try:
        with open(trojan_ips_filename, 'w') as trojan_file:
            # Refined regex to accurately find "services":["TROJAN"]
            grep_trojan_command = ["grep", "-i", "-E", "\"services\":\\[\"TROJAN\"\\]", decompressed_source_file]
            subprocess.run(grep_trojan_command, stdout=trojan_file, check=True)
        print(f"Results for 'services:[\"TROJAN\"]' written to {trojan_ips_filename}")
    except subprocess.CalledProcessError as e:
        if e.returncode == 1:
            print(f"No matches found for 'services:[\"TROJAN\"]' in {decompressed_source_file}. This is not necessarily an error.", file=sys.stderr)
        else:
            print(f"An error occurred during grep execution for 'services:[\"TROJAN\"]': {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred during 'TROJAN' query: {e}", file=sys.stderr)

    # New command: shuf -n 10000 YYYYMMDDAnonRes
    shuf_filename = f"{current_date}AnonRes10kShuf.txt"
    print(f"Shuffling 10000 lines from {decompressed_source_file} to {shuf_filename}...")
    try:
        # Use subprocess.run with stdout redirection to save shuf output to a file
        with open(shuf_filename, 'w') as shuf_file:
            shuf_command = ["shuf", "-n", "10000", decompressed_source_file]
            subprocess.run(shuf_command, stdout=shuf_file, check=True)
        print(f"Successfully shuffled 10000 lines to {shuf_filename}")
    except subprocess.CalledProcessError as e:
        print(f"Error running shuf command: {e}", file=sys.stderr)
        sys.exit(1) # Exit if shuf fails
    except Exception as e:
        print(f"An unexpected error occurred during shuffling: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nProcessing queried and shuffled data to CSV...")

    # Process KP IPs
    # Check if file exists and is not empty before processing
    if os.path.exists(kp_ips_filename) and os.path.getsize(kp_ips_filename) > 0:
        kp_output_csv = f"{current_date}AnonResKPIPs.csv"
        print(f"Processing {kp_ips_filename} to {kp_output_csv}")
        process_json_to_csv(kp_ips_filename, kp_output_csv)
    else:
        print(f"Skipping CSV conversion for {kp_ips_filename} as it does not exist or is empty.")

    # Process Trojan IPs
    if os.path.exists(trojan_ips_filename) and os.path.getsize(trojan_ips_filename) > 0:
        trojan_output_csv = f"{current_date}AnonResTrojanIPs.csv"
        print(f"Processing {trojan_ips_filename} to {trojan_output_csv}")
        process_json_to_csv(trojan_ips_filename, trojan_output_csv)
    else:
        print(f"Skipping CSV conversion for {trojan_ips_filename} as it does not exist or is empty.")

    # Process 10k Shuffled data
    if os.path.exists(shuf_filename) and os.path.getsize(shuf_filename) > 0:
        shuf_output_csv = f"{current_date}AnonRes10kShuf.csv"
        print(f"Processing {shuf_filename} to {shuf_output_csv}")
        process_json_to_csv(shuf_filename, shuf_output_csv)
    else:
        print(f"Skipping CSV conversion for {shuf_filename} as it does not exist or is empty.")

    print("\nScript finished.")
