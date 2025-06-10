import os
import subprocess
import datetime
import sys
import json
import csv

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
    TOKEN = os.environ.get('TOKEN') # Get TOKEN from environment variable
    if not TOKEN:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    # 1. Fetch file from API
    # Corrected date format to YYYYMMDD (4-digit year)
    current_date = datetime.datetime.now().strftime("%Y%m%d") # Changed from %y to %Y
    gz_filename = f"{current_date}AnonRes.gz"
    # The decompressed file will have the same name without the .gz extension
    decompressed_filename = f"{current_date}AnonRes" 
    api_url = "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz"

    print(f"Fetching {api_url} to {gz_filename}...")
    try:
        curl_command = [
            "curl",
            "--location", api_url,
            "--header", f"TOKEN:{TOKEN}",
            "--output", gz_filename
        ]
        subprocess.run(curl_command, check=True)
        print(f"Successfully downloaded {gz_filename}")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading file: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Decompress the output file
    print(f"Decompressing {gz_filename}...")
    try:
        # gunzip -df will decompress in place, removing the .gz extension
        subprocess.run(["gunzip", "-df", gz_filename], check=True)
        print(f"Successfully decompressed {gz_filename} to {decompressed_filename}")
    except subprocess.CalledProcessError as e:
        print(f"Error decompressing file: {e}", file=sys.stderr)
        sys.exit(1)

    # Check if the decompressed file exists before proceeding
    if not os.path.exists(decompressed_filename):
        print(f"Error: Decompressed file {decompressed_filename} not found.", file=sys.stderr)
        sys.exit(1)

    # 3. Run queries against the data
    kp_ips_filename = f"{current_date}AnonResKPIPs.txt"
    trojan_ips_filename = f"{current_date}AnonResTrojanIPs.txt"

    print(f"Running queries against {decompressed_filename}...")
    try:
        # Query 1: grep for "country":"KP"
        # Using shell=True for simple shell commands like redirection can be risky
        # but for piped grep into file, it's generally simpler.
        # Alternatively, use Python's file handling for output as done previously.
        # Sticking with the Python file handling for consistency and safety.
        with open(kp_ips_filename, 'w') as kp_file:
            grep_kp_command = ["grep", "-i", "-E", "country\":\"KP", decompressed_filename]
            subprocess.run(grep_kp_command, stdout=kp_file, check=True)
        print(f"Results for 'country':'KP' written to {kp_ips_filename}")

        # Query 2: grep for "TROJAN"
        with open(trojan_ips_filename, 'w') as trojan_file:
            grep_trojan_command = ["grep", "-i", "-E", "TROJAN", decompressed_filename]
            subprocess.run(grep_trojan_command, stdout=trojan_file, check=True)
        print(f"Results for 'TROJAN' written to {trojan_ips_filename}")

    except subprocess.CalledProcessError as e:
        # Grep returns 1 if no lines were selected, which is not an error if no matches are expected.
        if e.returncode == 1:
            print(f"No matches found for a grep query in {decompressed_filename}. This is not necessarily an error.", file=sys.stderr)
        else:
            print(f"An error occurred during grep execution with return code {e.returncode}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred during querying: {e}", file=sys.stderr)

    # 4. Run the attached code (process_json_to_csv function) against the output .txt files
    print("\nProcessing queried data to CSV...")

    # Process KP IPs
    # Check if file exists and is not empty before processing
    if os.path.exists(kp_ips_filename) and os.path.getsize(kp_ips_filename) > 0:
        kp_output_csv = f"{current_date}AnonResKPIPs.csv"
        print(f"Processing {kp_ips_filename} to {kp_output_csv}")
        process_json_to_csv(kp_ips_filename, kp_output_csv)
    else:
        print(f"Skipping CSV conversion for {kp_ips_filename} as it does not exist or is empty.")

    # Process Trojan IPs
    # Check if file exists and is not empty before processing
    if os.path.exists(trojan_ips_filename) and os.path.getsize(trojan_ips_filename) > 0:
        trojan_output_csv = f"{current_date}AnonResTrojanIPs.csv"
        print(f"Processing {trojan_ips_filename} to {trojan_output_csv}")
        process_json_to_csv(trojan_ips_filename, trojan_output_csv)
    else:
        print(f"Skipping CSV conversion for {trojan_ips_filename} as it does not exist or is empty.")

    print("\nScript finished.")
