#!/usr/bin/env python3
import sys
import os
import json
import re
import datetime
import time

# --- Functions ---
def extract_tag_values_from_json_file(file_path):
    """
    Extracts the values from the "tag" field in the JSON data from a file,
    handling multiple JSON objects (JSON Lines format) or a single JSON array.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        list: A list of tag values (strings), or None on error.
    """
    tag_values = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            # Attempt 1: Load as a single JSON array
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    # Filter to ensure all items in the list are strings (tags)
                    tags = [item for item in data if isinstance(item, str)]
                    if len(tags) != len(data):
                        print(f"Warning: Some non-string items found in JSON array in {file_path}. Skipping them.", file=sys.stderr)
                    tag_values.extend(tags)
                elif isinstance(data, dict) and 'tag' in data:
                    tag_values.append(data['tag'])
                else:
                    print(f"Warning: Expected a JSON array of strings or object with 'tag' in {file_path}, but got type {type(data)}. Trying line-by-line.", file=sys.stderr)
                    # Fallback to line-by-line if primary parse is not an array/simple tag object
                    for line_num, line in enumerate(content.split('\n'), 1):
                        line = line.strip()
                        if line:
                            try:
                                parsed_line = json.loads(line)
                                if isinstance(parsed_line, str):
                                    tag_values.append(parsed_line)
                                elif isinstance(parsed_line, dict) and 'tag' in parsed_line:
                                    tag_values.append(parsed_line['tag'])
                                else:
                                    print(f"Warning: Line {line_num} in {file_path} is valid JSON but not a simple string or dict with 'tag'. Skipping: {line[:80]}...", file=sys.stderr)
                            except json.JSONDecodeError:
                                # If it's not valid JSON, treat it as a raw string if it's not empty
                                tag_values.append(line)
            except json.JSONDecodeError as e_full_parse:
                # If the entire file is not a single JSON array, try parsing line by line (JSON Lines format)
                print(f"Warning: File {file_path} is not a single valid JSON array ({e_full_parse}). Attempting line-by-line parsing for potential JSON Lines.", file=sys.stderr)
                for line_num, line in enumerate(content.split('\n'), 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        json_data = json.loads(line)
                        if isinstance(json_data, dict) and 'tag' in json_data:
                            tag_values.append(json_data['tag'])
                        elif isinstance(json_data, str): # Handle cases where lines might just be plain strings in quotes
                            tag_values.append(json_data)
                        else:
                            print(f"Warning: Line {line_num} in {file_path} contains valid JSON but not a 'tag' field or is not a string. Skipping: {line[:80]}...", file=sys.stderr)
                    except json.JSONDecodeError:
                        # If a line is not valid JSON, assume it's a plain string tag
                        tag_values.append(line)

        print(f"Successfully extracted {len(tag_values)} tags from {file_path}")
        return tag_values
    except FileNotFoundError:
        print(f"Error: Input file not found at {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error reading and processing {file_path}: {e}", file=sys.stderr)
        return None

def write_tags_to_file(tags, filename):
    """
    Writes the list of tags to a file, one tag per line.

    Args:
        tags (list): The list of tags to write.
        filename (str): The name of the file to write to.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for tag in tags:
                f.write(tag + '\n')
        print(f"Tags successfully written to {filename}")
        return True
    except Exception as e:
        print(f"Error writing tags to file '{filename}': {e}", file=sys.stderr)
        return False

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

# --- Main Script ---
if __name__ == "__main__":
    script_start_time = time.time()

    input_file_path = None

    # Check if a file path is provided as a command-line argument
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1]
        print(f"Using input file from command-line argument: '{input_file_path}'")
    else:
        # 1. Prompt for the input Service Metrics JSON file if no argument is given
        input_file_path = input("Enter the full path to your Service Metrics JSON file (e.g., '/path/to/20240610ServiceMetricsAll.json'): ").strip()

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at '{input_file_path}'. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # 2. Extract tags from the input file
    tags = extract_tag_values_from_json_file(input_file_path)
    if tags is None:
        print("Failed to extract tags from the input file. Exiting.", file=sys.stderr)
        sys.exit(1)
    
    # 3. Determine default output filename based on input file's date and name
    input_file_date = get_date_from_filename_or_creation(input_file_path)
    
    # New default output filename format: [input filename's YYYYMMDDServiceMetricsList.txt
    default_output_filename = f"{input_file_date}ServiceMetricsList.txt"

    # 4. Prompt for output filename
    output_filename_prompt = f"Enter the desired output filename for the tags (e.g., {default_output_filename}): "
    output_filename = input(output_filename_prompt).strip()

    if not output_filename:
        output_path = os.path.join(os.getcwd(), default_output_filename)
        print(f"Using default output filename: {default_output_filename}")
    else:
        # Sanitize filename and ensure .txt extension
        output_filename = "".join(x for x in output_filename if x.isalnum() or x in "._-")
        if not output_filename.lower().endswith(".txt"):
            output_filename += ".txt"
        output_path = os.path.join(os.getcwd(), output_filename)

    # 5. Write tags to the output file
    if not write_tags_to_file(tags, output_path):
        sys.exit(1)

    print("\nScript finished.")

    # Calculate and print total completion time
    script_end_time = time.time()
    total_elapsed_seconds = script_end_time - script_start_time
    minutes = int(total_elapsed_seconds // 60)
    seconds = int(total_elapsed_seconds % 60)
    print(f"\nTotal script execution time: {minutes} Minutes {seconds} Seconds")
