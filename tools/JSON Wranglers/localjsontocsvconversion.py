#!/usr/bin/env python3
import json
import csv
import sys
import os

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

# --- Main Script ---
if __name__ == "__main__":
    # Check for correct number of command-line arguments
    if len(sys.argv) != 2:
        print("Usage: python localenrich.py <input_file>", file=sys.stderr)
        sys.exit(1)

    input_file_path = sys.argv[1]

    # Validate if the input file exists
    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}", file=sys.stderr)
        sys.exit(1)

    # Determine output directory (same as input file's directory)
    output_dir = os.path.dirname(input_file_path)
    # Get base name of input file without its extension
    base_input_filename = os.path.splitext(os.path.basename(input_file_path))[0]

    # Prompt user for output filename
    # Default suggested filename is based on input filename with .csv extension
    default_output_filename = f"{base_input_filename}.csv"
    output_file_name_prompt = f"Enter the desired output file name (e.g., {default_output_filename}): "
    
    output_file_name = input(output_file_name_prompt).strip()
    
    if not output_file_name:
        # Use default filename if user input is empty
        output_file_path = os.path.join(output_dir, default_output_filename)
        print(f"Using default output file name: {default_output_filename}")
    else:
        # Sanitize filename and ensure .csv extension
        output_file_name = "".join(x for x in output_file_name if x.isalnum() or x in "._-")
        if not output_file_name.lower().endswith(".csv"):
            output_file_name += ".csv"
        output_file_path = os.path.join(output_dir, output_file_name)

    raw_data = [] # List to store parsed JSON dictionaries

    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            file_content = f.read().strip() # Read entire file content and strip whitespace

        # --- Parsing Attempt 1: Load the entire file content as a single JSON object/array ---
        try:
            full_json_data = json.loads(file_content)
            if isinstance(full_json_data, list):
                # If it's a list of JSON objects, extend raw_data with its elements
                raw_data.extend(full_json_data)
            elif isinstance(full_json_data, dict):
                # If it's a single JSON object, append it to raw_data
                raw_data.append(full_json_data)
            else:
                # Handle cases where the JSON is valid but not a list or dict (e.g., "null", 123)
                print(f"Warning: Entire file is valid JSON but not a list or dictionary (type: {type(full_json_data)}). Skipping content.", file=sys.stderr)

        except json.JSONDecodeError as e_full:
            # --- Parsing Attempt 2: Fallback to line-by-line parsing (for JSON Lines format) ---
            print(f"Attempt 1 (full file JSON) failed: {e_full}. Falling back to line-by-line parsing.", file=sys.stderr)
            
            # Process each line as a potential JSON object
            for line_num, line in enumerate(file_content.splitlines(), 1):
                line = line.strip()
                if not line:
                    continue # Skip empty lines

                try:
                    json_object = json.loads(line)
                    if isinstance(json_object, dict):
                        raw_data.append(json_object)
                    elif isinstance(json_object, list):
                        # If a line is a JSON array, extend with its elements
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
    except Exception as e: # Catch any other unexpected errors during file reading
        print(f"An unexpected error occurred during file reading: {e}", file=sys.stderr)
        sys.exit(1)

    # Check if any valid JSON data was parsed
    if not raw_data:
        print("No valid JSON data found in the input file to process. Exiting.", file=sys.stderr)
        sys.exit(0) # Exit gracefully if no data

    processed_data = []
    for item in raw_data:
        # Ensure item is a dictionary before flattening
        if isinstance(item, dict):
            flattened_data = flatten_json(item)
            processed_data.append(flattened_data)
        else:
            # This should ideally not happen if parsing is correct, but as a safeguard
            print(f"Skipping non-dictionary item during processing: {item} (Type: {type(item)})", file=sys.stderr)

    # Write the processed (flattened) data to the CSV file
    write_to_csv(processed_data, output_file_path)
