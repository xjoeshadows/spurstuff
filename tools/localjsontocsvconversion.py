import json
import csv
import sys
import os

# --- Functions ---
def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary."""
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, str(v)))  # Store the entire list as a string
        else:
            items.append((new_key, v))
    return dict(items)

def write_to_csv(data, output_path):
    """Writes a list of dictionaries to a CSV file, ensuring all fields are included."""
    if not data:
        print("No data to write to CSV.")
        return

    # Collect all unique fieldnames from all dictionaries
    fieldnames = set()
    for row in data:
        fieldnames.update(row.keys())
    fieldnames = sorted(list(fieldnames))  # Sort for consistent column order

    try:
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"Processed data written to {output_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}")

# --- Main Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python localenrich.py <input_file>")
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        sys.exit(1)

    # Extract the directory and base filename from the input file path
    output_dir = os.path.dirname(input_file_path)
    base_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    output_file_path = os.path.join(output_dir, f"{base_filename}.csv")

    raw_data = []
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        json_object = json.loads(line)
                        raw_data.append(json_object)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e} in line: {line}")
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        exit()

    processed_data = []
    for item in raw_data:
        flattened_data = flatten_json(item)
        processed_data.append(flattened_data)

    write_to_csv(processed_data, output_file_path)