import requests
import json
import csv
import sys
import os

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
# Retrieve API token from environment variable
    api_token = os.environ.get("TOKEN")
    if not api_token:
        print("Error: TOKEN environment variable not set.", file=sys.stderr)
        print("Please set it using: export TOKEN='YOUR_API_TOKEN'", file=sys.stderr)
        sys.exit(1)
# --- Functions ---
def enrich_ip(ip_address):
    """Enriches a single IP address using the Spur API."""
    url = f"{api_url_base}{ip_address}"
    headers = {'TOKEN': api_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address}: {e}")
        return None

def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary."""
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_json(item, new_key + sep + str(i), sep=sep).items())
                else:
                    items.append((new_key + sep + str(i), item))
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
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='') # Added restval
            writer.writeheader()
            writer.writerows(data)
        print(f"Enriched data written to {output_path}")
    except Exception as e:
        print(f"Error writing to CSV file: {e}")

# --- Main Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python enrich_ip_api.py <input_file>")
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        sys.exit(1)

    # Extract the directory and base filename from the input file path
    output_dir = os.path.dirname(input_file_path)
    base_filename = os.path.splitext(os.path.basename(input_file_path))[0]
    output_file_path = os.path.join(output_dir, f"{base_filename}.csv")

    ip_addresses = []
    try:
        with open(input_file_path, 'r') as f:
            for line in f:
                ip = line.strip()
                if ip:
                    ip_addresses.append(ip)
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        exit()

    enriched_data = []
    for ip in ip_addresses:
        print(f"Enriching IP: {ip}")
        enrichment_result = enrich_ip(ip)
        if enrichment_result:
            flattened_data = flatten_json(enrichment_result)
            enriched_data.append(flattened_data)

    write_to_csv(enriched_data, output_file_path)
