import requests
import json
import sys
import os

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
api_token = "HNRDfEWuYGCvuD3I1hCPsK"  # Replace with your actual API token
default_output_file = "ip_data.json"

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

def write_to_json(data, output_path):
    """Writes a list of JSON objects to a JSON file."""
    if not data:
        print("No data to write to JSON.")
        return

    try:
        with open(output_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=4, ensure_ascii=False)  # Use ensure_ascii=False
        print(f"Enriched data written to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")

# --- Main Script ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python enrich_ip_api.py <input_file>")
        sys.exit(1)

    input_file_path = sys.argv[1]

    if not os.path.exists(input_file_path):
        print(f"Error: Input file not found at {input_file_path}")
        sys.exit(1)

    # Extract the directory from the input file path
    output_dir = os.path.dirname(input_file_path)

    # Get the output file name from the user
    output_file_name = input("Enter the desired output file name (e.g., ip_data.json): ").strip()
    if not output_file_name:
        output_file_path = os.path.join(output_dir, default_output_file)
        print(f"Using default output file name: {default_output_file}")
    else:
        # Sanitize the filename
        output_file_name = "".join(x for x in output_file_name if x.isalnum() or x in "._-")
        if not output_file_name.endswith(".json"):
            output_file_name += ".json"
        output_file_path = os.path.join(output_dir, output_file_name)

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
            enriched_data.append(enrichment_result) #append the result

    write_to_json(enriched_data, output_file_path)
