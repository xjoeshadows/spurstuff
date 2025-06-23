import requests
import json
import sys
import os
import pandas as pd  # Import pandas
from datetime import datetime

# --- Configuration ---
api_url_base = "https://api.spur.us/v2/context/"
api_token = "HNRDfEWuYGCvuD3I1hCPsK"  # Replace with your actual API token
default_output_file = "ip_data.json"

# --- Functions ---
def enrich_ip(ip_address, timestamp=None):
    """Enriches a single IP address using the Spur API, optionally with a timestamp."""
    url = f"{api_url_base}{ip_address}"
    if timestamp:
        url += f"?dt={timestamp}"
    headers = {'TOKEN': api_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error enriching {ip_address} (timestamp={timestamp}): {e}")
        return None

def write_to_json(data, output_path):
    """Writes a list of JSON objects to a JSON file."""
    if not data:
        print("No data to write to JSON.")
        return
    try:
        with open(output_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=4)
        print(f"Enriched data written to {output_path}")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")

def read_ip_timestamp_data(input_file_path):
    """Reads IP address and timestamp data from a CSV or XLSX file using pandas."""
    try:
        if input_file_path.lower().endswith('.csv'):
            df = pd.read_csv(input_file_path)
        elif input_file_path.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(input_file_path)
        else:
            raise ValueError("Unsupported file format. Please use CSV or XLSX.")

        # Check for required columns
        if not all(col in df.columns for col in ['IP', 'Timestamp']):
            raise ValueError("Input file must contain columns named 'IP' and 'Timestamp'.")

        ip_timestamp_list = []
        for _, row in df.iterrows():
            ip = row['IP']
            timestamp_str = str(row['Timestamp']) if pd.notna(row['Timestamp']) else None
            if timestamp_str:
                try:
                    # Attempt to parse the timestamp string
                    dt_obj = datetime.fromisoformat(timestamp_str)  # Use fromisoformat
                    timestamp = dt_obj.strftime('%Y%m%d')  # Format as YYYYMMDD
                except ValueError:
                    timestamp = None

            ip_timestamp_list.append((ip, timestamp))
        return ip_timestamp_list

    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)
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

    # Read IP and timestamp data
    ip_timestamp_data = read_ip_timestamp_data(input_file_path)

    enriched_data = []
    for ip, timestamp in ip_timestamp_data:
        if ip and str(ip) != 'nan': # Check if IP is valid
            print(f"Enriching IP: {ip} with timestamp: {timestamp}")
            enrichment_result = enrich_ip(ip, timestamp)
            if enrichment_result:
                enriched_data.append(enrichment_result)
        else:
            print(f"Skipping invalid IP address: {ip}")

    write_to_json(enriched_data, output_file_path)
