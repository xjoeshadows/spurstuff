#!/usr/bin/env python3

import requests
import json
import sys
import os
import time
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import ipaddress
from datetime import datetime

# --- Configuration ---
MAX_WORKERS = 10
MAX_RETRIES = 3
BACKOFF_FACTOR = 1
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def enrich_single_ip(ip: str, token: str, date_str: str = None, use_mmgeo: bool = False) -> tuple:
    """Enriches a single IP and returns the RAW response from the API."""
    headers = {"Token": token}
    base_url = f"https://api.spur.us/v2/context/{ip}"
    params = []

    if date_str:
        params.append(f"dt={date_str}")
    if use_mmgeo:
        params.append("mmgeo=1")

    api_url = base_url
    if params:
        api_url += "?" + "&".join(params)
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            return ("success", response.json())
        except requests.exceptions.HTTPError as err:
            if err.response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip}: HTTP {err.response.status_code}")
        except requests.exceptions.RequestException as err:
            if attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip} after {MAX_RETRIES} retries: {err}")
    return ("error", f"Failed for {ip} after {MAX_RETRIES} retries.")


def enrich_single_tag(tag: str, token: str) -> tuple:
    """Retrieves metadata for a single service tag."""
    headers = {"Token": token}
    api_url = f"https://api.spur.us/v2/metadata/tags/{tag}"

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            return ("success", response.json())
        except requests.exceptions.HTTPError as err:
            if err.response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return ("error", f"Failed for tag '{tag}': HTTP {err.response.status_code}")
        except requests.exceptions.RequestException as err:
            if attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                time.sleep(delay)
            else:
                return ("error", f"Failed for tag '{tag}' after {MAX_RETRIES} retries: {err}")
    return ("error", f"Failed for tag '{tag}' after {MAX_RETRIES} retries.")

def get_items_from_user(item_type: str) -> list:
    """Gets items (IPs or tags) from a file (Text or Excel) or interactive pasting."""
    input_text = ""
    if len(sys.argv) > 1:
        filepath = sys.argv[1]

        if filepath.lower().endswith(('.xlsx', '.xls')):
            try:
                print(f"✅ Reading {item_type} from Excel file: {filepath}", file=sys.stderr)
                df = pd.read_excel(filepath, header=None)
                # Join all cells from the first column into a single string, separated by spaces
                input_text = ' '.join(df[0].dropna().astype(str).tolist())
            except Exception as e:
                print(f"❌ Error reading Excel file: {e}", file=sys.stderr)
                return []
        else:  # Text file
            try:
                print(f"✅ Reading {item_type} from text file: {filepath}", file=sys.stderr)
                with open(filepath, 'r', encoding='utf-8') as f:
                    input_text = f.read()
            except FileNotFoundError:
                print(f"❌ Error: File not found at '{filepath}'", file=sys.stderr)
                return []
            except UnicodeDecodeError:
                print(f"❌ Error: Could not read file as UTF-8.", file=sys.stderr)
                return []
    else:  # Interactive
        script_name = os.path.basename(sys.argv[0])
        print(f"💡 Tip: For large lists, run with a filename: ./{script_name} {item_type.capitalize()}.txt\n", file=sys.stderr)
        print("--- Interactive Mode ---", file=sys.stderr)
        print(f"Paste {item_type}, then press Enter on a blank line to continue.", file=sys.stderr)
        lines = []
        while True:
            try:
                line = input()
                if line:
                    lines.append(line)
                else:
                    break
            except EOFError:
                break
        input_text = "\n".join(lines)

    processed_text = input_text.replace(',', ' ')
    raw_items = [item.strip() for item in processed_text.split() if item.strip()]

    # If we're not looking for IPs, just return the raw list (e.g., for tags)
    if item_type != "IPs":
        return raw_items

    # If we are looking for IPs, expand CIDRs
    final_items = []
    for item in raw_items:
        if '/' in item:
            try:
                network = ipaddress.ip_network(item, strict=False)
                num_ips = network.num_addresses

                if num_ips > 1_000_000:
                    print(f"⚠️ WARNING: CIDR range '{item}' contains {num_ips:,} IP addresses, which is more than 1 million.", file=sys.stderr)
                    sys.stderr.write("Are you sure you want to proceed with all lookups? (yes/no): ")
                    sys.stderr.flush()
                    confirm = sys.stdin.readline().strip().lower()
                    if confirm not in ['y', 'yes']:
                        print(f"Skipping CIDR range '{item}'.", file=sys.stderr)
                        continue

                final_items.extend([str(ip) for ip in network])

            except ValueError:
                # Not a valid CIDR, treat as a single item.
                final_items.append(item)
        else:
            final_items.append(item)

    return final_items

def get_historical_date() -> str | None:
    """Asks user if they want to perform a historical lookup."""
    while True:
        sys.stderr.write("\nPerform a historical lookup for a specific date? (yes/no): ")
        sys.stderr.flush()
        choice = sys.stdin.readline().strip().lower()
        if choice in ['no', 'n']: return None
        if choice in ['yes', 'y']:
            while True:
                sys.stderr.write("Enter date in YYYYMMDD format: ")
                sys.stderr.flush()
                date_input = sys.stdin.readline().strip()
                try:
                    datetime.strptime(date_input, '%Y%m%d')
                    return date_input
                except ValueError:
                    print("❌ Invalid format. Please use YYYYMMDD.", file=sys.stderr)
        print("Invalid input. Please enter 'yes' or 'no'.", file=sys.stderr)

def get_mmgeo_preference() -> bool:
    """Asks user if they want to use MaxMind for geolocation."""
    while True:
        sys.stderr.write("\nUse MaxMind for geolocation (mmgeo=1)? (yes/no): ")
        sys.stderr.flush()
        choice = sys.stdin.readline().strip().lower()
        if choice in ['no', 'n']: return False
        if choice in ['yes', 'y']: return True
        print("Invalid input. Please enter 'yes' or 'no'.", file=sys.stderr)


def run_enrichment_flow():
    """Main management function for the enrichment flow."""
    api_token = os.getenv("TOKEN")
    if not api_token:
        print("❌ Error: The 'TOKEN' environment variable is not set.", file=sys.stderr)
        return

    # --- Mode selection ---
    while True:
        sys.stderr.write("\nSelect lookup type:\n")
        sys.stderr.write("  1: IP Context Enrichment\n")
        sys.stderr.write("  2: Service Tag Metadata\n")
        sys.stderr.write("Enter choice (1 or 2): ")
        sys.stderr.flush()
        mode_choice = sys.stdin.readline().strip()
        if mode_choice in ['1', '2']:
            break
        print("Invalid choice. Please enter 1 or 2.", file=sys.stderr)

    # --- Set up parameters based on mode ---
    if mode_choice == '1':
        item_type = "IPs"
        item_list = get_items_from_user(item_type)
        if not item_list:
            print(f"\nNo {item_type} were found. Exiting.", file=sys.stderr)
            return

        date_str = get_historical_date()
        use_mmgeo = get_mmgeo_preference()
        
        print(f"\nFound {len(item_list)} {item_type}. Starting enrichment... ⚙️\n", file=sys.stderr)
        
        filename_prefix = date_str or datetime.now().strftime('%Y%m%d')
        default_filename = f"{filename_prefix}_IPEnrichment.jsonl"

    else:  # mode_choice == '2'
        item_type = "tags"
        item_list = get_items_from_user(item_type)
        if not item_list:
            print(f"\nNo {item_type} were found. Exiting.", file=sys.stderr)
            return

        print(f"\nFound {len(item_list)} {item_type}. Starting lookup... ⚙️\n", file=sys.stderr)
        
        filename_prefix = datetime.now().strftime('%Y%m%d')
        default_filename = f"{filename_prefix}_TagMetadata.jsonl"

    # --- Common execution and result handling ---
    all_results = []
    failed_items = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        if mode_choice == '1':
            future_to_item = {executor.submit(enrich_single_ip, ip, api_token, date_str, use_mmgeo): ip for ip in item_list}
        else:  # mode_choice == '2'
            future_to_item = {executor.submit(enrich_single_tag, tag, api_token): tag for tag in item_list}

        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                status, data = future.result()
                if status == "success":
                    print(json.dumps(data, ensure_ascii=False))
                    all_results.append(data)
                else:
                    failed_items.append(data)
            except Exception as exc:
                failed_items.append(f"Unexpected error for {item}: {exc}")

    print("\nEnrichment complete.", file=sys.stderr)
    
    if failed_items:
        print("\n--- Summary of Errors ---", file=sys.stderr)
        for error in failed_items:
            print(f"  ! {error}", file=sys.stderr)

    if not all_results:
        return

    while True:
        try:
            sys.stderr.write("\nSave results to a file? (yes/no): ")
            sys.stderr.flush()
            if sys.stdin.readline().strip().lower() in ["yes", "y"]:
                sys.stderr.write(f"Enter filename (default: {default_filename}): ")
                sys.stderr.flush()
                filename = sys.stdin.readline().strip() or default_filename
                
                with open(filename, 'w', encoding='utf-8') as f:
                    for record in all_results:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                print(f"✅ Success! Data exported to: {filename}", file=sys.stderr)
                break
            else:
                break
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    run_enrichment_flow()
