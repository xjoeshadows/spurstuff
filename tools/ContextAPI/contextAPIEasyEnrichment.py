#!/usr/bin/env python3

import requests
import json
import sys
import os
import time
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def get_ips_from_user() -> list:
    """Gets IPs from a file (Text or Excel) or interactive pasting."""
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        
        if filepath.lower().endswith(('.xlsx', '.xls')):
            try:
                print(f"✅ Reading IPs from Excel file: {filepath}", file=sys.stderr)
                df = pd.read_excel(filepath, header=None)
                return df[0].dropna().astype(str).tolist()
            except Exception as e:
                print(f"❌ Error reading Excel file: {e}", file=sys.stderr)
                return []
        
        try:
            print(f"✅ Reading IPs from text file: {filepath}", file=sys.stderr)
            with open(filepath, 'r', encoding='utf-8') as f:
                input_text = f.read()
        except FileNotFoundError:
            print(f"❌ Error: File not found at '{filepath}'", file=sys.stderr)
            return []
        except UnicodeDecodeError:
            print(f"❌ Error: Could not read file as UTF-8.", file=sys.stderr)
            return []
    else:
        print("💡 Tip: For large lists, run with a filename: ./contextAPI_Easyenrichment.py IPs.xlsx\n", file=sys.stderr)
        print("--- Interactive Mode ---", file=sys.stderr)
        print("Paste IPs, then press Enter on a blank line to continue.", file=sys.stderr)
        lines = []
        while True:
            try:
                line = input()
                if line: lines.append(line)
                else: break
            except EOFError: break
        input_text = "\n".join(lines)

    processed_text = input_text.replace(',', ' ')
    return [ip.strip() for ip in processed_text.split() if ip.strip()]

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

    ip_list = get_ips_from_user()
    if not ip_list:
        print("\nNo IP addresses were found. Exiting.", file=sys.stderr)
        return

    date_str = get_historical_date()
    use_mmgeo = get_mmgeo_preference()

    print(f"\nFound {len(ip_list)} IP(s). Starting enrichment... ⚙️\n", file=sys.stderr)

    all_results = []
    failed_ips = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ip = {executor.submit(enrich_single_ip, ip, api_token, date_str, use_mmgeo): ip for ip in ip_list}
        for future in as_completed(future_to_ip):
            ip = future_to_ip[future]
            try:
                status, data = future.result()
                if status == "success":
                    # --- FIX: Added ensure_ascii=False for terminal output ---
                    print(json.dumps(data, ensure_ascii=False))
                    all_results.append(data)
                else:
                    failed_ips.append(data)
            except Exception as exc:
                failed_ips.append(f"Unexpected error for {ip}: {exc}")

    print("\nEnrichment complete.", file=sys.stderr)
    
    if failed_ips:
        print("\n--- Summary of Errors ---", file=sys.stderr)
        for error in failed_ips:
            print(f"  ! {error}", file=sys.stderr)

    if not all_results:
        return

    while True:
        try:
            sys.stderr.write(f"\nSave results to a file? (yes/no): ")
            sys.stderr.flush()
            if sys.stdin.readline().strip().lower() in ["yes", "y"]:
                filename_date = date_str or datetime.now().strftime('%Y%m%d')
                default_fn = f"{filename_date}IPEnrichment.json"
                
                sys.stderr.write(f"Enter filename (default: {default_fn}): ")
                sys.stderr.flush()
                filename = sys.stdin.readline().strip() or default_fn
                
                # --- FIX: Ensure UTF-8 encoding and avoid ASCII escaping ---
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
