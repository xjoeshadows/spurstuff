#!/usr/bin/env python3

import requests
import json
import sys
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- Configuration ---
MAX_WORKERS = 10
MAX_RETRIES = 3
BACKOFF_FACTOR = 1
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def enrich_single_ip(ip: str, token: str, date_str: str = None) -> tuple:
    """
    Enriches a single IP, optionally for a specific date, with retry logic.
    """
    headers = {"Token": token}
    api_url = f"https://api.spur.us/v2/context/{ip}"
    if date_str:
        api_url += f"?dt={date_str}"
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            result = {
                "query": {"ip": ip, "lookupDate": date_str},
                "enrichmentData": response.json()
            }
            return ("success", result)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                if attempt > 0:
                     print(f"  > Retrying {ip} in {delay:.2f}s...", file=sys.stderr)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip} (date: {date_str}): HTTP {err.response.status_code} - {err.response.reason}")
        except requests.exceptions.RequestException as err:
            if attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                print(f"  > Retrying {ip} in {delay:.2f}s...", file=sys.stderr)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip} (date: {date_str}) after {MAX_RETRIES} retries: {err}")
    return ("error", f"Failed for {ip} (date: {date_str}) after {MAX_RETRIES} retries.")


def get_ips_from_user() -> list:
    """Gets IPs from a file or interactive pasting."""
    input_text = ""
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
        print(f"✅ Reading IPs from file: {filepath}", file=sys.stderr)
        try:
            with open(filepath, 'r') as f:
                input_text = f.read()
        except FileNotFoundError:
            print(f"❌ Error: File not found at '{filepath}'", file=sys.stderr)
            return []
    else:
        print("💡 Tip: For large lists, run with a filename: ./enrich.py my_ips.txt\n", file=sys.stderr)
        print("--- Interactive Mode ---", file=sys.stderr)
        print("Paste IPs, then press Enter on a blank line to continue.", file=sys.stderr)
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
    return [ip.strip() for ip in processed_text.split() if ip.strip()]

def get_historical_date() -> str | None:
    """Asks user if they want to perform a historical lookup and validates date."""
    while True:
        sys.stderr.write("Perform a historical lookup for a specific date? (yes/no): ")
        sys.stderr.flush()
        choice = sys.stdin.readline().strip().lower()

        if choice in ['no', 'n']:
            return None
        
        if choice in ['yes', 'y']:
            while True:
                sys.stderr.write("Enter date in YYYYMMDD format: ")
                sys.stderr.flush()
                date_input = sys.stdin.readline().strip()
                try:
                    datetime.strptime(date_input, '%Y%m%d')
                    return date_input
                except ValueError:
                    print("❌ Invalid format or date. Please use YYYYMMDD.", file=sys.stderr)
        
        print("Invalid input. Please enter 'yes' or 'no'.", file=sys.stderr)


def run_enrichment_flow():
    """Main function to manage the enrichment workflow."""
    api_token = os.getenv("TOKEN")
    if not api_token:
        print("❌ Error: The 'TOKEN' environment variable is not set.", file=sys.stderr)
        return

    ip_list = get_ips_from_user()
    if not ip_list:
        print("\nNo IP addresses were provided. Exiting.", file=sys.stderr)
        return

    date_str = get_historical_date()

    if date_str:
        print(f"\nPerforming historical lookup for date: {date_str}", file=sys.stderr)
    
    print(f"Found {len(ip_list)} IP(s). Starting enrichment... ⚙️\n", file=sys.stderr)

    all_results = []
    failed_ips = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ip = {executor.submit(enrich_single_ip, ip, api_token, date_str): ip for ip in ip_list}
        for future in as_completed(future_to_ip):
            ip = future_to_ip[future]
            try:
                status, data = future.result()
                if status == "success":
                    print(json.dumps(data))
                    all_results.append(data)
                else:
                    failed_ips.append(data)
            except Exception as exc:
                failed_ips.append(f"An unexpected error occurred for {ip}: {exc}")

    print("\nEnrichment complete.", file=sys.stderr)
    
    if failed_ips:
        print("\n--- Summary of Errors ---", file=sys.stderr)
        for error in failed_ips:
            print(f"  ! {error}", file=sys.stderr)

    if not all_results:
        print("\nNo data was successfully enriched.", file=sys.stderr)
        return

    while True:
        try:
            sys.stderr.write(f"\nSave all {len(all_results)} successful results to a file? (yes/no): ")
            sys.stderr.flush()
            export_choice = sys.stdin.readline().strip().lower()

            if export_choice in ["yes", "y"]:
                
                # --- MODIFIED: Use the historical date for the filename if it exists ---
                # Otherwise, use the current date
                filename_date = date_str or datetime.now().strftime('%Y%m%d')
                filename = f"{filename_date}IPEnrichment.json"
                
                print(f"Writing to {filename}...", file=sys.stderr)
                with open(filename, 'w') as json_file:
                    for record in all_results:
                        json_file.write(json.dumps(record) + '\n')

                print(f"✅ Success! Data exported to: {filename}", file=sys.stderr)
                break
            elif export_choice in ["no", "n"]:
                print("\nOK. Data not saved.", file=sys.stderr)
                break
            else:
                print("Invalid input. Please enter 'yes' or 'no'.", file=sys.stderr)
        except (IOError, OSError) as e:
            print(f"\n❌ Error writing file: {e}", file=sys.stderr)
            break
        except KeyboardInterrupt:
            print("\nOperation cancelled.", file=sys.stderr)
            break

if __name__ == "__main__":
    run_enrichment_flow()
