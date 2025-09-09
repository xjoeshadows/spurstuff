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


def enrich_single_ip(ip: str, token: str) -> tuple:
    """Enriches a single IP address with retry and backoff logic."""
    headers = {"Token": token}
    api_url = f"https://api.spur.us/v2/context/{ip}"
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            result = {"queryIP": ip, "enrichmentData": response.json()}
            return ("success", result)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code in RETRY_STATUS_CODES and attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                if attempt > 0:
                     print(f"  > Retrying {ip} in {delay:.2f}s...", file=sys.stderr)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip}: HTTP {err.response.status_code} - {err.response.reason}")
        except requests.exceptions.RequestException as err:
            if attempt < MAX_RETRIES:
                delay = BACKOFF_FACTOR * (2 ** attempt) + random.uniform(0, 1)
                print(f"  > Retrying {ip} in {delay:.2f}s...", file=sys.stderr)
                time.sleep(delay)
            else:
                return ("error", f"Failed for {ip} after {MAX_RETRIES} retries: {err}")
    return ("error", f"Failed for {ip} after {MAX_RETRIES} retries.")


def get_ips_from_user() -> list:
    """Gets IPs from either a file argument or interactive pasting, printing prompts to stderr."""
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

    print(f"\nFound {len(ip_list)} IP(s). Starting enrichment... ⚙️\n", file=sys.stderr)

    all_results = []
    failed_ips = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ip = {executor.submit(enrich_single_ip, ip, api_token): ip for ip in ip_list}
        for future in as_completed(future_to_ip):
            ip = future_to_ip[future]
            try:
                status, data = future.result()
                if status == "success":
                    # Print real-time JSONL to stdout
                    print(json.dumps(data))
                    # Also collect it for the final save
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

    # --- NEW: Re-introduced the prompt to save the collected results ---
    while True:
        try:
            # Write prompt to stderr to not interfere with stdout redirection
            sys.stderr.write(f"\nSave all {len(all_results)} successful results to a file? (yes/no): ")
            sys.stderr.flush()
            # Read response from stdin
            export_choice = sys.stdin.readline().strip().lower()

            if export_choice in ["yes", "y"]:
                current_date_str = datetime.now().strftime('%Y%m%d')
                filename = f"{current_date_str}IPEnrichment.json"
                
                print(f"Writing to {filename}...", file=sys.stderr)
                with open(filename, 'w') as json_file:
                    # Save as a proper, pretty-printed JSON array
                    json.dump(all_results, json_file, indent=4)
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
