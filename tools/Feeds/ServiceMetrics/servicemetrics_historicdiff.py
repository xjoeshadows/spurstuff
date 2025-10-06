#!/usr/bin/env python3
import requests
import gzip
import json
import io
import sys
import re
import os
from datetime import datetime

# --- Configuration ---
TODAY_FEED_URL = "https://feeds.spur.us/v2/service-metrics/latest.json.gz"
HISTORIC_FEED_BASE_URL = "https://feeds.spur.us/v2/service-metrics"

def get_api_token():
    """
    Checks the environment for the TOKEN variable. 
    If not found, prompts the user to enter it.
    """
    # Check for TOKEN in environment variables
    token = os.environ.get("TOKEN")
    
    if token:
        print("✅ Using TOKEN from environment variable.")
        return token
    else:
        print("❌ TOKEN environment variable not found.")
        print("Please supply your API token to proceed.")
        
        while True:
            # Masking input is difficult in standard Python terminal, so we just prompt.
            user_token = input("Enter API Token: ").strip()
            if user_token:
                return user_token
            else:
                print("Token cannot be empty. Please try again.")


def fetch_and_extract(url, feed_name, token):
    """Fetches, decompresses, and extracts the set of 'tag' values from a feed URL."""
    print(f"--- Fetching {feed_name} feed from: {url} ---")

    headers = {"Token": token}
    
    try:
        # 1. Fetch the compressed data
        # 'stream=True' is a good practice, but since we read all at once anyway, it's fine.
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=30)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        print(f"✅ Downloaded {len(response.content) / 1024:.2f} KB.")

        # 2. Decompress the data in memory
        gzipper = gzip.GzipFile(fileobj=io.BytesIO(response.content))
        data = gzipper.read().decode('utf-8')
        
        # 3. Parse the JSON and extract tags
        tags = set()
        
        try:
            # Attempt 1: Parse as a single JSON array
            records = json.loads(data)
            if isinstance(records, list):
                tags = {record['tag'] for record in records if 'tag' in record}
            else:
                raise TypeError # Force fallback if top level is not a list
        
        except (json.JSONDecodeError, TypeError):
            # Attempt 2: Parse as JSON Lines (one object per line)
            sys.stdout.write("Trying JSON Lines parsing...\n") # Use sys.stdout.write for immediate output
            try:
                for line in data.splitlines():
                    if line.strip():
                        record = json.loads(line)
                        if 'tag' in record:
                            tags.add(record['tag'])
            except json.JSONDecodeError:
                print(f"❌ ERROR: Failed to parse {feed_name} data as both JSON Array and JSON Lines.")
                return None
        
        if not tags:
            print(f"❌ ERROR: Extracted 0 tags. Check the JSON structure for a 'tag' key.")
            return None

        print(f"✅ Extracted {len(tags)} unique tags.")
        return tags

    except requests.exceptions.HTTPError as e:
        print(f"❌ ERROR: HTTP request failed ({e.response.status_code}). Check your token and date.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Network or Request error: {e}")
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        return None

def get_historic_date():
    """Prompts the user for a historic date in YYYYMMDD format."""
    while True:
        date_str = input("\nEnter a historic date to compare (YYYYMMDD): ").strip()
        if re.fullmatch(r'\d{8}', date_str):
            try:
                datetime.strptime(date_str, '%Y%m%d')
                return date_str
            except ValueError:
                print("Invalid date. Please ensure it is a real date (e.g., 20240131).")
        else:
            print("Invalid format. Please use YYYYMMDD (e.g., 20201015).")

def main():
    """Main function to run the script logic."""
    
    # Get the token first
    api_token = get_api_token()
    if not api_token:
        print("Cannot proceed without a token.")
        sys.exit(1)

    # 1. Fetch Today's Feed
    today_tags = fetch_and_extract(TODAY_FEED_URL, "TODAY'S", api_token)
    
    if today_tags is None:
        sys.exit(1)

    # 2. Ask user for historic date
    historic_date = get_historic_date()
    historic_url = f"{HISTORIC_FEED_BASE_URL}/{historic_date}/feed.json.gz"
    
    # 3. Fetch Historic Feed
    historic_tags = fetch_and_extract(historic_url, f"HISTORIC ({historic_date})", api_token)

    if historic_tags is None:
        sys.exit(1)

    # 4. Calculate Diff using set operations
    
    # Tags ADDED: Present Today, NOT Present Historically (Today - Historic)
    tags_added = today_tags - historic_tags
    
    # Tags REMOVED: Present Historically, NOT Present Today (Historic - Today)
    tags_removed = historic_tags - today_tags

    # 5. Show Results
    print("\n" + "="*55)
    print(f"📊 TAG DIFFERENCE (Today vs. {historic_date})")
    print("="*55)

    print(f"\n➕ Tags Added Since {historic_date} ({len(tags_added)} total):")
    if tags_added:
        # Sort and print
        for tag in sorted(list(tags_added)):
            print(f"  - {tag}")
    else:
        print("  (None)")

    print(f"\n➖ Tags Removed Since {historic_date} ({len(tags_removed)} total):")
    if tags_removed:
        # Sort and print
        for tag in sorted(list(tags_removed)):
            print(f"  - {tag}")
    else:
        print("  (None)")

    print("\n✨ Diff complete.")

if __name__ == "__main__":
    main()