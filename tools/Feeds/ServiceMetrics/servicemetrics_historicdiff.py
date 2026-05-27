#!/usr/bin/env python3
import requests
import gzip
import json
import io
import sys
import re
import os
import textwrap
from datetime import datetime
from collections import Counter

# --- Configuration ---
TODAY_FEED_URL = "https://feeds.spur.us/v2/service-metrics/latest.json.gz"
HISTORIC_FEED_BASE_URL = "https://feeds.spur.us/v2/service-metrics"

def get_api_token():
    """
    Checks the environment for the TOKEN variable. 
    If not found, prompts the user to enter it.
    """
    token = os.environ.get("TOKEN")
    
    if token:
        print("✅ Using TOKEN from environment variable.")
        return token
    else:
        print("❌ TOKEN environment variable not found.")
        print("Please supply your API token to proceed.")
        
        while True:
            user_token = input("Enter API Token: ").strip()
            if user_token:
                return user_token
            else:
                print("Token cannot be empty. Please try again.")

def fetch_and_extract(url, feed_name, token):
    """
    Fetches, decompresses, and extracts data from a feed URL.
    Returns a dictionary mapping 'tag' to the full JSON record.
    """
    print(f"--- Fetching {feed_name} feed from: {url} ---")

    headers = {"Token": token}
    
    try:
        # 1. Fetch the compressed data
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=30)
        response.raise_for_status() 
        print(f"✅ Downloaded {len(response.content) / 1024:.2f} KB.")

        # 2. Decompress the data in memory
        gzipper = gzip.GzipFile(fileobj=io.BytesIO(response.content))
        data = gzipper.read().decode('utf-8')
        
        # 3. Parse the JSON and extract into a dictionary {tag: full_record}
        records_dict = {}
        
        try:
            # Attempt 1: Parse as a single JSON array
            records = json.loads(data)
            if isinstance(records, list):
                records_dict = {record['tag']: record for record in records if 'tag' in record}
            else:
                raise TypeError # Force fallback if top level is not a list
        
        except (json.JSONDecodeError, TypeError):
            # Attempt 2: Parse as JSON Lines (one object per line)
            sys.stdout.write("Trying JSON Lines parsing...\n")
            try:
                for line in data.splitlines():
                    if line.strip():
                        record = json.loads(line)
                        if 'tag' in record:
                            records_dict[record['tag']] = record
            except json.JSONDecodeError:
                print(f"❌ ERROR: Failed to parse {feed_name} data as both JSON Array and JSON Lines.")
                return None
        
        if not records_dict:
            print(f"❌ ERROR: Extracted 0 tags. Check the JSON structure for a 'tag' key.")
            return None

        print(f"✅ Extracted {len(records_dict)} unique tags.")
        return records_dict

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

    # 1. Fetch Today's Feed (Returns dict: {tag: record})
    today_data = fetch_and_extract(TODAY_FEED_URL, "TODAY'S", api_token)
    if today_data is None:
        sys.exit(1)

    # 2. Ask user for historic date
    historic_date = get_historic_date()
    historic_url = f"{HISTORIC_FEED_BASE_URL}/{historic_date}/feed.json.gz"
    
    # 3. Fetch Historic Feed (Returns dict: {tag: record})
    historic_data = fetch_and_extract(historic_url, f"HISTORIC ({historic_date})", api_token)
    if historic_data is None:
        sys.exit(1)

    # 4. Calculate Diff using set operations on the dictionary keys
    tags_added = set(today_data.keys()) - set(historic_data.keys())
    tags_removed = set(historic_data.keys()) - set(today_data.keys())

    # 5. Calculate Category Counts for Added Tags
    category_counts = Counter()
    for tag in tags_added:
        record = today_data[tag]
        categories = record.get('categories', [])
        
        if not categories:
            category_counts["Uncategorized"] += 1
        else:
            for category in categories:
                category_counts[category] += 1

    # 6. Show Basic Diff Results
    print("\n" + "="*55)
    print(f"📊 TAG DIFFERENCE (Today vs. {historic_date})")
    print("="*55)

    # Display the new category breakdown section
    if tags_added:
        print(f"\n📈 Category Breakdown for Added Tags:")
        # .most_common() sorts by highest count first
        for category, count in category_counts.most_common():
            print(f"  - {category}: {count}")
    
    print(f"\n➕ Tags Added Since {historic_date} ({len(tags_added)} total):")
    if tags_added:
        for tag in sorted(list(tags_added)):
            print(f"  - {tag}")
    else:
        print("  (None)")

    print(f"\n➖ Tags Removed Since {historic_date} ({len(tags_removed)} total):")
    if tags_removed:
        for tag in sorted(list(tags_removed)):
            print(f"  - {tag}")
    else:
        print("  (None)")

    # 7. Ask for details on added tags
    if tags_added:
        print("\n" + "-"*55)
        show_details = input(f"Would you like to see details for the {len(tags_added)} newly added tags? (y/n): ").strip().lower()
        
        if show_details in ['y', 'yes']:
            print("\n" + "="*55)
            print("📝 ADDED TAG DETAILS")
            print("="*55)
            
            for tag in sorted(list(tags_added)):
                record = today_data[tag]
                
                categories_list = record.get('categories', [])
                categories_str = ", ".join(categories_list) if categories_list else "None"
                
                description_str = record.get('description', "")
                if not description_str:
                    description_str = "No description provided."
                
                # Print the tag clearly on its own line
                print(f"\n🏷️  {tag}")
                print(f"    Categories:  {categories_str}")
                
                # Use textwrap to keep long descriptions perfectly aligned
                wrapper = textwrap.TextWrapper(
                    initial_indent="    Description: ", 
                    subsequent_indent="                 ", 
                    width=85 # Adjust width based on your preferred terminal size
                )
                print(wrapper.fill(description_str))

    print("\n✨ Diff complete.")

if __name__ == "__main__":
    main()
