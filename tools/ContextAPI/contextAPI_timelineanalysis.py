#!/usr/bin/env python3
import requests
import os
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# --- Configuration ---
CURRENT_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}"
HISTORIC_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}?dt={date}"
OUTPUT_FILENAME = "spur_ip_analysis_timeline.jsonl" 

# --- Helper function for deep, order-agnostic comparison ---

def make_hashable(item: Any) -> Any:
    """Converts a dictionary or list into a hashable representation (JSON string or tuple)."""
    if isinstance(item, dict):
        # Convert dictionary to a canonical JSON string (sorted keys for consistency)
        return json.dumps(item, sort_keys=True)
    elif isinstance(item, list):
        # Convert list of items to a tuple of hashable items (recursive)
        return tuple(make_hashable(i) for i in item)
    return item

def compare_unordered_lists(list1: List[Any], list2: List[Any]) -> bool:
    """Compares two lists for functional equality (content, not order) using hashable items."""
    # Convert lists to sets of hashable items for comparison
    hashable_set1 = set(make_hashable(i) for i in list1)
    hashable_set2 = set(make_hashable(i) for i in list2)
    return hashable_set1 == hashable_set2

def deep_diff_recursive(old_data: Dict[str, Any], new_data: Dict[str, Any], path: str = "") -> Optional[Dict[str, Any]]:
    """
    Performs a recursive deep-diff on two dictionaries.
    New keys are now reported as a value change from None to the new value.
    """
    changes = {
        'keys_disappeared': [],
        'value_changes': {}
    }
    
    old_keys = set(old_data.keys())
    new_keys = set(new_data.keys())
    
    # --- Key/Value Changes ---
    
    # 1. New keys are treated as value changes from None to the new value
    for key in new_keys - old_keys:
        current_path = f"{path}{key}"
        changes['value_changes'][current_path] = {
            'old_value': None, # Indicates a new key appeared
            'new_value': new_data[key]
        }
    
    # 2. Disappeared keys
    for key in old_keys - new_keys:
        changes['keys_disappeared'].append(f"{path}{key}")

    # 3. Value changes and Nested Recursion
    for key in old_keys.intersection(new_keys):
        old_val = old_data.get(key)
        new_val = new_data.get(key)
        current_path = f"{path}{key}"

        # Recurse if both are dictionaries
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            nested_changes = deep_diff_recursive(old_val, new_val, path=f"{current_path}.")
            if nested_changes:
                # Merge nested changes into the parent's change lists
                changes['keys_disappeared'].extend(nested_changes['keys_disappeared'])
                changes['value_changes'].update(nested_changes['value_changes'])
            continue

        # Handle list comparison (order-agnostic)
        is_list_and_different_content = (
            isinstance(old_val, list) and isinstance(new_val, list) and 
            not compare_unordered_lists(old_val, new_val)
        )

        # Handle standard comparison (for strings, numbers, or non-dict/list items)
        is_standard_different = not (isinstance(old_val, dict) or isinstance(new_val, dict) or isinstance(old_val, list) or isinstance(new_val, list)) and old_val != new_val

        # Handle type change (e.g., string to list) or differing types
        is_type_different = type(old_val) != type(new_val)

        if is_list_and_different_content or is_standard_different or is_type_different:
            changes['value_changes'][current_path] = {
                'old_value': old_val,
                'new_value': new_val
            }

    # Clean up empty change lists before returning
    if not changes['keys_disappeared'] and not changes['value_changes']:
        return None
        
    return changes


# --- AUTH/FETCH/INPUT/OUTPUT Functions (Unchanged) ---

def get_spur_token():
    """Checks for the TOKEN environment variable or prompts the user."""
    token = os.environ.get("TOKEN")
    if not token:
        print("Spur Token not found in environment variable 'TOKEN'.")
        token = input("Please enter your Spur Token: ").strip()
        if not token:
            print("Error: Spur Token is required. Exiting.")
            exit(1)
    return token

def load_ips(ip_file=None):
    """Loads IP addresses from a file or prompts the user."""
    ips = []
    if ip_file:
        try:
            with open(ip_file, 'r') as f:
                ips = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"Error: Input file '{ip_file}' not found.")
            exit(1)
    else:
        ip_input = input("Enter one or more IP addresses (separated by commas or spaces): ").strip()
        ips = [ip.strip() for ip in ip_input.replace(',', ' ').split() if ip.strip()]

    if not ips:
        print("Error: No IP addresses provided. Exiting.")
        exit(1)

    return list(set(ips)) 

def get_historical_dates():
    """Prompts the user for a historical look-up span and generates YYYYMMDD dates."""
    while True:
        span = input("\nEnter historical look-up span (e.g., '30 days', '4 weeks'): ").strip().lower()
        parts = span.split()
        
        if len(parts) == 2 and parts[0].isdigit():
            num = int(parts[0])
            unit = parts[1].rstrip('s')
            
            if unit == 'day':
                delta = timedelta(days=num)
                break
            elif unit == 'week':
                delta = timedelta(weeks=num)
                break
            else:
                print("Invalid unit. Please use 'day(s)' or 'week(s)'.")
        else:
            print("Invalid format. Please use 'X days' or 'X weeks'.")

    end_date = datetime.now().date()
    start_date = end_date - delta
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)

    return dates

def fetch_ip_data(ip, date_list, token):
    """
    Fetches current and historical data for a single IP.
    """
    print(f"\n--- Fetching data for IP: **{ip}** ---")
    results = {}
    headers = {"Token": token}
    today_dt = datetime.now().strftime("%Y%m%d")
    
    for dt in date_list:
        if dt == today_dt:
            url_to_use = CURRENT_CONTEXT_URL.format(ip=ip)
            print(f"  -> Fetching **current** context ({dt})...")
        else:
            url_to_use = HISTORIC_CONTEXT_URL.format(ip=ip, date=dt)
            print(f"  -> Fetching **historical** context for {dt}...")

        try:
            response = requests.get(url_to_use, headers=headers)
            response.raise_for_status()
            results[dt] = response.json()
        except requests.exceptions.RequestException as e:
            if response.status_code == 401:
                print("  -> ERROR: 401 Unauthorized. Check your Spur Token.")
                exit(1)
            elif response.status_code not in (400, 404):
                 print(f"  -> API error fetching data for {dt}: {e}")
            print(f"  -> No data found for {dt} or API error. Skipping.")
    
    sorted_results = {k: results[k] for k in sorted(results.keys())}
    return sorted_results

# --- CORE Logic Functions ---

def diff_json(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Main entry point for deep-diff.
    """
    return deep_diff_recursive(old_data, new_data)

def analyze_timeline(ip_results):
    """
    Performs a timeline analysis on the sorted results.
    """
    timeline = []
    dates = list(ip_results.keys())
    
    if not dates:
        return timeline

    # Initial Context
    timeline.append({
        'date': dates[0],
        'type': 'Initial Context',
        'full_context': ip_results[dates[0]], 
        'changes': {} 
    })

    # Compare subsequent dates to the immediate previous date
    for i in range(1, len(dates)):
        prev_data = ip_results[dates[i-1]]
        current_data = ip_results[dates[i]]
        
        # Primary check: Skip if data is identical
        if prev_data == current_data:
            continue
            
        # Call the deep diff function
        diff = diff_json(prev_data, current_data)
        
        if diff:
            # Only record if there are notable changes after deep comparison
            timeline.append({
                'date': dates[i],
                'type': 'Change Detected (Compared to ' + dates[i-1] + ')',
                'changes': diff,
            })
            
    return timeline

def print_timeline_to_terminal(ip, timeline_analysis):
    """Prints the analyzed timeline for a single IP in a user-friendly format."""
    print(f"\n==============================================")
    print(f"🔍 **Timeline Analysis for IP: {ip}**")
    print(f"==============================================")

    if not timeline_analysis:
        print("No historical data or notable changes observed.")
        return

    for event in timeline_analysis:
        date_str = event['date']
        
        try:
            formatted_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            formatted_date = date_str

        print(f"\n--- **Date: {formatted_date}** ({event['type']}) ---")

        changes = event.get('changes', {})
        
        if event['type'] == 'Initial Context':
            print("  (This entry represents the baseline context on this date)")
            continue

        # --- Keys and Values Added/Changed ---
        value_changes = changes.get('value_changes', {})
        
        print("\nChanges Observed:")
        
        if value_changes:
            for key_path, vals in value_changes.items():
                # Dump complex objects (lists, dicts) to JSON string for clear printing
                old = json.dumps(vals['old_value'], sort_keys=True) if isinstance(vals['old_value'], (dict, list)) else vals['old_value']
                new = json.dumps(vals['new_value'], sort_keys=True) if isinstance(vals['new_value'], (dict, list)) else vals['new_value']
                
                if vals['old_value'] is None:
                    # New Key Appeared (value was None)
                    print(f"    ➕ **KEY ADDED: {key_path}** with value: `{new}`")
                else:
                    # Existing Value Modified
                    print(f"    🔄 **VALUE MODIFIED: {key_path}** changed from `{old}` to `{new}`")
        else:
             print("  (None added or modified)")


        # --- Keys Removed ---
        keys_removed = changes.get('keys_disappeared', [])
        
        print("\nKeys Removed:")
        if keys_removed:
            print("  ➖ **KEYS REMOVED:**")
            for key in keys_removed:
                print(f"    - {key}")
        else:
            print("  (None removed)")
            

def main():
    """Main function to run the script."""
    
    parser = argparse.ArgumentParser(description="Spur IP Historical Enrichment & Analysis Script.")
    parser.add_argument("ip_file", nargs='?', help="Optional path to a file containing IP addresses (one per line).")
    args = parser.parse_args()
    
    print("✨ Starting Spur IP Historical Analysis Script...")

    token = get_spur_token()
    ips_to_check = load_ips(args.ip_file)
    print(f"\n🚀 Checking **{len(ips_to_check)}** unique IP address(es): {', '.join(ips_to_check)}")
    
    date_list = get_historical_dates()
    print(f"✅ Will look up dates from **{date_list[0]}** to **{date_list[-1]}**.")
    
    print(f"\n\n--- Outputting and Exporting Results ---")
    try:
        with open(OUTPUT_FILENAME, 'w') as f: 
            for ip in ips_to_check:
                ip_data_by_date = fetch_ip_data(ip, date_list, token)
                
                if not ip_data_by_date:
                    record = {"ip": ip, "date": "N/A", "type": "Error", "message": "No data found for this IP in the specified time range."}
                    f.write(json.dumps(record) + '\n')
                    print(f"\n--- No data found for {ip}. ---\n")
                    continue
                    
                timeline_analysis = analyze_timeline(ip_data_by_date)
                
                print_timeline_to_terminal(ip, timeline_analysis)
                
                for event in timeline_analysis:
                    record = {
                        "ip": ip,
                        "date": event['date'],
                        "type": event['type'],
                        "changes": event.get('changes', {}), 
                    }
                    if 'full_context' in event:
                         record['full_context'] = event['full_context']
                    
                    json_line = json.dumps(record)
                    f.write(json_line + '\n')

        print(f"\n\n🎉 Success! All timeline events also exported to **{OUTPUT_FILENAME}**.")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    main()