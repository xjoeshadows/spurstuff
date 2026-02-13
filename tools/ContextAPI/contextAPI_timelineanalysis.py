#!/usr/bin/env python3
import requests
import os
import argparse
import json
import concurrent.futures
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union

# --- Configuration ---
CURRENT_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}"
HISTORIC_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}?dt={date}"
OUTPUT_FILENAME = "spur_ip_analysis_timeline.jsonl"
MAX_THREADS = 10 

# --- Helper Functions ---

def get_nested_value(data: Any, key_path: str) -> Any:
    """
    Retrieves a value from a nested structure using dot notation (e.g., 'tunnels.operator').
    Supports traversing lists by collecting values from all items.
    """
    keys = key_path.split('.')
    current = data
    
    for k in keys:
        if isinstance(current, dict):
            if k in current:
                current = current[k]
            else:
                return None
        elif isinstance(current, list):
            next_values = []
            for item in current:
                if isinstance(item, dict) and k in item:
                    val = item[k]
                    if isinstance(val, list):
                        next_values.extend(val)
                    else:
                        next_values.append(val)
            
            if not next_values:
                return None
            current = next_values
        else:
            return None
            
    return current

def parse_user_value(val: str) -> Any:
    """Attempts to convert user string input into native Python types."""
    if val.lower() in ('null', 'none'):
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return val

def check_match(actual_value: Any, target_value: Any) -> bool:
    """Checks equality or list membership for search."""
    if actual_value is None:
        return False
    if isinstance(actual_value, list):
        return target_value in actual_value
    return actual_value == target_value

def make_hashable(item: Any) -> Any:
    """Converts a dictionary or list into a hashable representation."""
    if isinstance(item, dict):
        return json.dumps(item, sort_keys=True)
    elif isinstance(item, list):
        return tuple(make_hashable(i) for i in item)
    return item

def compare_unordered_lists(list1: List[Any], list2: List[Any]) -> bool:
    """Compares two lists for functional equality (content, not order)."""
    hashable_set1 = set(make_hashable(i) for i in list1)
    hashable_set2 = set(make_hashable(i) for i in list2)
    return hashable_set1 == hashable_set2

def calculate_list_delta(old_list: List[Any], new_list: List[Any]) -> Tuple[List[Any], List[Any]]:
    """Calculates items added and removed between two lists (order-agnostic)."""
    old_set = set(make_hashable(i) for i in old_list)
    new_set = set(make_hashable(i) for i in new_list)
    
    added_hashable = new_set - old_set
    removed_hashable = old_set - new_set

    added = sorted([json.loads(i) if isinstance(i, str) and i.startswith(('{', '[')) else i for i in added_hashable])
    removed = sorted([json.loads(i) if isinstance(i, str) and i.startswith(('{', '[')) else i for i in removed_hashable])
    
    return added, removed

# --- Recursive Deep-Diff Logic ---

def deep_diff_recursive(old_data: Dict[str, Any], new_data: Dict[str, Any], path: str = "") -> Optional[Dict[str, Any]]:
    """Performs a recursive deep-diff on two dictionaries."""
    changes = {
        'keys_disappeared': [],
        'value_changes': {}
    }
    
    old_keys = set(old_data.keys())
    new_keys = set(new_data.keys())
    
    # 1. New keys
    for key in new_keys - old_keys:
        current_path = f"{path}{key}"
        changes['value_changes'][current_path] = {
            'old_value': None,
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

        if isinstance(old_val, dict) and isinstance(new_val, dict):
            nested_changes = deep_diff_recursive(old_val, new_val, path=f"{current_path}.")
            if nested_changes:
                changes['keys_disappeared'].extend(nested_changes['keys_disappeared'])
                changes['value_changes'].update(nested_changes['value_changes'])
            continue

        is_list_and_different_content = (
            isinstance(old_val, list) and isinstance(new_val, list) and 
            not compare_unordered_lists(old_val, new_val)
        )

        is_standard_different = not (isinstance(old_val, dict) or isinstance(new_val, dict) or isinstance(old_val, list) or isinstance(new_val, list)) and old_val != new_val
        is_type_different = type(old_val) != type(new_val)

        if is_list_and_different_content or is_standard_different or is_type_different:
            changes['value_changes'][current_path] = {
                'old_value': old_val,
                'new_value': new_val
            }

    if not changes['keys_disappeared'] and not changes['value_changes']:
        return None
        
    return changes

def diff_json(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Main entry point for deep-diff."""
    return deep_diff_recursive(old_data, new_data)

# --- API and Data Fetching ---

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
    """
    Loads IP addresses from a file or prompts the user.
    Supports multiline input (copy-pasting a list) and comma/space separation.
    """
    raw_text = ""
    
    if ip_file:
        try:
            with open(ip_file, 'r') as f:
                raw_text = f.read()
        except FileNotFoundError:
            print(f"Error: Input file '{ip_file}' not found.")
            exit(1)
    else:
        # Multiline Input Loop
        print("\nEnter IP addresses below (paste a list, comma separated, or space separated).")
        print("➡️  **Press ENTER twice (on an empty line) to finish:**")
        
        lines = []
        while True:
            try:
                line = input()
                if line.strip() == "":
                    break # Stop on empty line
                lines.append(line)
            except EOFError:
                break # Stop on Ctrl+D
        
        raw_text = "\n".join(lines)

    if not raw_text.strip():
        print("Error: No IP addresses provided. Exiting.")
        exit(1)

    # Use Regex to split on Commas (,), Newlines (\n), or Spaces (\s)
    # [,\s]+ matches one or more occurrences of any whitespace or comma
    tokens = re.split(r'[,\s]+', raw_text)
    
    # Filter out empty strings and return unique list
    unique_ips = list(set(t.strip() for t in tokens if t.strip()))
    
    if not unique_ips:
        print("Error: No valid IP addresses found in input. Exiting.")
        exit(1)

    return unique_ips

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

def fetch_single_date(ip, dt, token, today_dt):
    """Helper function to fetch data for a single date (for threading)."""
    headers = {"Token": token}
    result = None
    
    if dt == today_dt:
        url = CURRENT_CONTEXT_URL.format(ip=ip)
    else:
        url = HISTORIC_CONTEXT_URL.format(ip=ip, date=dt)

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 401:
            return dt, "401" 
        response.raise_for_status()
        result = response.json()
    except requests.exceptions.RequestException:
        result = None

    return dt, result

def fetch_ip_data(ip, date_list, token):
    """Fetches current and historical data for a single IP using Parallel Requests."""
    print(f"\n--- Fetching data for IP: **{ip}** ---")
    print(f"    (Launching {MAX_THREADS} parallel threads...)")
    
    results = {}
    today_dt = datetime.now().strftime("%Y%m%d")
    
    total_dates = len(date_list)
    completed_dates = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_date = {
            executor.submit(fetch_single_date, ip, dt, token, today_dt): dt 
            for dt in date_list
        }
        
        for future in concurrent.futures.as_completed(future_to_date):
            dt, data = future.result()
            
            if data == "401":
                print("\n  -> ERROR: 401 Unauthorized. Check your Spur Token.")
                exit(1)
            
            if data:
                results[dt] = data
            
            completed_dates += 1
            print(f"\r    ⏳ Progress: [{completed_dates}/{total_dates}] dates fetched...", end="", flush=True)

    print()
    sorted_results = {k: results[k] for k in sorted(results.keys())}
    return sorted_results

# --- Analysis & Output Functions ---

def analyze_timeline(ip_results):
    """Performs a timeline analysis on the sorted results."""
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

    for i in range(1, len(dates)):
        prev_data = ip_results[dates[i-1]]
        current_data = ip_results[dates[i]]
        
        if prev_data == current_data:
            continue
            
        diff = diff_json(prev_data, current_data)
        
        if diff:
            timeline.append({
                'date': dates[i],
                'type': 'Change Detected (Compared to ' + dates[i-1] + ')',
                'changes': diff,
            })
            
    return timeline

def analyze_attribute_presence(ip, ip_results, search_key, search_value):
    """Analyzes when a specific attribute was present."""
    dates = sorted(list(ip_results.keys()))
    presence_intervals = []
    
    if not dates:
        return

    print(f"\n🔎 **Attribute Search Results for IP: {ip}**")
    print(f"   Searching for Key:   `{search_key}`")
    print(f"   Matching Value:      `{search_value}`")
    print(f"==============================================")
    
    current_interval_start = None
    is_present = False

    for dt in dates:
        data = ip_results[dt]
        actual_val = get_nested_value(data, search_key)
        match = check_match(actual_val, search_value)

        if match and not is_present:
            current_interval_start = dt
            is_present = True
        elif not match and is_present:
            presence_intervals.append((current_interval_start, dt))
            is_present = False
            current_interval_start = None
            
    if is_present:
        presence_intervals.append((current_interval_start, "Present"))

    if not presence_intervals:
        print("❌ Attribute was NOT found in the analyzed timeframe.")
    else:
        print("✅ Attribute was PRESENT during the following periods:")
        for start, end in presence_intervals:
            try:
                fmt_start = datetime.strptime(start, "%Y%m%d").strftime("%Y-%m-%d")
            except: fmt_start = start
            
            if end == "Present":
                 print(f"   📅 {fmt_start}  ➡️  (Latest Data)")
            else:
                try:
                    end_dt_obj = datetime.strptime(end, "%Y%m%d") - timedelta(days=1)
                    fmt_end = end_dt_obj.strftime("%Y-%m-%d")
                except: fmt_end = end
                
                if fmt_start == fmt_end:
                    print(f"   📅 {fmt_start} (1 Day)")
                else:
                    print(f"   📅 {fmt_start}  ➡️  {fmt_end}")

def format_change_summary(changes: Dict[str, Any]) -> str:
    """Creates a concise, single-line summary string for all non-count changes."""
    summary_parts = []
    
    value_changes = changes.get('value_changes', {})
    
    for key_path, vals in value_changes.items():
        if key_path.endswith('.count'):
            continue 
            
        old_val = vals['old_value']
        new_val = vals['new_value']
        
        if old_val is None:
            new_str = json.dumps(new_val, sort_keys=True, ensure_ascii=False)
            summary_parts.append(f"➕ ADDED: {key_path} = {new_str}")
        elif isinstance(old_val, list) and isinstance(new_val, list):
            added, removed = calculate_list_delta(old_val, new_val)
            added_str = json.dumps(added, ensure_ascii=False)
            removed_str = json.dumps(removed, ensure_ascii=False)

            if added and removed:
                summary_parts.append(f"🔄 MODIFIED: {key_path} | +{added_str} | -{removed_str}")
            elif added:
                summary_parts.append(f"🟢 ADDED: {key_path} {added_str}")
            elif removed:
                summary_parts.append(f"🔴 REMOVED: {key_path} {removed_str}")
        else:
            summary_parts.append(f"🔄 MODIFIED: {key_path}")

    keys_removed = changes.get('keys_disappeared', [])
    if keys_removed:
        summary_parts.append(f"➖ REMOVED: {', '.join(keys_removed)}")
        
    return ' | '.join(summary_parts) if summary_parts else 'No other changes'

def print_timeline_to_terminal(ip, timeline_analysis):
    """Prints the analyzed timeline for a single IP as a markdown table."""
    print(f"\n==============================================")
    print(f"🔍 **Timeline Analysis for IP: {ip}**")
    print(f"==============================================")

    if not timeline_analysis:
        print("No historical data or notable changes observed.")
        return
        
    table = [["Date", "Client Count", "Trend", "Other Changes Summary"]]
    table.append(["---", "---", "---", "---"])
    
    for event in timeline_analysis:
        date_str = event['date']
        try:
            formatted_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            formatted_date = date_str
            
        changes = event.get('changes', {})
        value_changes = changes.get('value_changes', {})
        
        count_display = ""
        trend_display = ""
        count_change = value_changes.get('client.count')
        
        if event['type'] == 'Initial Context':
            initial_count = event['full_context'].get('client', {}).get('count', 'N/A')
            count_display = f"Initial ({initial_count})"
            trend_display = "---"
        elif count_change:
            old_count = count_change['old_value']
            new_count = count_change['new_value']
            if isinstance(old_count, int) and isinstance(new_count, int):
                diff = new_count - old_count
                trend_symbol = "⬆️" if diff > 0 else "⬇️" if diff < 0 else "="
                trend_display = f"{trend_symbol} ({'+' if diff > 0 else ''}{diff})"
                count_display = f"{new_count}"
            else:
                count_display = f"Changed: {new_count}"
                trend_display = "MODIFIED"
        
        summary = format_change_summary(changes)
        table.append([formatted_date, count_display, trend_display, summary])

    col_widths = [max(len(str(item)) for item in col) for col in zip(*table)]
    
    def format_row(row):
        return "| " + " | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(row)) + " |"

    for i, row in enumerate(table):
        print(format_row(row))
        if i == 0:
            separator = "+-" + "-+-".join("-" * width for width in col_widths) + "-+"
            print(separator)

def main():
    """Main function to run the script."""
    
    parser = argparse.ArgumentParser(description="Spur IP Historical Enrichment & Analysis Script.")
    parser.add_argument("ip_file", nargs='?', help="Optional path to a file containing IP addresses (one per line).")
    parser.add_argument("--search-key", help="Specific JSON key to search for (e.g., tunnels.operator)")
    parser.add_argument("--search-value", help="Value to match for the search key")
    
    args = parser.parse_args()
    
    print("✨ Starting Spur IP Historical Analysis Script...")

    token = get_spur_token()
    ips_to_check = load_ips(args.ip_file)
    print(f"\n🚀 Checking **{len(ips_to_check)}** unique IP address(es): {', '.join(ips_to_check)}")
    
    date_list = get_historical_dates()
    print(f"✅ Will look up dates from **{date_list[0]}** to **{date_list[-1]}**.")
    
    # --- Attribute Search Setup ---
    search_key = args.search_key
    search_value = args.search_value
    
    if not search_key:
        ask_search = input("\nWould you like to search for a specific attribute history? (y/n): ").strip().lower()
        if ask_search.startswith('y'):
            search_key = input("Enter the Key to search (e.g., tunnels.operator): ").strip()
            val_input = input("Enter the Value to match (e.g., PROTON_VPN): ").strip()
            search_value = parse_user_value(val_input)
    else:
        search_value = parse_user_value(search_value)

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
                
                if search_key:
                    analyze_attribute_presence(ip, ip_data_by_date, search_key, search_value)
                
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
