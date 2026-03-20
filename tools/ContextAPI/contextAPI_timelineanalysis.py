#!/usr/bin/env python3
import requests
import os
import argparse
import json
import concurrent.futures
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# --- Configuration ---
CURRENT_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}"
HISTORIC_CONTEXT_URL = "https://api.spur.us/v2/context/{ip}?dt={date}"
OUTPUT_FILENAME = "spur_ip_analysis_timeline.jsonl"
MAX_THREADS = 10 

# --- Helper Functions ---

def flatten_dict(d: Dict[str, Any], parent_key: str = '') -> Dict[str, Any]:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict) and v: 
            items.extend(flatten_dict(v, new_key).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_nested_value(data: Any, key_path: str) -> Any:
    keys = key_path.split('.')
    current = data
    for k in keys:
        if isinstance(current, dict):
            if k in current: current = current[k]
            else: return None
        elif isinstance(current, list):
            next_values = []
            for item in current:
                if isinstance(item, dict) and k in item:
                    val = item[k]
                    if isinstance(val, list): next_values.extend(val)
                    else: next_values.append(val)
            if not next_values: return None
            current = next_values
        else: return None
    return current

def parse_user_value(val: str) -> Any:
    if not val or val.strip() == "": return None
    if val.lower() in ('null', 'none'): return None
    try: return json.loads(val)
    except: return val

def check_match(actual_value: Any, target_value: Any) -> bool:
    if actual_value is None: return False
    if target_value is None: return True # Key-only search
    if isinstance(actual_value, list): return target_value in actual_value
    return str(actual_value) == str(target_value)

def calculate_list_delta(old_list: List[Any], new_list: List[Any]) -> Tuple[List[Any], List[Any]]:
    def make_h(i): return json.dumps(i, sort_keys=True) if isinstance(i, (dict, list)) else i
    old_set = set(make_h(i) for i in old_list)
    new_set = set(make_h(i) for i in new_list)
    added = sorted([json.loads(i) if isinstance(i, str) and i.startswith(('{', '[')) else i for i in new_set - old_set])
    removed = sorted([json.loads(i) if isinstance(i, str) and i.startswith(('{', '[')) else i for i in old_set - new_set])
    return added, removed

def deep_diff_recursive(old_data: Dict[str, Any], new_data: Dict[str, Any], path: str = "") -> Optional[Dict[str, Any]]:
    changes = {'keys_disappeared': {}, 'value_changes': {}}
    old_keys, new_keys = set(old_data.keys()), set(new_data.keys())
    for key in new_keys - old_keys:
        changes['value_changes'][f"{path}{key}"] = {'old_value': None, 'new_value': new_data[key]}
    for key in old_keys - new_keys:
        changes['keys_disappeared'][f"{path}{key}"] = old_data[key]
    for key in old_keys.intersection(new_keys):
        old_val, new_val = old_data.get(key), new_data.get(key)
        curr_path = f"{path}{key}"
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            nested = deep_diff_recursive(old_val, new_val, path=f"{curr_path}.")
            if nested:
                changes['keys_disappeared'].update(nested['keys_disappeared'])
                changes['value_changes'].update(nested['value_changes'])
            continue
        if old_val != new_val:
            changes['value_changes'][curr_path] = {'old_value': old_val, 'new_value': new_val}
    return changes if (changes['keys_disappeared'] or changes['value_changes']) else None

# --- Restored Workflow Functions ---

def get_spur_token():
    token = os.environ.get("TOKEN")
    if not token:
        print("Spur Token not found in environment variable 'TOKEN'.")
        token = input("Please enter your Spur Token: ").strip()
        if not token:
            print("Error: Spur Token is required. Exiting.")
            sys.exit(1)
    return token

def load_ips(ip_file=None):
    raw_text = ""
    if ip_file:
        try:
            with open(ip_file, 'r') as f: raw_text = f.read()
        except FileNotFoundError:
            print(f"Error: Input file '{ip_file}' not found.")
            sys.exit(1)
    else:
        print("\nEnter IP addresses below (paste a list, comma separated, or space separated).")
        print("➡️  **Press ENTER twice (on an empty line) to finish:**")
        lines = []
        while True:
            try:
                line = input()
                if line.strip() == "": break
                lines.append(line)
            except EOFError: break
        raw_text = "\n".join(lines)
    
    tokens = re.split(r'[,\s]+', raw_text)
    unique_ips = list(set(t.strip() for t in tokens if t.strip()))
    if not unique_ips:
        print("Error: No valid IP addresses found. Exiting.")
        sys.exit(1)
    return unique_ips

def get_historical_dates():
    while True:
        prompt = "\nEnter historical look-up span (e.g., '30 days', '20260302-20260327'): "
        span = input(prompt).strip().lower()
        if '-' in span:
            parts = [p.strip() for p in span.split('-')]
            try:
                start = datetime.strptime(parts[0], "%Y%m%d").date()
                end = datetime.strptime(parts[1], "%Y%m%d").date()
                if start > end:
                    print("Error: Start date cannot be after end date.")
                    continue
                dates = []
                while start <= end:
                    dates.append(start.strftime("%Y%m%d")); start += timedelta(days=1)
                return dates
            except ValueError:
                print("Invalid date format. Please use YYYYMMDD-YYYYMMDD.")
                continue
        parts = span.split()
        if len(parts) == 2 and parts[0].isdigit():
            num = int(parts[0])
            unit = parts[1].rstrip('s')
            delta = timedelta(days=num) if unit == 'day' else timedelta(weeks=num)
            end = datetime.now().date(); start = end - delta
            dates = []
            while start <= end:
                dates.append(start.strftime("%Y%m%d")); start += timedelta(days=1)
            return dates
        print("Invalid format. Use 'X days', 'X weeks', or 'YYYYMMDD-YYYYMMDD'.")

# --- UI & Table Rendering ---

def print_timeline_to_terminal(ip, timeline):
    print(f"\n" + "="*105 + f"\n📈 TIMELINE ANALYSIS: {ip}\n" + "="*105)
    table = [["Date", "🔄 Modified (Key)", "➕ Added (New Value)", "➖ Removed (Old Value)"]]
    for event in timeline:
        dt = event['date']
        f_dt = datetime.strptime(dt, "%Y%m%d").strftime("%Y-%m-%d")
        if event['type'] == 'Initial Context':
            base = flatten_dict(event.get('full_context', {}))
            for i, k in enumerate(sorted(base.keys())):
                v = json.dumps(base[k], ensure_ascii=False) if isinstance(base[k], (dict, list)) else str(base[k])
                table.append([f_dt if i == 0 else "", f"(Baseline) {k}", v, ""])
            continue
        rows = []
        ch = event.get('changes', {})
        for k, v in ch.get('value_changes', {}).items():
            old, new = v['old_value'], v['new_value']
            if old is None: rows.append((k, json.dumps(new, ensure_ascii=False), ""))
            elif k.endswith('.count'): rows.append((k, f"{new} ({'⬆️' if new > old else '⬇️'} {new-old:+d})", str(old)))
            else: rows.append((k, json.dumps(new, ensure_ascii=False), json.dumps(old, ensure_ascii=False)))
        for k, v in ch.get('keys_disappeared', {}).items():
            rows.append((k, "", json.dumps(v, ensure_ascii=False)))
        for i, (k, a, r) in enumerate(sorted(rows)):
            table.append([f_dt if i == 0 else "", k, a, r])
            
    widths = [max(len(str(x)) for x in col) for col in zip(*table)]
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    for i, row in enumerate(table):
        if i > 1 and row[0] != "": print(sep)
        print("| " + " | ".join(str(row[j]).ljust(widths[j]) for j in range(4)) + " |")
        if i == 0: print(sep)
    print(sep)

def main():
    parser = argparse.ArgumentParser(); parser.add_argument("ip_file", nargs='?'); args = parser.parse_args()
    print("✨ Starting Spur IP Historical Analysis Script...")
    token = get_spur_token()
    ips = load_ips(args.ip_file)
    dates = get_historical_dates()
    
    search_key, search_value = None, None
    ask_search = input("\nWould you like to search for a specific attribute history? (y/n): ").strip().lower()
    if ask_search.startswith('y'):
        search_key = input("Enter the Key to search (e.g., client.proxies): ").strip()
        search_value = parse_user_value(input("Enter the Value to match (Leave blank for ANY): ").strip())

    print(f"\n🚀 Processing {len(ips)} IPs over {len(dates)} dates...")
    with open(OUTPUT_FILENAME, 'w') as f:
        for ip in ips:
            if not ip.strip(): continue
            # Logic for Parallel Fetching (Restored)
            results, today = {}, datetime.now().strftime("%Y%m%d")
            print(f"\n--- Fetching Data: {ip} ---")
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as exe:
                futures = {exe.submit(requests.get, (CURRENT_CONTEXT_URL.format(ip=ip) if dt == today else HISTORIC_CONTEXT_URL.format(ip=ip, date=dt)), headers={"Token": token}): dt for dt in dates}
                for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                    dt = futures[fut]
                    try:
                        resp = fut.result()
                        if resp.status_code == 200: results[dt] = resp.json()
                    except: pass
                    print(f"\r    ⏳ Progress: [{i}/{len(dates)}] dates fetched...", end="", flush=True)
            print()
            
            if not results:
                print(f"⚠️  No data found for {ip} in the specified timeframe.")
                continue
            
            # Analyze Timeline
            sorted_dates = sorted(results.keys())
            tl = [{'date': sorted_dates[0], 'type': 'Initial Context', 'full_context': results[sorted_dates[0]]}]
            for i in range(1, len(sorted_dates)):
                diff = deep_diff_recursive(results[sorted_dates[i-1]], results[sorted_dates[i]])
                if diff: tl.append({'date': sorted_dates[i], 'type': 'Change', 'changes': diff})
            
            print_timeline_to_terminal(ip, tl)
            if search_key:
                from __main__ import analyze_attribute_presence # Local call
                analyze_attribute_presence(ip, results, search_key, search_value)
            
            for e in tl: f.write(json.dumps({'ip': ip, **e}) + '\n')
    
    print(f"\n✨ Analysis complete. Events exported to {OUTPUT_FILENAME}")

def analyze_attribute_presence(ip, ip_results, search_key, search_value):
    dates = sorted(list(ip_results.keys()))
    intervals, current_start, is_present = [], None, False
    print(f"\n🔎 SEARCH [{ip}]: `{search_key}` | Value: `{search_value if search_value else 'ANY'}`")
    print("-" * 60)
    for dt in dates:
        match = check_match(get_nested_value(ip_results[dt], search_key), search_value)
        if match and not is_present:
            current_start, is_present = dt, True
        elif not match and is_present:
            intervals.append((current_start, dt))
            is_present = False
    if is_present: intervals.append((current_start, "Present"))
    if not intervals: print(f"❌ No matches found.")
    else:
        for s, e in intervals:
            fs = datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
            if e == "Present": print(f"   ✅ PRESENT: {fs}  ➡️  (Latest Data)")
            else:
                fe = (datetime.strptime(e, "%Y%m%d") - timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"   ✅ PRESENT: {fs}  ➡️  {fe}" if fs != fe else f"   ✅ PRESENT: {fs} (1 Day)")

if __name__ == "__main__": main()
