#!/usr/bin/env python3
import json
import sys
import os
import argparse
from datetime import datetime

def load_json_file(filepath):
    """Loads a JSON or JSONL file into a dictionary keyed by IP."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    records = data
            except json.JSONDecodeError:
                f.seek(0)
                for line in f:
                    if line.strip():
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
    except Exception as e:
        print(f"[-] Error reading {filepath}: {e}")
        sys.exit(1)
        
    ip_key = None
    if records:
        for k in records[0].keys():
            if k.lower() in ['ip', 'ip_address', 'ipaddress']:
                ip_key = k
                break
                
    if not ip_key:
        print(f"[-] Could not identify IP column in {filepath}")
        sys.exit(1)

    print(f"  - Using '{ip_key}' as primary key. Loaded {len(records):,} records.")
    return {str(r[ip_key]).strip(): r for r in records if r.get(ip_key)}

def generate_modified_record(ip, record1, record2, current_date, ignore_keys=None):
    """Compares two dictionaries and generates the structured diff record with dynamic date keys."""
    if ignore_keys is None:
        ignore_keys = ['Queried_Date', 'Timestamp', 'timestamp'] 
        
    changes = {}
    unchanged = {}
    
    # Extract the timestamp from the historic record (fallback to a string if missing)
    historic_date = str(record1.get('Timestamp', record1.get('timestamp', 'historic_value')))
    
    all_keys = set(record1.keys()).union(set(record2.keys()))
    
    for key in all_keys:
        if key in ignore_keys: 
            continue
            
        val1 = record1.get(key)
        val2 = record2.get(key)
        
        val1_cmp = json.dumps(val1, sort_keys=True) if isinstance(val1, (dict, list)) else val1
        val2_cmp = json.dumps(val2, sort_keys=True) if isinstance(val2, (dict, list)) else val2
        
        if val1_cmp != val2_cmp:
            # Inject the dynamic dates as the keys
            changes[key] = {historic_date: val1, current_date: val2}
        else:
            unchanged[key] = val1
            
    if not changes:
        return None
        
    return {
        "ip": ip,
        "status": "MODIFIED",
        "changes": changes,
        "unchanged": unchanged
    }

def get_valid_filepath(prompt_text):
    """Prompts the user for a file path and ensures it exists."""
    while True:
        filepath = input(prompt_text).strip()
        filepath = filepath.replace('"', '').replace("'", "")
        
        if os.path.exists(filepath):
            return filepath
        print("[-] Error: File not found. Please try again.")

def main():
    # Capture today's date once for the entire run
    current_date = datetime.now().strftime('%Y%m%d')

    print("\n" + "="*60)
    print(" JSON/JSONL IP Feed Differ ".center(60))
    print("="*60)
    print("How would you like to run the diff?")
    print("  1: Stats Only (Calculate overlap and modification metrics)")
    print("  2: Full Diff  (Export exact IP changes to a new JSON file)")
    print("-" * 60)

    mode_choice = input("Select mode (1 or 2): ").strip()
    while mode_choice not in ['1', '2']:
        mode_choice = input("Invalid choice. Select 1 or 2: ").strip()

    run_mode = 'stats' if mode_choice == '1' else 'full'

    print("\n--- Input Files ---")
    file1_path = get_valid_filepath("Enter path to the Timestamp (Historic) JSON file: ")
    file2_path = get_valid_filepath("Enter path to the Non-timestamp (Current) JSON file: ")

    out_file_path = None
    if run_mode == 'full':
        print("\n--- Output Configuration ---")
        default_out = "deep_diff_export.jsonl"
        out_input = input(f"Enter output file name [Default: {default_out}]: ").strip()
        out_file_path = out_input if out_input else default_out

    print(f"\n[+] Loading {file1_path} into memory...")
    dict1 = load_json_file(file1_path)
    
    print(f"\n[+] Loading {file2_path} into memory...")
    dict2 = load_json_file(file2_path)

    total_1 = len(dict1)
    total_2 = len(dict2)

    ips1 = set(dict1.keys())
    ips2 = set(dict2.keys())

    shared_ips = ips1.intersection(ips2)
    removed_ips = ips1 - ips2
    added_ips = ips2 - ips1

    out_file = None
    if run_mode == 'full':
        print(f"\n[+] Full mode enabled. Writing diffs to {out_file_path}...")
        out_file = open(out_file_path, 'w', encoding='utf-8')

    changed_ips_count = 0
    unchanged_ips_count = 0
    attribute_change_counts = {}

    print("[+] Calculating overlaps and differences...")

    # --- Process Shared / Modified IPs ---
    for ip in shared_ips:
        diff_record = generate_modified_record(ip, dict1[ip], dict2[ip], current_date)
        
        if diff_record:
            changed_ips_count += 1
            for changed_key in diff_record['changes'].keys():
                attribute_change_counts[changed_key] = attribute_change_counts.get(changed_key, 0) + 1
                
            if out_file:
                out_file.write(json.dumps(diff_record) + '\n')
        else:
            unchanged_ips_count += 1

    # --- Process Removed IPs ---
    if out_file:
        for ip in removed_ips:
            # Data comes straight from the historic file, so it already has a timestamp
            removed_record = {"ip": ip, "status": "REMOVED", "data": dict1[ip]}
            out_file.write(json.dumps(removed_record) + '\n')

    # --- Process Added IPs ---
    if out_file:
        for ip in added_ips:
            # Create a new dictionary starting with Timestamp to force it to be the first key,
            # then unpack the rest of the data from the current file behind it.
            added_data = {"Timestamp": current_date, **dict2[ip]}
            
            added_record = {"ip": ip, "status": "ADDED", "data": added_data}
            out_file.write(json.dumps(added_record) + '\n')

    if out_file:
        out_file.close()

    # --- Print Stats ---
    pct_unchanged = (unchanged_ips_count / total_1 * 100) if total_1 else 0
    pct_modified  = (changed_ips_count / total_1 * 100) if total_1 else 0
    pct_removed   = (len(removed_ips) / total_1 * 100) if total_1 else 0
    pct_added     = (len(added_ips) / total_2 * 100) if total_2 else 0

    print("\n" + "="*65)
    print(" IP-BY-IP DELTA STATISTICS ".center(65, "="))
    print("="*65)
    print(f"Total IPs File 1: {total_1:,}")
    print(f"Total IPs File 2: {total_2:,}")
    print("-" * 65)
    print(f"IPs Unchanged:   {unchanged_ips_count:>12,} ({pct_unchanged:>5.1f}% of File 1)")
    print(f"IPs Modified:    {changed_ips_count:>12,} ({pct_modified:>5.1f}% of File 1)")
    print(f"IPs Removed:     {len(removed_ips):>12,} ({pct_removed:>5.1f}% of File 1)")
    print(f"IPs Added:       {len(added_ips):>12,} ({pct_added:>5.1f}% of File 2)")

    print("\n" + "="*65)
    print(" ATTRIBUTE MODIFICATIONS ".center(65, "="))
    print("="*65)

    if changed_ips_count == 0:
        print("No attributes were modified between the shared IPs.")
    else:
        sorted_attr_changes = sorted(attribute_change_counts.items(), key=lambda item: item[1], reverse=True)
        for attr, count in sorted_attr_changes:
            print(f"  - '{attr}': Shifted on {count:,} IPs ({(count/changed_ips_count)*100:.1f}% of modified IPs)")
            
    if run_mode == 'full':
        print("\n" + "-" * 65)
        print(f"[!] Full Diff Export saved to: {out_file_path}")

    print("\n" + "="*65 + "\n")

if __name__ == "__main__":
    main()