#!/usr/bin/env python3
import json
import sys
import os
from datetime import datetime
from collections import defaultdict

# =====================================================================
# DATA LOADING FUNCTIONS
# =====================================================================

def load_json_file(filepath):
    """Loads a JSON or JSONL file into a dictionary keyed by IP, collapsing duplicates to the latest occurrence."""
    records = []
    failed_lines = 0
    
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
                            failed_lines += 1
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

    print(f"  - Using '{ip_key}' as primary key. Read {len(records) + failed_lines:,} total lines.")

    final_dict = {}
    duplicates_identical = 0
    duplicates_conflicting = 0
    
    for r in records:
        ip_val = r.get(ip_key)
        if not ip_val:
            continue
            
        ip_str = str(ip_val).strip()
        if ip_str in final_dict:
            if json.dumps(final_dict[ip_str], sort_keys=True) == json.dumps(r, sort_keys=True):
                duplicates_identical += 1
            else:
                duplicates_conflicting += 1
                
        final_dict[ip_str] = r

    if duplicates_conflicting > 0:
        print(f"    [!] Collapsed {duplicates_conflicting:,} conflicting duplicate IPs (kept the latest occurrence).")
        
    print(f"  - Successfully loaded {len(final_dict):,} distinct IPs into memory.")
    return final_dict

def load_cumulative_json_file(filepath):
    """Loads a JSON or JSONL file into a dictionary tracking all historical states per IP."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
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

    print(f"  - Read {len(records):,} total historical logs.")

    # Dictionary mapping IP -> { Timestamp: Record }
    cumulative_dict = defaultdict(dict)
    
    for r in records:
        ip_val = r.get(ip_key)
        if not ip_val:
            continue
            
        ip_str = str(ip_val).strip()
        timestamp = str(r.get('Timestamp', r.get('timestamp', 'unknown_date')))
        
        cumulative_dict[ip_str][timestamp] = r

    print(f"  - Extracted {len(cumulative_dict):,} global distinct IPs with timelines.")
    return cumulative_dict

# =====================================================================
# A-TO-B DIFF GENERATOR
# =====================================================================

def generate_modified_record(ip, record1, record2, current_date, ignore_keys=None):
    if ignore_keys is None:
        ignore_keys = ['Queried_Date', 'Timestamp', 'timestamp'] 
        
    changes = {}
    unchanged = {}
    historic_richer = False
    current_richer = False
    
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
            changes[key] = {historic_date: val1, current_date: val2}
            
            v1_has_data = bool(val1 and (not isinstance(val1, list) or len(val1) > 0))
            v2_has_data = bool(val2 and (not isinstance(val2, list) or len(val2) > 0))
            
            if v1_has_data and not v2_has_data:
                historic_richer = True
            if v2_has_data and not v1_has_data:
                current_richer = True
                
        else:
            unchanged[key] = val1
            
    if not changes:
        return None
        
    return {
        "ip": ip,
        "status": "MODIFIED",
        "changes": changes,
        "unchanged": unchanged,
        "historic_richer": historic_richer,
        "current_richer": current_richer
    }

# =====================================================================
# FORENSIC TIMELINE GENERATOR
# =====================================================================

def generate_timeline_record(ip, hist_timeline, curr_record, current_date, ignore_keys=None):
    if ignore_keys is None:
        ignore_keys = ['Queried_Date', 'Timestamp', 'timestamp']
        
    all_keys = set()
    for rec in hist_timeline.values():
        all_keys.update(rec.keys())
    if curr_record:
        all_keys.update(curr_record.keys())
        
    all_keys = all_keys - set(ignore_keys)
    
    changes = {}
    unchanged = {}
    transient_keys = set()
    escalating_keys = set()
    multi_stage = False
    
    for key in all_keys:
        hist_vals = {}
        hist_rich = False
        val_variations = set()
        
        for h_date, h_rec in sorted(hist_timeline.items()):
            v = h_rec.get(key)
            hist_vals[h_date] = v
            val_variations.add(json.dumps(v, sort_keys=True))
            
            if v and (not isinstance(v, list) or len(v) > 0):
                hist_rich = True
                
        c_val = curr_record.get(key) if curr_record else None
        c_rich = bool(c_val and (not isinstance(c_val, list) or len(c_val) > 0))
        val_variations.add(json.dumps(c_val, sort_keys=True))
        
        if len(val_variations) > 1:
            changes[key] = {
                "timeline": hist_vals,
                f"current_{current_date}": c_val
            }
            
            if hist_rich and not c_rich:
                transient_keys.add(key)
            if c_rich and not hist_rich:
                escalating_keys.add(key)
                
            hist_only_variations = set(json.dumps(v, sort_keys=True) for v in hist_vals.values())
            if len(hist_only_variations) > 1:
                multi_stage = True
        else:
            unchanged[key] = c_val if curr_record else (list(hist_vals.values())[0] if hist_vals else None)
            
    return changes, unchanged, transient_keys, multi_stage, escalating_keys

# =====================================================================
# MAIN EXECUTION
# =====================================================================

def get_valid_filepath(prompt_text):
    while True:
        filepath = input(prompt_text).strip()
        filepath = filepath.replace('"', '').replace("'", "")
        if os.path.exists(filepath):
            return filepath
        print("[-] Error: File not found. Please try again.")

def main():
    print("\n" + "="*70)
    print(" JSON/JSONL IP Feed Differ ".center(70))
    print("="*70)
    print("How would you like to run the diff?")
    print("  1: Strict A-to-B Diff      (Latest Historic State vs Current)")
    print("  2: Threat Hunter Timeline  (Full Cumulative History vs Current)")
    print("-" * 70)

    mode_choice = input("Select mode (1 or 2): ").strip()
    while mode_choice not in ['1', '2']:
        mode_choice = input("Invalid choice. Select 1 or 2: ").strip()

    run_mode = 'a_to_b' if mode_choice == '1' else 'timeline'

    print("\n--- Input Files ---")
    file1_path = get_valid_filepath("Enter path to the Timestamp'd (Historic) JSON file: ")
    file2_path = get_valid_filepath("Enter path to the Non-timestamp'd (Current) JSON file: ")

    file2_mtime = os.path.getmtime(file2_path)
    auto_date = datetime.fromtimestamp(file2_mtime).strftime('%Y%m%d')
    
    print(f"\n  * Auto-detected date from file metadata: {auto_date}")
    user_date = input("  * Press Enter to keep this date, or type a custom date (YYYYMMDD): ").strip()
    current_date = user_date if user_date else auto_date

    print("\n[+] Loading Current File...")
    dict2 = load_json_file(file2_path)
    
    records_to_export = []

    # -----------------------------------------------------------------
    # BRANCH 2: TIMELINE MODE
    # -----------------------------------------------------------------
    if run_mode == 'timeline':
        print("\n[+] Loading Historic File (Cumulative Timeline Mode)...")
        dict1_cumulative = load_cumulative_json_file(file1_path)
        
        hist_ips = set(dict1_cumulative.keys())
        curr_ips = set(dict2.keys())
        
        global_pool = hist_ips.union(curr_ips)
        
        continuous_ips = hist_ips.intersection(curr_ips)
        aged_out_ips = hist_ips - curr_ips
        emerging_ips = curr_ips - hist_ips
        
        stats_transient_ips = 0
        stats_multi_stage_ips = 0
        stats_escalating_ips = 0
        
        aged_out_attributes = defaultdict(int)
        new_current_attributes = defaultdict(int)

        print(f"\n[+] Processing {len(global_pool):,} global IPs into timelines...")
        
        # 1. Process Continuous
        for ip in continuous_ips:
            hist_timeline = dict1_cumulative[ip]
            curr_record = dict2[ip]
            
            changes, unchanged, transient_keys, is_multi, escalating_keys = generate_timeline_record(
                ip, hist_timeline, curr_record, current_date
            )
            
            if transient_keys:
                stats_transient_ips += 1
                for k in transient_keys:
                    aged_out_attributes[k] += 1
            if is_multi:
                stats_multi_stage_ips += 1
            if escalating_keys:
                stats_escalating_ips += 1
                for k in escalating_keys:
                    new_current_attributes[k] += 1
                
            records_to_export.append({
                "ip": ip,
                "status": "CONTINUOUS",
                "changes": changes,
                "unchanged": unchanged
            })
            
        # 2. Process Aged Out
        for ip in aged_out_ips:
            records_to_export.append({
                "ip": ip,
                "status": "AGED_OUT",
                "historical_timeline": dict1_cumulative[ip]
            })
            
        # 3. Process Emerging
        for ip in emerging_ips:
            curr_record = {"Timestamp": current_date, **dict2[ip]}
            records_to_export.append({
                "ip": ip,
                "status": "EMERGING",
                "data": curr_record
            })
            
        # --- Timeline Terminal Output ---
        total_global = len(global_pool)
        pct_cont = (len(continuous_ips) / total_global * 100) if total_global else 0
        pct_aged = (len(aged_out_ips) / total_global * 100) if total_global else 0
        pct_emerg = (len(emerging_ips) / total_global * 100) if total_global else 0
        
        total_cont = len(continuous_ips)
        pct_transient = (stats_transient_ips / total_cont * 100) if total_cont else 0
        pct_multi = (stats_multi_stage_ips / total_cont * 100) if total_cont else 0
        pct_esc = (stats_escalating_ips / total_cont * 100) if total_cont else 0

        print("\n" + "="*70)
        print(" THREAT HUNTER TIMELINE STATISTICS ".center(70))
        print("="*70)
        print(" DATASET OVERVIEW (Cumulative History):")
        print(f"  - Total Distinct IPs in Global Pool:       {total_global:,}")
        print("-" * 70)
        print("FORENSIC IP STATUS:")
        print(f"  Continuous (Active historically & today):  {len(continuous_ips):>9,} ({pct_cont:>5.1f}% of Global)")
        print(f"  Aged Out   (Vanished from Current):        {len(aged_out_ips):>9,} ({pct_aged:>5.1f}% of Global)")
        print(f"  Emerging   (Net-new in Current):           {len(emerging_ips):>9,} ({pct_emerg:>5.1f}% of Global)")

        print("\n" + "="*70)
        print(" TRANSIENT INTELLIGENCE (CONTINUOUS IPs) ".center(70))
        print("="*70)
        print(f"(Evaluating the {len(continuous_ips):,} shared IPs for historical breadcrumbs)\n")
        print(f"  [!] IPs with Transient Historical Context: {stats_transient_ips:>9,} ({pct_transient:>5.1f}%)")
        print("      > Historic had rich data on at least one past date,")
        print("        but the Current feed is now null/empty.\n")
        print(f"  [!] IPs with Multi-Stage Evolution:        {stats_multi_stage_ips:>9,} ({pct_multi:>5.1f}%)")
        print("      > IP changed states or context multiple times across")
        print("        the historical timeline before reaching its current state.\n")
        print(f"  [!] IPs with Escalating Current Risks:     {stats_escalating_ips:>9,} ({pct_esc:>5.1f}%)")
        print("      > Current file contains net-new context that was NEVER")
        print("        seen on any date in the historical timeline.")

        print("\n" + "="*70)
        print(" TOP ATTRIBUTES WITH AGED-OUT CONTEXT ".center(70))
        print("="*70)
        print("(Fields where the historical timeline had data, but Current is null/empty)")
        if not aged_out_attributes:
            print("  - No transient attributes detected.")
        else:
            sorted_aged_out = sorted(aged_out_attributes.items(), key=lambda item: item[1], reverse=True)
            for attr, count in sorted_aged_out:
                print(f"  - '{attr}': Dropped off on {count:,} IPs")
                
        print("\n" + "="*70)
        print(" TOP ATTRIBUTES WITH NEW CURRENT CONTEXT ".center(70))
        print("="*70)
        print("(Fields where the Current file has data, but the historical timeline was null/empty)")
        if not new_current_attributes:
            print("  - No new current attributes detected.")
        else:
            sorted_new_current = sorted(new_current_attributes.items(), key=lambda item: item[1], reverse=True)
            for attr, count in sorted_new_current:
                print(f"  - '{attr}': Surfaced on {count:,} IPs")

    # -----------------------------------------------------------------
    # BRANCH 1: A-TO-B MODE
    # -----------------------------------------------------------------
    else:
        print(f"\n[+] Loading Historic File (Deduplicating Latest State)...")
        dict1 = load_json_file(file1_path)

        total_1 = len(dict1)
        total_2 = len(dict2)

        ips1 = set(dict1.keys())
        ips2 = set(dict2.keys())

        shared_ips = ips1.intersection(ips2)
        removed_ips = ips1 - ips2
        added_ips = ips2 - ips1

        changed_ips_count = 0
        unchanged_ips_count = 0
        historic_richer_count = 0
        current_richer_count = 0
        
        attribute_change_counts = {}
        attr_historic_richer_counts = {}
        attr_current_richer_counts = {}

        print("[+] Calculating overlaps and differences...")

        for ip in shared_ips:
            diff_record = generate_modified_record(ip, dict1[ip], dict2[ip], current_date)
            
            if diff_record:
                changed_ips_count += 1
                if diff_record.get('historic_richer'):
                    historic_richer_count += 1
                if diff_record.get('current_richer'):
                    current_richer_count += 1
                    
                for changed_key in diff_record['changes'].keys():
                    attribute_change_counts[changed_key] = attribute_change_counts.get(changed_key, 0) + 1
                    val1 = dict1[ip].get(changed_key)
                    val2 = dict2[ip].get(changed_key)
                    
                    v1_has_data = bool(val1 and (not isinstance(val1, list) or len(val1) > 0))
                    v2_has_data = bool(val2 and (not isinstance(val2, list) or len(val2) > 0))
                    
                    if v1_has_data and not v2_has_data:
                        attr_historic_richer_counts[changed_key] = attr_historic_richer_counts.get(changed_key, 0) + 1
                    if v2_has_data and not v1_has_data:
                        attr_current_richer_counts[changed_key] = attr_current_richer_counts.get(changed_key, 0) + 1
                    
                diff_export = dict(diff_record)
                diff_export.pop('historic_richer', None)
                diff_export.pop('current_richer', None)
                records_to_export.append(diff_export)
            else:
                unchanged_ips_count += 1

        for ip in removed_ips:
            records_to_export.append({"ip": ip, "status": "REMOVED", "data": dict1[ip]})
        for ip in added_ips:
            added_data = {"Timestamp": current_date, **dict2[ip]}
            records_to_export.append({"ip": ip, "status": "ADDED", "data": added_data})

        # --- A-to-B Terminal Output ---
        pct_unchanged_1   = (unchanged_ips_count / total_1 * 100) if total_1 else 0
        pct_modified_1    = (changed_ips_count / total_1 * 100) if total_1 else 0
        pct_removed_1     = (len(removed_ips) / total_1 * 100) if total_1 else 0
        pct_curr_richer_1 = (current_richer_count / total_1 * 100) if total_1 else 0

        pct_unchanged_2   = (unchanged_ips_count / total_2 * 100) if total_2 else 0
        pct_modified_2    = (changed_ips_count / total_2 * 100) if total_2 else 0
        pct_added_2       = (len(added_ips) / total_2 * 100) if total_2 else 0
        pct_hist_richer_2 = (historic_richer_count / total_2 * 100) if total_2 else 0

        print("\n" + "="*70)
        print(" DATASET OVERVIEW (Post-Deduplication) ".center(70, "="))
        print("="*70)
        print(f"Total Distinct IPs in Historic File:  {total_1:>9,}")
        print(f"Total Distinct IPs in Current File:   {total_2:>9,}")

        print("\n" + "="*70)
        print(f" {current_date} Enrichment vs Historic Enrichment ".center(70, "="))
        print("="*70)
        print(f"IPs Unchanged (Carried over exactly):               {unchanged_ips_count:>9,} ({pct_unchanged_1:>5.1f}% of Historic)")
        print(f"IPs Modified  (Overall IP changes):                 {changed_ips_count:>9,} ({pct_modified_1:>5.1f}% of Historic)")
        print(f"  > IPs where Current had data & Historic was null: {current_richer_count:>9,} ({pct_curr_richer_1:>5.1f}% of Historic)")
        print(f"IPs Removed   (Missing from Current File):          {len(removed_ips):>9,} ({pct_removed_1:>5.1f}% of Historic)")

        print("\n" + "="*70)
        print(f" Historic Enrichment vs {current_date} Enrichment ".center(70, "="))
        print("="*70)
        print(f"IPs Unchanged (Carried over exactly):               {unchanged_ips_count:>9,} ({pct_unchanged_2:>5.1f}% of Current)")
        print(f"IPs Modified  (Overall IP changes):                 {changed_ips_count:>9,} ({pct_modified_2:>5.1f}% of Current)")
        print(f"  > IPs where Historic had data & Current was null: {historic_richer_count:>9,} ({pct_hist_richer_2:>5.1f}% of Current)")
        print(f"IPs Added     (New in Current File):                {len(added_ips):>9,} ({pct_added_2:>5.1f}% of Current)")

        print("\n" + "="*70)
        print(" OVERALL ATTRIBUTE MODIFICATIONS ".center(70, "="))
        print("="*70)

        if changed_ips_count == 0:
            print("No attributes were modified between the shared IPs.")
        else:
            sorted_attr_changes = sorted(attribute_change_counts.items(), key=lambda item: item[1], reverse=True)
            for attr, count in sorted_attr_changes:
                print(f"  - '{attr}': Modified on {count:,} IPs ({(count/changed_ips_count)*100:.1f}% of all modified IPs)")

        if attr_current_richer_counts:
            print("\n" + "="*70)
            print(f" ATTRIBUTE GAINS ({current_date} Current > Historic) ".center(70, "="))
            print("="*70)
            print(f"(Fields where the {current_date} file had data, but Historic was null/empty)")
            sorted_curr_richer = sorted(attr_current_richer_counts.items(), key=lambda item: item[1], reverse=True)
            for attr, count in sorted_curr_richer:
                print(f"  - '{attr}': {current_date} added new data on {count:,} IPs")

        if attr_historic_richer_counts:
            print("\n" + "="*70)
            print(f" ATTRIBUTE RETENTIONS (Historic > {current_date} Current) ".center(70, "="))
            print("="*70)
            print(f"(Fields where Historic had data, but the {current_date} file was null/empty)")
            sorted_hist_richer = sorted(attr_historic_richer_counts.items(), key=lambda item: item[1], reverse=True)
            for attr, count in sorted_hist_richer:
                print(f"  - '{attr}': Historic retained data on {count:,} IPs")

    # -----------------------------------------------------------------
    # JSON EXPORT PROMPT (For both branches)
    # -----------------------------------------------------------------
    print("\n" + "-" * 70)
    export_choice = input("Would you like to export the full details to a JSON file? (y/n): ").strip().lower()
    
    if export_choice == 'y':
        default_out = "threat_hunter_timeline.jsonl" if run_mode == 'timeline' else "deep_diff_export.jsonl"
        out_input = input(f"Enter output file name [Default: {default_out}]: ").strip()
        out_file_path = out_input if out_input else default_out
        
        with open(out_file_path, 'w', encoding='utf-8') as f:
            for record in records_to_export:
                f.write(json.dumps(record) + '\n')
        print(f"\n[+] Successfully saved export to: {out_file_path}")
    
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
