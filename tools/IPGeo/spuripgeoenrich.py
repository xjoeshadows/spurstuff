#!/usr/bin/env python3
import argparse
import json
import sys
from ipwhois import IPWhois

def get_ips_from_input(input_source):
    """
    Parses a string of IPs separated by commas or newlines.
    
    Args:
        input_source (str): A string containing IP addresses.
        
    Returns:
        list: A list of cleaned IP address strings.
    """
    print("Parsing provided input to extract IP addresses...")
    if not input_source:
        print("No input source provided.")
        return []
    
    # Replace newlines with commas and split by comma
    ips = input_source.replace('\n', ',').split(',')
    
    # Clean up whitespace and filter out empty strings
    cleaned_ips = [ip.strip() for ip in ips if ip.strip()]
    print(f"Found {len(cleaned_ips)} IP(s) to process.")
    return cleaned_ips

def lookup_cidrs(ips):
    """
    Performs RDAP lookups for a list of IPs and returns CIDR data.

    Args:
        ips (list): A list of IP address strings.

    Returns:
        dict: A dictionary where each key is an IP and the value is a list of
              CIDRs, or an error message.
    """
    print("\nStarting RDAP lookups for each IP...")
    results = {}
    
    for i, ip in enumerate(ips):
        print(f"[{i+1}/{len(ips)}] Looking up RDAP record for IP: {ip}")
        try:
            # Create an IPWhois object to perform the lookup
            obj = IPWhois(ip)
            
            # Perform the RDAP lookup. The result should be a dictionary.
            rdap_data = obj.lookup_rdap(depth=1)
            
            # 💡 Step 1: Check the data type of the RDAP response
            if not isinstance(rdap_data, dict):
                error_message = f"Error: RDAP response for {ip} was not a dictionary. It returned: {rdap_data}"
                print(f"⚠️ {error_message}")
                results[ip] = [error_message]
                continue

            # 💡 Step 2: Extract CIDR data from the network object
            network_data = rdap_data.get('network', {})
            
            # 💡 Step 3: Check the data type of the network data
            if not isinstance(network_data, dict):
                error_message = f"Error: Network data for {ip} was not a dictionary. It returned: {network_data}"
                print(f"⚠️ {error_message}")
                results[ip] = [error_message]
                continue

            cidrs_list = network_data.get('cidr', [])
            
            # 💡 Step 4: Validate and parse the CIDR list
            if not isinstance(cidrs_list, list) or not cidrs_list:
                # Fallback to the 'asn_cidr' field if 'cidr' list is not present or invalid
                print(f"   No 'cidr' list found in network data for {ip}. Attempting fallback to 'asn_cidr'.")
                cidr = rdap_data.get('asn_cidr')
                if cidr:
                    cidrs = [cidr]
                    print(f"   ✅ Found CIDR from fallback: {cidrs}")
                else:
                    cidrs = ["No CIDR found in RDAP record"]
                    print(f"   ❌ No CIDR found for {ip} in either location.")
            else:
                # Extract the 'value' from each dictionary in the 'cidr' list
                cidrs = [c.get('value') for c in cidrs_list if isinstance(c, dict) and 'value' in c]
                print(f"   ✅ Found CIDR(s) in RDAP record: {cidrs}")

            results[ip] = cidrs
            
        except Exception as e:
            error_message = f"Error during lookup for {ip}: {e}"
            print(f"   ❌ {error_message}")
            results[ip] = [error_message]
            
    return results

def enrich_and_save_data(cidr_results, ipgeo_file_path, output_filename):
    """
    Enriches CIDR data with information from the IPGeo file and saves it.
    
    Args:
        cidr_results (dict): The results from the RDAP lookup.
        ipgeo_file_path (str): The path to the IPGeo JSONL file.
        output_filename (str): The name of the file to save to.
    """
    print("\nStarting data enrichment and saving process...")
    
    try:
        # Open both files outside the main loop for efficiency
        with open(ipgeo_file_path, 'r', encoding='utf-8') as ipgeo_file, \
             open(output_filename, 'w', encoding='utf-8') as output_file:
            
            # Create a set of CIDRs to look up for faster matching
            cidrs_to_find = set()
            for ip, cidrs in cidr_results.items():
                for cidr in cidrs:
                    if cidr and "Error" not in cidr:
                        cidrs_to_find.add(cidr)
            
            print(f"Looking for {len(cidrs_to_find)} unique CIDR(s) in the IPGeo file.")
            
            # Process the IPGeo file line-by-line without loading it all at once
            found_records = 0
            for line_number, line in enumerate(ipgeo_file, 1):
                try:
                    record = json.loads(line)
                    # Use the 'prefix' key to find the CIDR
                    ipgeo_cidr = record.get('prefix')
                    
                    if ipgeo_cidr and ipgeo_cidr in cidrs_to_find:
                        # Found a match!
                        found_records += 1
                        # Find the corresponding IP from the initial RDAP results
                        for ip, cidrs in cidr_results.items():
                            if ipgeo_cidr in cidrs:
                                # Create an enriched entry and write it to the output file
                                enriched_entry = {
                                    "ip": ip,
                                    "cidrs": [{
                                        "cidr": ipgeo_cidr,
                                        "ipgeo_data": record
                                    }]
                                }
                                # Write to file with ensure_ascii=False to preserve Unicode characters
                                output_file.write(json.dumps(enriched_entry, ensure_ascii=False) + '\n')
                                break
                except json.JSONDecodeError:
                    print(f"   ⚠️ Skipping invalid JSON on line {line_number} in '{ipgeo_file_path}'.")
            
            if found_records > 0:
                print(f"\n✅ Successfully saved {found_records} enriched records to '{output_filename}'.")
            else:
                print(f"\nNo matching records found in the IPGeo file for the CIDRs from the RDAP lookups.")

    except FileNotFoundError:
        print(f"❌ Error: The file '{ipgeo_file_path}' was not found. Cannot enrich data.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        sys.exit(1)

def main():
    """
    Main function to orchestrate the script's execution.
    """
    print("--- RDAP & IPGeo Enrichment Script ---")
    parser = argparse.ArgumentParser(description="Perform RDAP lookups and enrich data with IPGeo information.")
    parser.add_argument("ips_arg", nargs="*", help="List of IP addresses as command-line arguments.")
    parser.add_argument("-f", "--file", type=str, help="Path to a file containing IP addresses (comma or newline separated).")
    
    args = parser.parse_args()
    
    all_ips = []
    
    # 📝 Step 1: Get IP addresses from arguments, file, or user input
    if args.ips_arg:
        print("Processing IP addresses from command-line arguments.")
        all_ips.extend(get_ips_from_input(" ".join(args.ips_arg)))
        
    if args.file:
        print(f"Processing IP addresses from file: {args.file}")
        try:
            with open(args.file, 'r') as f:
                file_content = f.read()
                all_ips.extend(get_ips_from_input(file_content))
        except FileNotFoundError:
            print(f"Error: The file '{args.file}' was not found.")
            sys.exit(1)
            
    if not all_ips:
        print("No IPs provided via arguments or file.")
        user_input = input("Please enter a list of IPs (comma or space separated): ")
        all_ips.extend(get_ips_from_input(user_input))
        
    all_ips = sorted(list(set(all_ips)))
    
    if not all_ips:
        print("No valid IP addresses were provided after all input methods. Exiting.")
        sys.exit(1)
    
    print(f"\nFinal list of unique IPs to process: {all_ips}")
    
    # 📝 Step 2: Perform RDAP lookups
    cidr_results = lookup_cidrs(all_ips)
    
    print("\n--- RDAP Lookup Complete ---")
    
    # 📝 Step 3: Get IPGeo file path and output filename from the user
    ipgeo_file_path = input("\nPlease enter the path to your IPGeo .jsonl file: ")
    output_filename = input(f"Please enter the output filename (default: IPGeoEnrichment.jsonl): ")
    if not output_filename:
        output_filename = "IPGeoEnrichment.jsonl"
    
    # 📝 Step 4: Enrich and save the data without preloading
    enrich_and_save_data(cidr_results, ipgeo_file_path, output_filename)
    
    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()
