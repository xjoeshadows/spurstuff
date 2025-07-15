#!/usr/bin/env python3
import requests
import gzip
import json
import io
import os
import sys
import datetime # Import datetime for date formatting
import subprocess # Import subprocess for wc -l command

def download_file(url, token):
    """
    Downloads a file from the specified URL.

    Args:
        url (str): The URL of the file to download.
        token (str): The API authentication token.

    Returns:
        bytes: The downloaded file content as bytes, or None on error.
    """
    headers = {'Token': token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.content  # Return the content as bytes
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {url}: {e}", file=sys.stderr)
        return None

def decompress_gzip(data):
    """
    Decompresses gzip data.

    Args:
        data (bytes): The gzip compressed data as bytes.

    Returns:
        bytes: The decompressed data as bytes, or None on error.
    """
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as f:
            return f.read()
    except Exception as e:
        print(f"Error decompressing gzip data: {e}", file=sys.stderr)
        return None

def extract_tag_values(data):
    """
    Extracts the values from the "tag" field in the JSON data, handling multiple JSON objects.

    Args:
        data (bytes): The JSON data as bytes.

    Returns:
        list: A list of tag values, or None on error.
    """
    tag_values = []
    try:
        # Decode the byte stream
        json_string = data.decode('utf-8')

        # Split the string into individual JSON objects.
        # This handles cases where the gzipped file contains JSON Lines (one JSON object per line)
        json_objects = json_string.strip().split('\n')

        for obj_str in json_objects:
            try:
                # Load each JSON object separately
                json_data = json.loads(obj_str)
                # Assuming 'tag' is the field containing the service metric.
                # The Service Metrics feed itself is often just a list of strings.
                # Let's adjust this to handle both cases: a dict with 'tag' or a direct string value.
                if isinstance(json_data, dict) and 'tag' in json_data:
                    tag_values.append(json_data['tag'])
                elif isinstance(json_data, str): # If the line is just a string (common for ServiceMetrics)
                    tag_values.append(json_data)
                else:
                    print(f"Warning: Skipping invalid JSON object line or non-tag data: {obj_str[:80]}...", file=sys.stderr)
            except json.JSONDecodeError:
                # If a line is not a valid JSON object, treat it as a raw string if it's not empty
                if obj_str:
                    tag_values.append(obj_str)
                # print(f"Warning: Skipping invalid JSON object line: {obj_str[:80]}...", file=sys.stderr)
                pass # Skip this line and try the next one

        return tag_values

    except Exception as e:
        print(f"Error processing data: {e}", file=sys.stderr)
        return None

def write_tags_to_file(tags, filename):
    """
    Writes the list of tags to a file, one tag per line.

    Args:
        tags (list): The list of tags to write.
        filename (str): The name of the file to write to.
    """
    try:
        # Ensure utf-8 encoding for tags
        with open(filename, 'w', encoding='utf-8') as f:
            for tag in tags:
                f.write(tag + '\n')
        print(f"Tags successfully written to {filename}")
        return True
    except Exception as e:
        print(f"Error writing tags to file '{filename}': {e}", file=sys.stderr)
        return False

def get_line_count(filename):
    """
    Runs 'wc -l' on the specified file and returns the line count.
    """
    try:
        # Execute 'wc -l' command
        result = subprocess.run(['wc', '-l', filename], capture_output=True, text=True, check=True)
        # The output is typically "   COUNT filename", so split and get the count
        line_count = int(result.stdout.strip().split(' ')[0])
        print(f"Line count for {filename}: {line_count}")
        return line_count
    except FileNotFoundError:
        print(f"Error: 'wc' command not found. Please ensure it's in your PATH.", file=sys.stderr)
        return None
    except subprocess.CalledProcessError as e:
        print(f"Error running 'wc -l' on {filename}: {e.stderr}", file=sys.stderr)
        return None
    except ValueError:
        print(f"Error: Could not parse line count from 'wc -l' output for {filename}.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while getting line count: {e}", file=sys.stderr)
        return None

def main():
    """
    Main function to download, decompress, extract tag values, and count lines.
    """
    # Use TOKEN from environment variable
    token = os.environ.get('TOKEN')
    if not token:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    file_url = 'https://feeds.spur.us/v2/service-metrics/latest.json.gz'
    
    # Generate output filename in YYYYMMDD-ServiceMetricsList.txt format
    current_date_ymd = datetime.datetime.now().strftime("%Y%m%d") # Changed %y to %Y for 4-digit year
    output_list_filename = f"{current_date_ymd}-ServiceMetricsList.txt"
    # New filename for the full decompressed JSON data
    output_full_json_filename = f"{current_date_ymd}ServiceMetricsAll.json"

    print(f"Downloading {file_url}...")
    file_content = download_file(file_url, token)
    if file_content:
        print("File downloaded successfully. Decompressing...")
        decompressed_data = decompress_gzip(file_content)
        if decompressed_data:
            # Save the full decompressed data to the new .json file
            print(f"Saving full decompressed data to {output_full_json_filename}...")
            try:
                with open(output_full_json_filename, 'wb') as f:
                    f.write(decompressed_data)
                print(f"Full decompressed data successfully written to {output_full_json_filename}")
            except Exception as e:
                print(f"Error writing full decompressed data to file '{output_full_json_filename}': {e}", file=sys.stderr)
                # Don't exit, continue with tag extraction if full save fails
            
            print("Data decompressed. Extracting tag values...")
            tag_values = extract_tag_values(decompressed_data)
            if tag_values:
                print(f"Tag values extracted. Writing to {output_list_filename}...")
                if write_tags_to_file(tag_values, output_list_filename):
                    print("Tags written successfully. Counting lines...")
                    get_line_count(output_list_filename) # Run wc -l and print line count
                else:
                    sys.exit(1) # Exit if writing fails
            else:
                print("Failed to extract tag values.", file=sys.stderr)
                sys.exit(1)
        else:
            print("Failed to decompress the data.", file=sys.stderr)
            sys.exit(1)
    else:
        print("Failed to download the file.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
