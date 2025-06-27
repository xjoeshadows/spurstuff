#!/usr/bin/env python3
import requests
import os
import sys
import datetime
import gzip
import json
import io

# --- Functions from dailyfeedcounts_script (original) ---
def fetch_data(endpoint, token):
    """
    Fetches data from the specified API endpoint.

    Args:
        endpoint (str): The API endpoint URL.
        token (str): The API authentication token.

    Returns:
        dict: The JSON response from the API. Returns None on error.
    """
    headers = {'Token': token}
    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}", file=sys.stderr)
        return None
    except ValueError as e:
        print(f"Error decoding JSON from {endpoint}: {e}", file=sys.stderr)
        return None

def calculate_line_count_difference(data1, data2):
    """
    Calculates the difference between the 'line_count' values
    extracted from the two data dictionaries (anonres - anonymous).

    Args:
        data1 (dict): The data from the anonymous API call.
        data2 (dict): The data from the anonres API call.

    Returns:
        int: The difference in line counts (anonres - anonymous). Returns None if either input is None.
    """
    if data1 is None or data2 is None:
        return None
    try:
        line_count1 = data1['json']['line_count']
        line_count2 = data2['json']['line_count']
        return line_count2 - line_count1  # anonres - anonymous
    except KeyError:
        print("Error: 'json' or 'line_count' key not found in data.", file=sys.stderr)
        return None
    except TypeError:
        print("Error: Input data was not a dictionary as expected.", file=sys.stderr)
        return None

# --- Functions for downloading, decompressing, and writing raw content ---
def download_gzip_file_content(url, token):
    """
    Downloads a gzip compressed file from the specified URL.

    Args:
        url (str): The URL of the file to download.
        token (str): The API authentication token.

    Returns:
        bytes: The downloaded gzip file content as bytes, or None on error.
    """
    headers = {'Token': token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.content  # Return the content as bytes
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file from {url}: {e}", file=sys.stderr)
        return None

def decompress_gzip_content(data):
    """
    Decompresses gzip data from bytes to bytes.

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

def write_bytes_to_file(data_bytes, filename):
    """
    Writes raw bytes content to a file.

    Args:
        data_bytes (bytes): The bytes content to write.
        filename (str): The name of the file to write to.
    """
    try:
        with open(filename, 'wb') as f:
            f.write(data_bytes)
        # print(f"Full decompressed data successfully written to {filename}") # Removed verbose output
        return True
    except Exception as e:
        print(f"Error writing full decompressed data to file '{filename}': {e}", file=sys.stderr)
        return False

# --- Functions for extracting and writing service metrics list ---
def extract_service_metrics_from_json_bytes(data_bytes):
    """
    Extracts service metric names from JSON data (as bytes), handling multiple JSON objects or a single list.
    This is specifically for the Service Metrics feed, which is often a JSON array of strings.

    Args:
        data_bytes (bytes): The JSON data as bytes.

    Returns:
        list: A list of service metric names (strings), or None on error.
    """
    service_metrics = []
    try:
        json_string = data_bytes.decode('utf-8')
        
        # Try to parse as a single JSON array first (expected for service metrics)
        try:
            parsed_data = json.loads(json_string)
            if isinstance(parsed_data, list):
                # Ensure all items in the list are strings
                service_metrics = [str(item) for item in parsed_data if isinstance(item, (str, int, float, bool))]
                if len(service_metrics) != len(parsed_data):
                    print("Warning: Non-primitive types found in Service Metrics list; some items may have been skipped.", file=sys.stderr)
                return service_metrics
            else:
                # print("Warning: Service Metrics JSON is not a list. Attempting line-by-line parsing.", file=sys.stderr) # Removed verbose output
                pass # Continue to line-by-line parsing
        except json.JSONDecodeError:
            # Fallback to line-by-line parsing if it's JSON Lines or other
            # print(f"Warning: File content is not a single JSON array. Attempting line-by-line parsing for potential JSON Lines.", file=sys.stderr) # Removed verbose output
            pass # Continue to line-by-line parsing

        # If not a single JSON array, or if initial parse failed, process line by line
        for line_num, line in enumerate(json_string.strip().split('\n'), 1):
            line = line.strip()
            if not line:
                continue
            try:
                # Try to parse each line as a JSON value (e.g., "metric_name" or {"tag": "metric"})
                parsed_line = json.loads(line)
                if isinstance(parsed_line, str):
                    service_metrics.append(parsed_line)
                elif isinstance(parsed_line, dict) and 'tag' in parsed_line:
                    service_metrics.append(str(parsed_line['tag']))
                else:
                    # print(f"Warning: Line {line_num} in Service Metrics data is not a string or dict with 'tag'. Skipping: {line[:80]}...", file=sys.stderr) # Removed verbose output
                    pass
            except json.JSONDecodeError:
                # If not valid JSON, treat as a raw string (e.g., if the file is just plain text lines)
                service_metrics.append(line)

        return service_metrics

    except Exception as e:
        print(f"Error extracting service metrics: {e}", file=sys.stderr)
        return None

def write_list_to_file(data_list, filename):
    """
    Writes a list of strings to a file, one item per line.

    Args:
        data_list (list): The list of strings to write.
        filename (str): The name of the file to write to.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for item in data_list:
                f.write(str(item) + '\n') # Ensure item is string before writing
        # print(f"List successfully written to {filename}") # Removed verbose output
        return True
    except Exception as e:
        print(f"Error writing list to file '{filename}': {e}", file=sys.stderr)
        return False

def cleanup_files(file_list):
    """
    Deletes a list of files from the filesystem.

    Args:
        file_list (list): A list of file paths to delete.
    """
    for file_path in file_list:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                # print(f"Deleted temporary file: {file_path}") # Removed verbose output
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}", file=sys.stderr)

# --- Main Script ---
def main():
    """
    Main function to execute the data fetching and comparison.
    """
    token = os.environ.get('TOKEN')
    if not token:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    current_date_ymd = datetime.datetime.now().strftime("%Y%m%d")

    # List to keep track of files created for cleanup
    files_to_delete = []

    # --- Part 1: Daily Feed Counts ---
    anonymous_feed_url = 'https://feeds.spur.us/v2/anonymous/latest'
    anonres_feed_url = 'https://feeds.spur.us/v2/anonymous-residential/latest'

    daily_feed_output_filename = f"{current_date_ymd}DailyFeedCount.txt"
    files_to_delete.append(daily_feed_output_filename) # Add to cleanup list

    try:
        with open(daily_feed_output_filename, 'w', encoding='utf-8') as outfile:
            anonymous_data = fetch_data(anonymous_feed_url, token)
            anonres_data = fetch_data(anonres_feed_url, token)

            if anonymous_data and anonres_data:
                line_count_diff = calculate_line_count_difference(anonymous_data, anonres_data)
                if line_count_diff is not None:
                    anon_count_str = f"Anonymous Feed Line Count: {anonymous_data['json']['line_count']}"
                    anonres_count_str = f"AnonRes Feed Line Count: {anonres_data['json']['line_count']}"
                    diff_count_str = f"Residential IP Address Line Count: {line_count_diff}"

                    # Only print these lines to console
                    print(anon_count_str)
                    print(anonres_count_str)
                    print(diff_count_str)

                    # Write to file
                    outfile.write(anon_count_str + '\n')
                    outfile.write(anonres_count_str + '\n')
                    outfile.write(diff_count_str + '\n')
                else:
                    error_msg = "Could not calculate line count difference."
                    print(error_msg, file=sys.stderr)
                    outfile.write(error_msg + '\n')
            else:
                error_msg = "Failed to retrieve data for daily feed counts from one or both APIs. Check network connection and TOKEN."
                print(error_msg, file=sys.stderr)
                outfile.write(error_msg + '\n')
    except IOError as e:
        print(f"Error writing to output file '{daily_feed_output_filename}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during daily feed count processing: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Part 2: Service Metrics List Export and Full JSON Save ---
    service_metrics_url = 'https://feeds.spur.us/v2/service-metrics/latest.json.gz'
    service_metrics_gz_filename = f"{current_date_ymd}ServiceMetrics.json.gz" # Temporary gz file
    service_metrics_output_filename = f"{current_date_ymd}ServiceMetricsList.txt"
    service_metrics_full_json_filename = f"{current_date_ymd}ServiceMetricsAll-Full.json"

    files_to_delete.append(service_metrics_gz_filename) # Add to cleanup list
    files_to_delete.append(service_metrics_output_filename) # Add to cleanup list
    files_to_delete.append(service_metrics_full_json_filename) # Add to cleanup list

    service_metrics_gz_content = download_gzip_file_content(service_metrics_url, token)

    if service_metrics_gz_content:
        # Save the downloaded gzip content to a temporary file before decompression
        try:
            with open(service_metrics_gz_filename, 'wb') as f:
                f.write(service_metrics_gz_content)
        except Exception as e:
            print(f"Error saving downloaded gzip content to file '{service_metrics_gz_filename}': {e}", file=sys.stderr)
            # Don't exit, try to decompress from memory if file save failed

        decompressed_service_metrics_content = decompress_gzip_content(service_metrics_gz_content)
        if decompressed_service_metrics_content:
            # Save the full decompressed JSON content
            if not write_bytes_to_file(decompressed_service_metrics_content, service_metrics_full_json_filename):
                print("Failed to save full decompressed Service Metrics data. Continuing with list extraction...", file=sys.stderr)

            service_metric_names = extract_service_metrics_from_json_bytes(decompressed_service_metrics_content)
            if service_metric_names:
                if write_list_to_file(service_metric_names, service_metrics_output_filename):
                    print(f"Service Metrics List Count: {len(service_metric_names)}")
                else:
                    print("Failed to write Service Metrics list to file.", file=sys.stderr)
            else:
                print("Failed to extract service metrics from the data.", file=sys.stderr)
        else:
            print("Failed to decompress Service Metrics data.", file=sys.stderr)
    else:
        print("Failed to download Service Metrics file.", file=sys.stderr)

    cleanup_files(files_to_delete)

if __name__ == "__main__":
    main()
