import requests
import gzip
import json
import io
import os
import sys # Import sys for exiting on critical errors

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
                if isinstance(json_data, dict) and 'tag' in json_data:
                    tag_values.append(json_data['tag'])
            except json.JSONDecodeError:
                # Print a warning for invalid JSON objects but continue processing
                print(f"Warning: Skipping invalid JSON object line: {obj_str[:80]}...", file=sys.stderr)
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


def main():
    """
    Main function to download, decompress, and extract tag values.
    """
    # Use TOKEN from environment variable
    token = os.environ.get('TOKEN')
    if not token:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)
        
    file_url = 'https://feeds.spur.us/v2/service-metrics/latest.json.gz'
    
    # Prompt the user for the output filename
    output_filename = input("Enter the name of the output file (e.g., servicetags.txt): ").strip()

    # Set default filename if user input is empty
    if not output_filename:
        output_filename = "servicetags.txt"
        print(f"No filename provided. Using default: {output_filename}")

    file_content = download_file(file_url, token)
    if file_content:
        decompressed_data = decompress_gzip(file_content)
        if decompressed_data:
            tag_values = extract_tag_values(decompressed_data)
            if tag_values:
                if not write_tags_to_file(tag_values, output_filename):
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
