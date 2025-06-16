import requests
import os # Import os for environment variables
import sys # Import sys for exiting on critical errors
import datetime # Import datetime for generating date-based filenames

def fetch_data(endpoint, token):
    """
    Fetches data from the specified API endpoint.

    Args:
        endpoint (str): The API endpoint URL.
        token (str): The API authentication token.

    Returns:
        dict: The JSON response from the API.  Returns None on error.
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

def main():
    """
    Main function to execute the data fetching and comparison.
    """
    # Use TOKEN from environment variable
    token = os.environ.get('TOKEN')
    if not token:
        print("Error: TOKEN environment variable not set. Please set the TOKEN environment variable.", file=sys.stderr)
        sys.exit(1)

    anonymous_feed_url = 'https://feeds.spur.us/v2/anonymous/latest'
    anonres_feed_url = 'https://feeds.spur.us/v2/anonymous-residential/latest'

    anonymous_data = fetch_data(anonymous_feed_url, token)
    anonres_data = fetch_data(anonres_feed_url, token)

    # Generate output filename with current date (YYYYMMDD)
    current_date_ymd = datetime.datetime.now().strftime("%Y%m%d")
    output_filename = f"{current_date_ymd}DailyFeedCount.txt"

    # Open the output file for writing
    try:
        with open(output_filename, 'w', encoding='utf-8') as outfile:
            if anonymous_data and anonres_data:
                line_count_diff = calculate_line_count_difference(anonymous_data, anonres_data)
                if line_count_diff is not None:
                    # Capture output strings
                    anon_count_str = f"Anonymous Feed Line Count: {anonymous_data['json']['line_count']}"
                    anonres_count_str = f"AnonRes Feed Line Count: {anonres_data['json']['line_count']}"
                    diff_count_str = f"Residential IP Address Line Count: {line_count_diff}"

                    # Print to console
                    print(anon_count_str)
                    print(anonres_count_str)
                    print(diff_count_str)

                    # Write to file
                    outfile.write(anon_count_str + '\n')
                    outfile.write(anonres_count_str + '\n')
                    outfile.write(diff_count_str + '\n')
                    print(f"\nResults successfully exported to {output_filename}")
                else:
                    error_msg = "Could not calculate line count difference."
                    print(error_msg, file=sys.stderr)
                    outfile.write(error_msg + '\n')
            else:
                error_msg = "Failed to retrieve data from one or both APIs. Check network connection and TOKEN."
                print(error_msg, file=sys.stderr)
                outfile.write(error_msg + '\n')
    except IOError as e:
        print(f"Error writing to output file '{output_filename}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
