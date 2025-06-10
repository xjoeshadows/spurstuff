import requests

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
        print(f"Error fetching data from {endpoint}: {e}")
        return None
    except ValueError as e:
        print(f"Error decoding JSON from {endpoint}: {e}")
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
        print("Error: 'json' or 'line_count' key not found in data.")
        return None
    except TypeError:
        print("Error: Input data was not a dictionary as expected.")
        return None

def main():
    """
    Main function to execute the data fetching and comparison.
    """
    spur_token = "YOURTOKENHERE"  # Hardcoded SPUR API Token
    anonymous_feed_url = 'https://feeds.spur.us/v2/anonymous/latest'
    anonres_feed_url = 'https://feeds.spur.us/v2/anonymous-residential/latest'

    anonymous_data = fetch_data(anonymous_feed_url, spur_token)
    anonres_data = fetch_data(anonres_feed_url, spur_token)

    if anonymous_data and anonres_data:
        line_count_diff = calculate_line_count_difference(anonymous_data, anonres_data)
        if line_count_diff is not None:
            print(f"Anonymous Feed Line Count: {anonymous_data['json']['line_count']}")
            print(f"AnonRes Feed Line Count: {anonres_data['json']['line_count']}")
            print(f"Residential IP Address Line Count: {line_count_diff}") # Changed output
        else:
            print("Could not calculate line count difference.")
    else:
        print("Failed to retrieve data from one or both APIs.")

if __name__ == "__main__":
    main()
