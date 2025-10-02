#!/usr/bin/env python3

import os
import requests
import json

# ANSI escape codes for color
CYAN = "\033[96m"
RESET = "\033[0m"

def get_external_ip():
    response = requests.get('https://api.ipify.org?format=json')
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()['ip']

def get_spur_token():
    token = os.getenv('TOKEN')
    if not token:
        token = input("Please enter your Spur token: ")
    return token

def fetch_spur_data(ip, token):
    url = f"https://api.spur.us/v2/context/{ip}?spurgeo=1"
    headers = {'Token': token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def pretty_print_json(data, indent=0):
    # Pretty print JSON with colorized keys
    spacing = '    '  # 4 spaces for indentation
    if isinstance(data, dict):
        for key, value in data.items():
            # Print the key with color and indentation
            print(f"{spacing * indent}{CYAN}{key}{RESET}: ", end="")
            if isinstance(value, (dict, list)):
                print()  # New line for nested structures
                pretty_print_json(value, indent + 1)  # Increase indentation for nested structures
            else:
                print(value)  # Print the value
    elif isinstance(data, list):
        for item in data:
            pretty_print_json(item, indent)  # Maintain the same indentation for list items
    else:
        print(f"{spacing * indent}{data}")  # Print the value with indentation

def main():
    try:
        ip = get_external_ip()
        token = get_spur_token()
        data = fetch_spur_data(ip, token)
        pretty_print_json(data)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
