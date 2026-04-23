import json
import sys
import os

# --- Configuration ---
# Default output file extension for JSON Lines (though user can specify .json)
DEFAULT_JSONL_EXTENSION = ".jsonl"

# --- Functions ---
def convert_json_array_to_jsonl(input_file_path, output_file_path):
    """
    Converts a JSON file containing a single large array of JSON objects
    into a JSON Lines (JSONL) formatted file.
    Each JSON object from the array will be written on a new line.
    """
    try:
        # Read the entire content of the input JSON file
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            file_content = infile.read().strip()

        # Attempt to parse the content as a single JSON array
        data = json.loads(file_content)

        # Ensure the parsed data is indeed a list (JSON array)
        if not isinstance(data, list):
            print(f"Error: Input JSON file must contain a single JSON array (e.g., [{{...}}, {{...}}]). Found: {type(data)}", file=sys.stderr)
            sys.exit(1)

        # Write each JSON object from the array to the output file as a separate line
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            for item in data:
                # json.dumps converts a Python dict to a JSON string
                # ensure_ascii=False ensures non-ASCII characters (like 'ó') are written directly
                outfile.write(json.dumps(item, ensure_ascii=False) + '\n')
        
        print(f"Successfully converted '{input_file_path}' to JSON Lines format at '{output_file_path}'")

    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_file_path}'", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from '{input_file_path}': {e}. Please ensure it's valid JSON.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

# --- Main Script Execution ---
if __name__ == "__main__":
    # Check for correct command-line arguments
    if len(sys.argv) != 2:
        print("Usage: python convert_to_jsonl.py <input_json_file_path>", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]

    # Determine output directory (same as input file's directory)
    output_directory = os.path.dirname(input_path)
    input_filename_without_ext = os.path.splitext(os.path.basename(input_path))[0]

    # Ask for output file name
    user_output_filename = input("Enter the desired output file name (e.g., output_data.json): ").strip()

    if not user_output_filename:
        # Use a default name if user provides no input
        output_filename = input_filename_without_ext + ".json"
        print(f"No output filename provided. Using default: {output_filename}")
    else:
        # Sanitize filename and ensure it ends with .json
        output_filename = "".join(x for x in user_output_filename if x.isalnum() or x in "._-")
        if not output_filename.lower().endswith(".json"):
            output_filename += ".json"
    
    output_path = os.path.join(output_directory, output_filename)

    # Perform the conversion
    convert_json_array_to_jsonl(input_path, output_path)
