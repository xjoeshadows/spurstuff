import sys

def read_tags_from_file(filename):
    """
    Reads tags from a file, one tag per line.

    Args:
        filename (str): The name of the file to read from.

    Returns:
        list: A list of tags, or None on error.
    """
    try:
        with open(filename, 'r') as f:
            tags = [line.strip() for line in f]  # Read lines and remove trailing newlines
        return tags
    except FileNotFoundError:
        print(f"Error: File not found: {filename}")
        return None
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        return None

def compare_tag_lists(tags1, tags2):
    """
    Compares two lists of tags and returns the differences.

    Args:
        tags1 (list): The first list of tags.
        tags2 (list): The second list of tags.

    Returns:
        dict: A dictionary containing the differences:
            {
                'added': tags present in tags2 but not in tags1,
                'removed': tags present in tags1 but not in tags2
            }
        Returns None if there is an error
    """
    if tags1 is None or tags2 is None:
        return None

    added = list(set(tags2) - set(tags1))
    removed = list(set(tags1) - set(tags2))
    return {'added': added, 'removed': removed}

def main():
    """
    Main function to compare tag lists from two files.
    """
    if len(sys.argv) != 3:
        print("Usage: python3 servicetagsdiff.py file1.txt file2.txt")
        return

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    tags1 = read_tags_from_file(file1)
    tags2 = read_tags_from_file(file2)

    if tags1 is not None and tags2 is not None:
        differences = compare_tag_lists(tags1, tags2)
        if differences is not None:
            print("Differences between tag lists:")
            print("Tags added:")
            for tag in differences['added']:
                print(f"  {tag}")
            print("Tags removed:")
            for tag in differences['removed']:
                print(f"  {tag}")
        else:
            print("Error: Could not compare tag lists.")
    else:
        print("Error: Could not read tag lists from files.")

if __name__ == "__main__":
    main()
