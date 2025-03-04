import json
import sys
import os

# Function to extract all workflow_ids from the JSON
def extract_workflow_ids(data):
    workflow_ids = []

    # Check if data is a dictionary or a list
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'workflow_id':
                workflow_ids.append(value)
            else:
                workflow_ids.extend(extract_workflow_ids(value))
    elif isinstance(data, list):
        for item in data:
            workflow_ids.extend(extract_workflow_ids(item))

    return workflow_ids

def process_json_file(json_file, txt_file='workflow_ids.txt'):
    # Check if file exists
    if not os.path.isfile(json_file):
        print(f"File {json_file} not found.")
        return
    
    # Load JSON data
    try:
        with open(json_file, 'r') as file:
            data = json.load(file)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return

    # Extract workflow IDs
    workflow_ids = extract_workflow_ids(data)

    # Check if there are any workflow IDs extracted
    if not workflow_ids:
        print("No workflow IDs found.")
        return

    # Check if the file already exists 
    file_exists = os.path.isfile(txt_file)

    # Write workflow_ids to a text file
    try:
        with open(txt_file, "a") as f:
            if not file_exists:
                f.write("workflow_run_id\n")  # Write the header only if the file doesn't exist
            for workflow_id in workflow_ids:
                f.write(str(workflow_id) + "\n")
    except Exception as e:
        print(f"Error writing to file: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <path_to_json_file>")
    else:
        json_file = sys.argv[1]
        process_json_file(json_file)
