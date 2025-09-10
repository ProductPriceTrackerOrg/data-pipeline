import json
import os

# Test JSON file reading
json_file = r"D:\Semester 5\DSE Project\data-pipeline\transformations\extraction\temp_data\sample_download.json"

print(f"Checking file: {json_file}")
print(f"File exists: {os.path.exists(json_file)}")

if os.path.exists(json_file):
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"JSON loaded successfully")
        print(f"Type: {type(data)}")
        print(f"Length: {len(data) if isinstance(data, (list, dict)) else 'N/A'}")
        
        if isinstance(data, list) and len(data) > 0:
            print(f"First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dict'}")
            
    except Exception as e:
        print(f"Error loading JSON: {e}")
else:
    print("File does not exist!")
