import os
import json
import json
from pathlib import Path

# Get the directory where this script is located
main_folder = Path(__file__).resolve().parent

# Create raw data folder
folder_path = main_folder / "data" / "raw" / "requests" 

for filename in os.listdir(folder_path):
    if filename.endswith('.json'):
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data.get("status") == "REQUEST_NOT_PROCESSED":
                os.remove(file_path)
                print(f"Deleted: {filename}")
        except (json.JSONDecodeError, OSError) as e:
            print(f"Skipped (error reading {filename}): {e}")

