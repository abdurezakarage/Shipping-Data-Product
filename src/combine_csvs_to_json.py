import os
import csv
import json

# Always use the absolute path to data/labeled
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LABELED_DIR = os.path.join(BASE_DIR, 'data', 'labeled')
OUTPUT_FILE = os.path.join(LABELED_DIR, 'all_data.json')

all_records = []

for root, dirs, files in os.walk(LABELED_DIR):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    all_records.append(row)

with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
    json.dump(all_records, out_f, ensure_ascii=False, indent=2)

print(f"Combined {len(all_records)} records from CSV files into {OUTPUT_FILE}") 