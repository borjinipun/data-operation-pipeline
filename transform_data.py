import os
import sys
import csv
import glob
import subprocess
from datetime import datetime

SOURCE_FOLDER = 'source_folder'  # Folder containing parts.csv and category_association.csv
DELTA_FOLDER = 'input_part_numbers'  # Folder containing delta_parts_*.csv
FEED_FILENAME = 'output_data_feed.csv'  # Output file name


def log(msg):
    print(f"[INFO] {msg}")

def error(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)

def read_csv(filepath):
    with open(filepath, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_csv(filepath, fieldnames, rows):
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def get_latest_delta_file():
    files = glob.glob(os.path.join(DELTA_FOLDER, 'delta_parts_*.csv'))
    if not files:
        return None
    return max(files, key=os.path.getctime)

def git_commit(file_path, message):
    subprocess.run(['git', 'add', file_path], check=True)
    subprocess.run(['git', 'commit', '-m', message], check=True)

def main():
    try:
        # Find latest delta file
        delta_file = get_latest_delta_file()
        if not delta_file:
            log('No delta part number file found. Exiting.')
            return
        log(f'Processing delta file: {delta_file}')

        # Read delta part numbers
        delta_parts = set()
        for row in read_csv(delta_file):
            part_num = row.get('part_number')
            if part_num:
                delta_parts.add(part_num)
        if not delta_parts:
            log('No part numbers found in delta file. Exiting.')
            return

        # Read source files
        parts_path = os.path.join(SOURCE_FOLDER, 'parts.csv')
        cat_assoc_path = os.path.join(SOURCE_FOLDER, 'category_association.csv')
        parts = read_csv(parts_path)
        cat_assoc = read_csv(cat_assoc_path)

        # Filter and enrich parts
        filtered_parts = [p for p in parts if p['part_number'] in delta_parts]
        cat_map = {c['part_number']: c['category'] for c in cat_assoc}
        for p in filtered_parts:
            p['category'] = cat_map.get(p['part_number'], '')

        # Write output feed file
        output_path = os.path.join(SOURCE_FOLDER, FEED_FILENAME)
        fieldnames = filtered_parts[0].keys() if filtered_parts else []
        write_csv(output_path, fieldnames, filtered_parts)
        log(f'Wrote data feed file: {output_path}')

        # Delete delta file
        os.remove(delta_file)
        log(f'Deleted processed delta file: {delta_file}')

    except Exception as e:
        error(f'Processing failed: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main()
