import os
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Paths
CSV_PATH = Path('data/labeled/telegram_data.csv')
OUTPUT_DIR = Path('data/raw/telegrammessages')

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_channel(channel_username):
    if pd.isna(channel_username):
        return 'unknown_channel'
    return channel_username.replace('@', '').replace('/', '_')

def main():
    df = pd.read_csv(CSV_PATH)
    
    # Normalize date column
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Group by date and channel
    for idx, row in df.iterrows():
        date = row['Date']
        if pd.isna(date):
            continue
        date_str = date.strftime('%Y-%m-%d')
        channel = sanitize_channel(row['Channel Username'])
        out_dir = OUTPUT_DIR / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f'{channel}.json'
        record = {
            'id': row.get('ID'),
            'chat': {
                'title': row.get('Channel Title'),
                'username': row.get('Channel Username'),
            },
            'text': row.get('Message'),
            'date': row.get('Date').isoformat() if not pd.isna(row.get('Date')) else None,
            'media': row.get('Media Path') if pd.notna(row.get('Media Path')) and row.get('Media Path') else None,
            'media_type': None  # Could infer from file extension if needed
        }
        # Append to file if exists, else create new list
        if out_path.exists():
            with open(out_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                data.append(record)
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()
        else:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump([record], f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main() 