"""
Data Loader Script
Loads raw JSON files from the data lake into PostgreSQL database
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'telegram_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        """Initialize the data loader with database connection"""
        self.engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        self.raw_data_dir = Path("../data/raw/telegram_messages")
        
    def create_raw_schema(self):
        """Create the raw schema and tables"""
        with self.engine.connect() as conn:
            # Create raw schema
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            
            # Create raw_telegram_messages table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.raw_telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    channel_title VARCHAR(255),
                    channel_username VARCHAR(100),
                    message_text TEXT,
                    message_date TIMESTAMP,
                    media_path VARCHAR(500),
                    views INTEGER,
                    forwards INTEGER,
                    replies INTEGER,
                    edit_date TIMESTAMP,
                    has_media BOOLEAN,
                    media_type VARCHAR(50),
                    raw_data JSONB,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create raw_channel_info table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.raw_channel_info (
                    id SERIAL PRIMARY KEY,
                    channel_username VARCHAR(100),
                    channel_title VARCHAR(255),
                    scraped_at TIMESTAMP,
                    message_count INTEGER,
                    raw_data JSONB,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            logger.info("Raw schema and tables created successfully")
    
    def load_json_file(self, file_path: Path) -> Optional[Dict]:
        """Load and parse a JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            return None
    
    def extract_messages_data(self, json_data: Dict, file_path: Path) -> List[Dict]:
        """Extract messages data from JSON and prepare for database insertion"""
        messages_data = []
        
        if 'messages' not in json_data:
            logger.warning(f"No messages found in {file_path}")
            return messages_data
        
        channel_info = json_data.get('channel_info', {})
        
        for message in json_data['messages']:
            # Parse date strings to datetime objects
            message_date = None
            if message.get('date'):
                try:
                    message_date = datetime.fromisoformat(message['date'].replace('Z', '+00:00'))
                except:
                    message_date = None
            
            edit_date = None
            if message.get('edit_date'):
                try:
                    edit_date = datetime.fromisoformat(message['edit_date'].replace('Z', '+00:00'))
                except:
                    edit_date = None
            
            # Prepare message data
            message_data = {
                'message_id': message.get('id'),
                'channel_title': channel_info.get('title'),
                'channel_username': channel_info.get('username'),
                'message_text': message.get('message'),
                'message_date': message_date,
                'media_path': message.get('media_path'),
                'views': message.get('views'),
                'forwards': message.get('forwards'),
                'replies': message.get('replies'),
                'edit_date': edit_date,
                'has_media': message.get('has_media'),
                'media_type': message.get('media_type'),
                'raw_data': json.dumps(message)
            }
            
            messages_data.append(message_data)
        
        return messages_data
    
    def insert_messages_batch(self, messages_data: List[Dict]):
        """Insert a batch of messages into the database"""
        if not messages_data:
            return
        
        with self.engine.connect() as conn:
            # Convert to DataFrame for easier insertion
            df = pd.DataFrame(messages_data)
            
            # Insert into database
            df.to_sql(
                'raw_telegram_messages',
                conn,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            conn.commit()
            logger.info(f"Inserted {len(messages_data)} messages into database")
    
    def insert_channel_info(self, json_data: Dict, file_path: Path):
        """Insert channel information into the database"""
        channel_info = json_data.get('channel_info', {})
        
        if not channel_info:
            logger.warning(f"No channel info found in {file_path}")
            return
        
        # Parse scraped_at date
        scraped_at = None
        if channel_info.get('scraped_at'):
            try:
                scraped_at = datetime.fromisoformat(channel_info['scraped_at'].replace('Z', '+00:00'))
            except:
                scraped_at = None
        
        channel_data = {
            'channel_username': channel_info.get('username'),
            'channel_title': channel_info.get('title'),
            'scraped_at': scraped_at,
            'message_count': channel_info.get('message_count'),
            'raw_data': json.dumps(channel_info)
        }
        
        with self.engine.connect() as conn:
            df = pd.DataFrame([channel_data])
            df.to_sql(
                'raw_channel_info',
                conn,
                schema='raw',
                if_exists='append',
                index=False
            )
            conn.commit()
            logger.info(f"Inserted channel info for {channel_info.get('username')}")
    
    def load_all_data(self):
        """Load all JSON files from the raw data directory"""
        logger.info("Starting data loading process")
        
        # Create schema and tables
        self.create_raw_schema()
        
        # Find all JSON files
        json_files = list(self.raw_data_dir.rglob("*.json"))
        
        if not json_files:
            logger.warning(f"No JSON files found in {self.raw_data_dir}")
            return
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        total_messages = 0
        
        for file_path in json_files:
            logger.info(f"Processing file: {file_path}")
            
            # Load JSON data
            json_data = self.load_json_file(file_path)
            if not json_data:
                continue
            
            # Extract and insert messages
            messages_data = self.extract_messages_data(json_data, file_path)
            if messages_data:
                self.insert_messages_batch(messages_data)
                total_messages += len(messages_data)
            
            # Insert channel info
            self.insert_channel_info(json_data, file_path)
        
        logger.info(f"Data loading completed. Total messages loaded: {total_messages}")

def main():
    """Main function to run the data loader"""
    try:
        loader = DataLoader()
        loader.load_all_data()
        logger.info("Data loading process completed successfully")
    except Exception as e:
        logger.error(f"Error in data loading process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 