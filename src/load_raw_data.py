#!/usr/bin/env python3
"""
Raw Data Loader Script
Loads raw JSON files from data lake into PostgreSQL database
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RawDataLoader:
    def __init__(self):
        """Initialize the data loader with database connection"""
        # Validate database environment variables
        self._validate_env_variables()
        
        # Get database configuration from environment
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'password')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'shipping_data_warehouse')
        
        self.db_url = (
            f"postgresql://{db_user}:{db_password}@"
            f"{db_host}:{db_port}/{db_name}"
        )
        self.engine = create_engine(self.db_url)
        self.data_lake_path = Path(os.getenv('DATA_LAKE_PATH', './data/raw/telegrammessages'))
        
        # Create logs directory if it doesn't exist
        Path('logs').mkdir(exist_ok=True)
        
    def _validate_env_variables(self):
        """Validate that required environment variables are set"""
        required_vars = {
            'DB_USER': 'Database username',
            'DB_PASSWORD': 'Database password',
            'DB_HOST': 'Database host',
            'DB_PORT': 'Database port',
            'DB_NAME': 'Database name'
        }
        
        missing_vars = []
        for var, description in required_vars.items():
            value = os.getenv(var)
            if not value or value in ['your_username_here', 'your_password_here', 'your_host_here', 'your_port_here', 'your_database_here']:
                missing_vars.append(f"{var} ({description})")
        
        if missing_vars:
            logger.error("❌ Missing or invalid environment variables:")
            for var in missing_vars:
                logger.error(f"   - {var}")
            logger.error("Please create a .env file with your database configuration")
            logger.error("You can copy env_template.txt to .env and fill in your credentials")
            raise ValueError("Missing required environment variables")
        
        logger.info("✅ Database environment variables validated successfully")
        
    def create_raw_schema(self):
        """Create the raw schema and tables if they don't exist"""
        try:
            with self.engine.connect() as conn:
                # Create raw schema
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
                
                # Create raw telegram messages table
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    channel_name VARCHAR(255),
                    channel_id BIGINT,
                    sender_id BIGINT,
                    sender_username VARCHAR(255),
                    message_text TEXT,
                    message_date TIMESTAMP,
                    has_media BOOLEAN,
                    media_type VARCHAR(50),
                    views INTEGER,
                    forwards INTEGER,
                    replies INTEGER,
                    raw_data JSONB,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                conn.execute(text(create_table_sql))
                conn.commit()
                logger.info("Raw schema and tables created successfully")
                
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise
    
    def load_json_file(self, file_path: Path):
        """Load a single JSON file into the database"""
        try:
            logger.info(f"Loading file: {file_path}")
            
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract channel information from file path
            # Expected path: data/raw/telegrammessages/YYYY-MM-DD/channelname.json
            path_parts = file_path.parts
            if len(path_parts) >= 4:
                date_str = path_parts[-2]  # YYYY-MM-DD
                channel_name = path_parts[-1].replace('.json', '')
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
                channel_name = 'unknown'
            
            # Prepare data for insertion
            records = []
            if isinstance(data, list):
                messages = data
            else:
                messages = [data]
            
            for msg in messages:
                record = {
                    'message_id': msg.get('id'),
                    'channel_name': channel_name,
                    'channel_id': msg.get('chat', {}).get('id'),
                    'sender_id': msg.get('from', {}).get('id'),
                    'sender_username': msg.get('from', {}).get('username'),
                    'message_text': msg.get('text', msg.get('message')),
                    'message_date': msg.get('date'),
                    'has_media': bool(msg.get('media')),
                    'media_type': msg.get('media_type'),
                    'views': msg.get('views'),
                    'forwards': msg.get('forwards'),
                    'replies': msg.get('replies'),
                    'raw_data': json.dumps(msg)
                }
                records.append(record)
            
            # Insert into database
            if records:
                df = pd.DataFrame(records)
                df.to_sql('telegram_messages', self.engine, schema='raw', 
                         if_exists='append', index=False, method='multi')
                logger.info(f"Successfully loaded {len(records)} records from {file_path}")
            
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {str(e)}")
            raise
    
    def load_all_raw_data(self):
        """Load all JSON files from the data lake"""
        try:
            logger.info("Starting raw data load process")
            
            # Create schema and tables
            self.create_raw_schema()
            
            # Find all JSON files in the data lake
            json_files = list(self.data_lake_path.rglob('*.json'))
            
            if not json_files:
                logger.warning(f"No JSON files found in {self.data_lake_path}")
                return
            
            logger.info(f"Found {len(json_files)} JSON files to process")
            
            # Load each file
            success_count = 0
            error_count = 0
            
            for file_path in json_files:
                try:
                    self.load_json_file(file_path)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Failed to load {file_path}: {str(e)}")
            
            logger.info(f"Data load completed. Success: {success_count}, Errors: {error_count}")
            
        except Exception as e:
            logger.error(f"Error in load_all_raw_data: {str(e)}")
            raise

def main():
    """Main function to run the data loader"""
    try:
        logger.info("🚀 Starting raw data loader...")
        loader = RawDataLoader()
        loader.load_all_raw_data()
        logger.info("✅ Raw data loading completed successfully")
    except ValueError as e:
        logger.error(f"❌ Configuration error: {str(e)}")
        logger.error("Please check your .env file configuration")
        exit(1)
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 