"""
Configuration file for Telegram scraping
"""

# Telegram API Configuration
TELEGRAM_CONFIG = {
    'session_name': 'scraping_session',
    'message_limit': 100,  # Number of messages to scrape per channel
    'delay_between_channels': 2,  # Seconds to wait between channels
    'retry_delay': 30,  # Seconds to wait before retry on failure
}

# Ethiopian Medical Channels
# Primary channels from the requirements
PRIMARY_CHANNELS = [
    '@CheMed123',
    '@lobelia4cosmetics',
    '@tikvahpharma'
]

# Additional channels from et.tgstat.com/medicine (example channels)
ADDITIONAL_CHANNELS = [
    '@ethiopianpharma',
    '@medicines_ethiopia', 
    '@pharmacy_ethiopia',
    '@healthcare_ethiopia',
    '@medical_supplies_ethiopia',
    '@ethiopia_pharmaceuticals',
    '@addis_pharma',
    '@ethiopian_healthcare'
]

# Combine all channels
ALL_CHANNELS = PRIMARY_CHANNELS + ADDITIONAL_CHANNELS

# Data storage configuration
DATA_CONFIG = {
    'base_data_dir': '../data/raw/telegram_messages',
    'media_dir': '../data/raw/media',
    'scraping_log_file': 'scraping_status.json',
    'log_file': 'telegram_scraper.log'
}

# Media types to collect for object detection
MEDIA_TYPES = ['photo', 'document']  # Focus on images and documents

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file_handler': True,
    'console_handler': True
} 