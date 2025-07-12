import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatAdminRequiredError
from telethon.tl.types import Message, Channel, Chat
from dotenv import load_dotenv

from config import TELEGRAM_CONFIG, ALL_CHANNELS, DATA_CONFIG, LOGGING_CONFIG

# Load environment variables
load_dotenv('.env')

# Configuration
API_ID = os.getenv('TG_API_ID')
API_HASH = os.getenv('TG_API_HASH')
PHONE = os.getenv('phone')

# Setup logging
handlers = []
if LOGGING_CONFIG['file_handler']:
    handlers.append(logging.FileHandler(DATA_CONFIG['log_file']))
if LOGGING_CONFIG['console_handler']:
    handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG['level']),
    format=LOGGING_CONFIG['format'],
    handlers=handlers
)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self):
        # Validate API credentials
        if not API_ID or not API_HASH:
            raise ValueError("TG_API_ID and TG_API_HASH must be set in environment variables")
        
        self.client = TelegramClient(TELEGRAM_CONFIG['session_name'], int(API_ID), API_HASH)
        self.base_data_dir = Path(DATA_CONFIG['base_data_dir'])
        self.media_dir = Path(DATA_CONFIG['media_dir'])
        self.scraping_log_file = DATA_CONFIG['scraping_log_file']
        
        # Create directories
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        
        # Load scraping status
        self.scraping_status = self.load_scraping_status()
        
        # Use channels from config
        self.channels = ALL_CHANNELS
    
    def load_scraping_status(self) -> Dict:
        """Load scraping status from file"""
        if os.path.exists(self.scraping_log_file):
            try:
                with open(self.scraping_log_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid scraping status file, starting fresh")
        return {}
    
    def save_scraping_status(self):
        """Save scraping status to file"""
        with open(self.scraping_log_file, 'w', encoding='utf-8') as f:
            json.dump(self.scraping_status, f, indent=2)
    
    def update_channel_status(self, channel: str, date: str, message_count: int, success: bool, error: str = None):
        """Update scraping status for a channel and date"""
        if channel not in self.scraping_status:
            self.scraping_status[channel] = {}
        
        self.scraping_status[channel][date] = {
            'message_count': message_count,
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'error': error
        }
        self.save_scraping_status()
    
    async def download_media(self, message: Message, channel_name: str, date_str: str) -> Optional[str]:
        """Download media from message and return the file path"""
        if not message.media:
            return None
        
        try:
            # Create media directory structure: media/YYYY-MM-DD/channel_name/
            media_channel_dir = self.media_dir / date_str / channel_name
            media_channel_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate unique filename
            file_ext = self.get_media_extension(message.media)
            if not file_ext:
                return None
                
            filename = f"{channel_name}_{message.id}_{int(time.time())}.{file_ext}"
            file_path = media_channel_dir / filename
            
            # Download the media
            await self.client.download_media(message, str(file_path))
            logger.info(f"Downloaded media: {file_path}")
            
            return str(file_path.relative_to(self.media_dir))
            
        except Exception as e:
            logger.error(f"Failed to download media from message {message.id}: {e}")
            return None
    
    def get_media_extension(self, media) -> Optional[str]:
        """Get file extension for media type"""
        if hasattr(media, 'photo'):
            return 'jpg'
        elif hasattr(media, 'document'):
            if hasattr(media.document, 'mime_type'):
                mime_type = media.document.mime_type
                if 'image' in mime_type:
                    return mime_type.split('/')[-1]
                elif 'video' in mime_type:
                    return 'mp4'
        return None
    
    def message_to_dict(self, message: Message, channel_title: str, channel_username: str, media_path: Optional[str] = None) -> Dict:
        """Convert Telegram message to dictionary"""
        return {
            'id': message.id,
            'channel_title': channel_title,
            'channel_username': channel_username,
            'message': message.message,
            'date': message.date.isoformat() if message.date else None,
            'media_path': media_path,
            'views': getattr(message, 'views', None),
            'forwards': getattr(message, 'forwards', None),
            'replies': getattr(message, 'replies', None) if hasattr(message, 'replies') else None,
            'edit_date': message.edit_date.isoformat() if message.edit_date else None,
            'has_media': message.media is not None,
            'media_type': self.get_media_type(message.media) if message.media else None
        }
    
    def get_media_type(self, media) -> Optional[str]:
        """Get media type string"""
        if hasattr(media, 'photo'):
            return 'photo'
        elif hasattr(media, 'document'):
            return 'document'
        elif hasattr(media, 'web_preview'):
            return 'web_preview'
        return None
    
    async def scrape_channel(self, channel_username: str, limit: int = None) -> bool:
        """Scrape messages from a single channel"""
        if limit is None:
            limit = TELEGRAM_CONFIG['message_limit']
            
        try:
            logger.info(f"Starting to scrape channel: {channel_username}")
            
            # Get channel entity
            entity = await self.client.get_entity(channel_username)
            
            # Handle different entity types
            if hasattr(entity, 'title'):
                channel_title = entity.title
            else:
                channel_title = channel_username
                
            channel_name = channel_username.replace('@', '')
            
            logger.info(f"Channel title: {channel_title}")
            
            # Get current date for partitioning
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            # Create partitioned directory
            channel_dir = self.base_data_dir / date_str
            channel_dir.mkdir(parents=True, exist_ok=True)
            
            # Prepare data collection
            messages_data = []
            message_count = 0
            
            # Scrape messages
            async for message in self.client.iter_messages(entity, limit=limit):
                try:
                    # Download media if present
                    media_path = await self.download_media(message, channel_name, date_str)
                    
                    # Convert message to dictionary
                    message_dict = self.message_to_dict(message, channel_title, channel_username, media_path)
                    messages_data.append(message_dict)
                    message_count += 1
                    
                    # Log progress every 10 messages
                    if message_count % 10 == 0:
                        logger.info(f"Scraped {message_count} messages from {channel_username}")
                    
                except Exception as e:
                    logger.error(f"Error processing message {message.id} from {channel_username}: {e}")
                    continue
            
            # Save messages to JSON file
            output_file = channel_dir / f"{channel_name}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'channel_info': {
                        'title': channel_title,
                        'username': channel_username,
                        'scraped_at': datetime.now().isoformat(),
                        'message_count': message_count
                    },
                    'messages': messages_data
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully scraped {message_count} messages from {channel_username}")
            logger.info(f"Data saved to: {output_file}")
            
            # Update scraping status
            self.update_channel_status(channel_username, date_str, message_count, True)
            
            return True
            
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Rate limited for {wait_time} seconds. Waiting...")
            await asyncio.sleep(wait_time)
            return False
            
        except ChannelPrivateError:
            logger.error(f"Channel {channel_username} is private or not accessible")
            self.update_channel_status(channel_username, date_str, 0, False, "Channel is private")
            return False
            
        except ChatAdminRequiredError:
            logger.error(f"Admin access required for channel {channel_username}")
            self.update_channel_status(channel_username, date_str, 0, False, "Admin access required")
            return False
            
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {e}")
            self.update_channel_status(channel_username, date_str, 0, False, str(e))
            return False
    
    async def scrape_all_channels(self):
        """Scrape all configured channels"""
        logger.info("Starting Telegram scraping process")
        
        for channel in self.channels:
            try:
                success = await self.scrape_channel(channel)
                if success:
                    # Add delay between channels to avoid rate limiting
                    await asyncio.sleep(2)
                else:
                    # If failed due to rate limiting, retry after delay
                    await asyncio.sleep(30)
                    
            except Exception as e:
                logger.error(f"Unexpected error scraping {channel}: {e}")
                continue
        
        logger.info("Telegram scraping process completed")

async def main():
    """Main function to run the scraper"""
    scraper = TelegramScraper()
    
    try:
        await scraper.client.start()
        logger.info("Telegram client started successfully")
        
        await scraper.scrape_all_channels()
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    finally:
        if scraper.client.is_connected():
            await scraper.client.disconnect()
        logger.info("Telegram client disconnected")

if __name__ == "__main__":
    asyncio.run(main())