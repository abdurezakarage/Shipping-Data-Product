from telethon import TelegramClient
import csv
import os
import sys
from dotenv import load_dotenv

# Load environment variables once
load_dotenv('.env')

# Get environment variables with better error handling
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Validate that all required environment variables are present
if not api_id or api_id == 'your_api_id_here':
    print("❌ Error: TG_API_ID not found or not set properly in .env file")
    print("Please create a .env file with your Telegram API credentials")
    print("You can copy env_template.txt to .env and fill in your credentials")
    sys.exit(1)

if not api_hash or api_hash == 'your_api_hash_here':
    print("❌ Error: TG_API_HASH not found or not set properly in .env file")
    print("Please create a .env file with your Telegram API credentials")
    print("You can copy env_template.txt to .env and fill in your credentials")
    sys.exit(1)

if not phone or phone == 'your_phone_number_here':
    print("❌ Error: phone not found or not set properly in .env file")
    print("Please create a .env file with your Telegram API credentials")
    print("You can copy env_template.txt to .env and fill in your credentials")
    sys.exit(1)

print("✅ Environment variables loaded successfully")

# Function to scrape data from a single channel
async def scrape_channel(client, channel_username, writer, media_dir):
    entity = await client.get_entity(channel_username)
    channel_title = entity.title  # Extract the channel's title
    async for message in client.iter_messages(entity, limit=100):
        media_path = None
        if message.media and hasattr(message.media, 'photo'):
            # Create a unique filename for the photo
            filename = f"{channel_username}_{message.id}.jpg"
            media_path = os.path.join(media_dir, filename)
            # Download the media to the specified directory if it's a photo
            await client.download_media(message.media, media_path)
        
        # Write the channel title along with other data
        writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])

# Initialize the client once
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    await client.start()
    
    # Create data/raw directory and photos subdirectory
    raw_data_dir = 'data/raw'
    media_dir = os.path.join(raw_data_dir, 'photos')
    os.makedirs(media_dir, exist_ok=True)

    # Open the CSV file in data/raw directory and prepare the writer
    csv_path = os.path.join(raw_data_dir, 'telegram_data.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])  # Include channel title in the header
        
        # List of channels to scrape
        channels = [  
            '@CheMed123',
             '@lobelia4cosmetics',
             '@tikvahpharma'
    
        ]
        
        # Iterate over channels and scrape data into the single CSV file
        for channel in channels:
            await scrape_channel(client, channel, writer, media_dir)
            print(f"Scraped data from {channel}")

with client:
    client.loop.run_until_complete(main())