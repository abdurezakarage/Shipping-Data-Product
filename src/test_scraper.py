"""
Test script for Telegram scraper functionality
"""

import asyncio
import os
import sys
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent))

from telegram_scraper import TelegramScraper
from config import TELEGRAM_CONFIG

async def test_scraper():
    """Test the scraper with a single channel and limited messages"""
    print("Testing Telegram scraper...")
    
    # Check environment variables
    api_id = os.getenv('TG_API_ID')
    api_hash = os.getenv('TG_API_HASH')
    phone = os.getenv('phone')
    
    if not all([api_id, api_hash, phone]):
        print("❌ Missing environment variables. Please set TG_API_ID, TG_API_HASH, and phone in .env file")
        return False
    
    print("✅ Environment variables found")
    
    try:
        # Create scraper instance
        scraper = TelegramScraper()
        print("✅ Scraper instance created")
        
        # Test with a single channel and limited messages
        test_channel = '@CheMed123'  # Use one of the primary channels
        test_limit = 5  # Very small limit for testing
        
        print(f"Testing with channel: {test_channel} (limit: {test_limit} messages)")
        
        # Start client
        await scraper.client.start()
        print("✅ Telegram client started")
        
        # Test scraping
        success = await scraper.scrape_channel(test_channel, limit=test_limit)
        
        if success:
            print("✅ Test scraping completed successfully")
            
            # Check if files were created
            date_str = scraper.scraping_status.get(test_channel, {}).get('2024-01-15', {}).get('message_count', 0)
            if date_str > 0:
                print(f"✅ Scraped {date_str} messages")
            else:
                print("⚠️  No messages found or status not updated")
                
        else:
            print("❌ Test scraping failed")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False
    finally:
        if 'scraper' in locals() and scraper.client.is_connected():
            await scraper.client.disconnect()
            print("✅ Client disconnected")
    
    return True

async def test_config():
    """Test configuration loading"""
    print("\nTesting configuration...")
    
    try:
        from config import ALL_CHANNELS, TELEGRAM_CONFIG, DATA_CONFIG
        
        print(f"✅ Loaded {len(ALL_CHANNELS)} channels from config")
        print(f"✅ Message limit: {TELEGRAM_CONFIG['message_limit']}")
        print(f"✅ Data directory: {DATA_CONFIG['base_data_dir']}")
        
        return True
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

async def main():
    """Main test function"""
    print("🧪 Running Telegram Scraper Tests\n")
    
    # Test configuration
    config_ok = await test_config()
    
    # Test scraper
    scraper_ok = await test_scraper()
    
    print("\n" + "="*50)
    if config_ok and scraper_ok:
        print("🎉 All tests passed! The scraper is ready to use.")
        print("\nNext steps:")
        print("1. Run: python telegram_scraper.py")
        print("2. Check the generated data in data/raw/")
        print("3. Monitor logs in telegram_scraper.log")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        if not config_ok:
            print("- Configuration issues detected")
        if not scraper_ok:
            print("- Scraper functionality issues detected")

if __name__ == "__main__":
    asyncio.run(main()) 