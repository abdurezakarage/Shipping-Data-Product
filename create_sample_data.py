#!/usr/bin/env python3
"""
Sample Data Generator
Creates sample JSON files for testing the DBT pipeline
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import random

def create_sample_data():
    """Create sample JSON files for testing"""
    
    # Create data directory structure
    data_dir = Path("data/raw/telegram_messages")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Sample channels
    channels = [
        {
            "username": "@CheMed123",
            "title": "CheMed Pharmaceuticals",
            "category": "pharmaceutical"
        },
        {
            "username": "@lobelia4cosmetics",
            "title": "Lobelia Cosmetics",
            "category": "cosmetics"
        },
        {
            "username": "@tikvahpharma",
            "title": "Tikvah Pharmaceuticals",
            "category": "pharmaceutical"
        },
        {
            "username": "@ethiopianpharma",
            "title": "Ethiopian Pharma",
            "category": "pharmaceutical"
        }
    ]
    
    # Generate data for the last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        date_dir = data_dir / date_str
        date_dir.mkdir(exist_ok=True)
        
        for channel in channels:
            # Generate 5-15 messages per channel per day
            num_messages = random.randint(5, 15)
            messages = []
            
            for i in range(num_messages):
                # Generate random message time within the day
                message_time = current_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Sample message content
                message_contents = [
                    "New pharmaceutical products available! Contact us for more information.",
                    "Special offer on cosmetics this week. Don't miss out!",
                    "Health tips: Stay hydrated and maintain a healthy lifestyle.",
                    "New shipment of medicines arrived. Available at all our locations.",
                    "Cosmetic consultation available. Book your appointment today!",
                    "Pharmaceutical products for all your health needs.",
                    "Quality healthcare products at affordable prices.",
                    "Professional medical consultation services available.",
                    "Beauty products for all skin types.",
                    "Health and wellness products for your family."
                ]
                
                # Generate message
                message = {
                    "id": random.randint(1000, 9999),
                    "channel_title": channel["title"],
                    "channel_username": channel["username"],
                    "message": random.choice(message_contents),
                    "date": message_time.isoformat(),
                    "media_path": None,
                    "views": random.randint(50, 500),
                    "forwards": random.randint(0, 20),
                    "replies": random.randint(0, 10),
                    "edit_date": None,
                    "has_media": random.choice([True, False]),
                    "media_type": random.choice(["photo", "document", None]) if random.choice([True, False]) else None
                }
                
                # Add media path if has media
                if message["has_media"] and message["media_type"]:
                    message["media_path"] = f"{date_str}/{channel['username'].replace('@', '')}/media_{message['id']}.jpg"
                
                messages.append(message)
            
            # Create channel info
            channel_info = {
                "title": channel["title"],
                "username": channel["username"],
                "scraped_at": datetime.now().isoformat(),
                "message_count": len(messages)
            }
            
            # Save to JSON file
            output_data = {
                "channel_info": channel_info,
                "messages": messages
            }
            
            filename = f"{channel['username'].replace('@', '')}.json"
            filepath = date_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"Created {len(messages)} messages for {channel['username']} on {date_str}")
        
        current_date += timedelta(days=1)
    
    print(f"\nSample data created successfully!")
    print(f"Total files created: {len(list(data_dir.rglob('*.json')))}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    create_sample_data() 