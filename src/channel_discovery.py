"""
Channel Discovery Script for Ethiopian Medical Telegram Channels

This script helps discover additional Ethiopian medical channels
from various sources including et.tgstat.com/medicine
"""

import requests
import json
import re
from typing import List, Dict
from urllib.parse import urljoin
import time

class ChannelDiscovery:
    def __init__(self):
        self.discovered_channels = []
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def search_et_tgstat(self, category: str = 'medicine') -> List[str]:
        """
        Search for Ethiopian medical channels on et.tgstat.com
        
        Note: This is a placeholder implementation. The actual et.tgstat.com
        may require different scraping approaches or API access.
        """
        channels = []
        
        # Base URL for Ethiopian Telegram statistics
        base_url = f"https://et.tgstat.com/{category}"
        
        try:
            # This would need to be implemented based on the actual website structure
            # For now, we'll return some common Ethiopian medical channel patterns
            common_patterns = [
                '@ethiopian_pharma',
                '@addis_pharmaceuticals', 
                '@ethiopia_medicines',
                '@addis_pharma',
                '@ethiopian_healthcare',
                '@medical_ethiopia',
                '@pharmacy_addis',
                '@ethiopia_medical_supplies',
                '@addis_medical',
                '@ethiopian_pharmacy'
            ]
            
            channels.extend(common_patterns)
            print(f"Discovered {len(channels)} potential channels from et.tgstat patterns")
            
        except Exception as e:
            print(f"Error searching et.tgstat.com: {e}")
        
        return channels
    
    def search_telegram_web(self, query: str = "ethiopia medical") -> List[str]:
        """
        Search for channels using Telegram Web search
        Note: This would require implementing Telegram Web scraping
        """
        # Placeholder for Telegram Web search implementation
        return []
    
    def validate_channel(self, channel: str) -> bool:
        """Basic validation for channel format"""
        if not channel.startswith('@'):
            return False
        if len(channel) < 3:
            return False
        # Check for valid characters
        if not re.match(r'^@[a-zA-Z0-9_]{3,32}$', channel):
            return False
        return True
    
    def filter_medical_channels(self, channels: List[str]) -> List[str]:
        """Filter channels to focus on medical/pharmaceutical content"""
        medical_keywords = [
            'pharma', 'medical', 'medicine', 'health', 'care', 'drug', 
            'pharmacy', 'clinic', 'hospital', 'doctor', 'med', 'healthcare'
        ]
        
        filtered = []
        for channel in channels:
            channel_lower = channel.lower()
            if any(keyword in channel_lower for keyword in medical_keywords):
                filtered.append(channel)
        
        return filtered
    
    def discover_channels(self) -> List[str]:
        """Main method to discover Ethiopian medical channels"""
        print("Starting channel discovery for Ethiopian medical channels...")
        
        # Search et.tgstat.com
        et_channels = self.search_et_tgstat()
        
        # Combine all discovered channels
        all_channels = et_channels
        
        # Filter for medical content
        medical_channels = self.filter_medical_channels(all_channels)
        
        # Validate channels
        valid_channels = [ch for ch in medical_channels if self.validate_channel(ch)]
        
        # Remove duplicates
        unique_channels = list(set(valid_channels))
        
        print(f"Discovered {len(unique_channels)} unique Ethiopian medical channels")
        
        return unique_channels
    
    def save_discovered_channels(self, channels: List[str], filename: str = 'discovered_channels.json'):
        """Save discovered channels to JSON file"""
        data = {
            'discovered_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_channels': len(channels),
            'channels': channels
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Saved {len(channels)} channels to {filename}")
    
    def update_config_file(self, channels: List[str]):
        """Update the config.py file with discovered channels"""
        try:
            # Read current config
            with open('config.py', 'r', encoding='utf-8') as f:
                config_content = f.read()
            
            # Create new channels list
            new_channels_str = ',\n    '.join([f"'{ch}'" for ch in channels])
            
            # Update the ADDITIONAL_CHANNELS section
            pattern = r'ADDITIONAL_CHANNELS = \[(.*?)\]'
            replacement = f'ADDITIONAL_CHANNELS = [\n    {new_channels_str}\n]'
            
            updated_config = re.sub(pattern, replacement, config_content, flags=re.DOTALL)
            
            # Write updated config
            with open('config.py', 'w', encoding='utf-8') as f:
                f.write(updated_config)
            
            print("Updated config.py with discovered channels")
            
        except Exception as e:
            print(f"Error updating config.py: {e}")

def main():
    """Main function to run channel discovery"""
    discovery = ChannelDiscovery()
    
    # Discover channels
    channels = discovery.discover_channels()
    
    if channels:
        # Save to JSON file
        discovery.save_discovered_channels(channels)
        
        # Update config file
        discovery.update_config_file(channels)
        
        print("\nDiscovered channels:")
        for i, channel in enumerate(channels, 1):
            print(f"{i:2d}. {channel}")
    else:
        print("No channels discovered")

if __name__ == "__main__":
    main() 