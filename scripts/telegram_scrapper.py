# telegram_scrapper.py

from telethon import TelegramClient
import csv
import os
from dotenv import load_dotenv

class TelegramScraper:
    def __init__(self, api_id, api_hash, phone):
        load_dotenv('.env')
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.client = TelegramClient('scraping_session', self.api_id, self.api_hash)

    async def scrape_channel(self, channel_username, writer, media_dir):
        entity = await self.client.get_entity(channel_username)
        channel_title = entity.title  # Extract the channel's title
        
        async for message in self.client.iter_messages(entity, limit=10000):
            media_path = None
            if message.media and hasattr(message.media, 'photo'):
                # Create a unique filename for the photo
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                # Download the media to the specified directory if it's a photo
                await self.client.download_media(message.media, media_path)
            
            # Write the channel title along with other data
            writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])

    async def main(self, channels):
        await self.client.start()
        
        # Create a directory for media files
        media_dir = 'photos'
        os.makedirs(media_dir, exist_ok=True)

        # Open the CSV file and prepare the writer
        with open('telegram_data.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])  # Include channel title in the header
            
            # Iterate over channels and scrape data into the single CSV file
            for channel in channels:
                await self.scrape_channel(channel, writer, media_dir)
                print(f"Scraped data from {channel}")

    def run(self, channels):
        with self.client:
            self.client.loop.run_until_complete(self.main(channels))