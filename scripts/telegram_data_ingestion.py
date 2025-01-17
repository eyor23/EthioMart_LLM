import pandas as pd
import os
import re
from dotenv import load_dotenv
from telegram_scrapper import TelegramScraper
import asyncio  # Import asyncio for running async functions

# Load environment variables from .env file
load_dotenv()

class TelegramDataIngestion:
    def __init__(self):
        # Create an instance of TelegramScraper without passing parameters
        self.scraper = TelegramScraper()

    async def ingest_data(self, channels):
        print("Starting data ingestion...")
        await self.scraper.run(channels)  # Await the run method

    def preprocess_data(self):
        # Load data from Excel and preprocess it
        df = pd.read_excel(r'C:\Users\Eyor.G\Downloads\Data-20250116T084145Z-001\Data\telegram_data.xlsx')
        
        processed_data = []
        for index, row in df.iterrows():
            message = row['Message']
            # Check if message is a string before tokenizing
            if isinstance(message, str):
                tokens = self.tokenize(message)
                processed_data.append({
                    'text': tokens,
                    'timestamp': row['Date'],
                    'sender': row['Channel Title']
                })
            else:
                print(f"Row {index} has non-string message: {message}. Skipping.")  # Optionally log the issue
        
        return processed_data
    
    @staticmethod
    def is_amharic(text):
        # Check if the text contains Amharic characters
        return bool(re.search(r'[\u1200-\u137F]+', text))  # Amharic Unicode range
    
    @staticmethod
    def tokenize(text):
        # Normalize Amharic text (example logic)
        text = text.lower()
        text = re.sub(r'[^አ-ፈ\s]', '', text)  # Keep only Amharic characters and spaces
        return text.split()  # Tokenization

    @staticmethod
    def tokenize(text):
        # Implement tokenization logic here (consider Amharic specifics)
        return text.split()  # Simple split for demonstration

    @staticmethod
    def save_preprocessed_data(processed_data):
        df = pd.DataFrame(processed_data)
        df.to_csv('processed_telegram_data.csv', index=False)

# Function to run the ingestion process
async def run_ingestion(channels):
    ingestion = TelegramDataIngestion()
    await ingestion.ingest_data(channels)  # Scraping data from channels
    processed_data = ingestion.preprocess_data()  # Preprocessing data
    ingestion.save_preprocessed_data(processed_data)  # Save processed data