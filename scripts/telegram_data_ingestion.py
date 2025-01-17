# telegram_data_ingestion.py

import pandas as pd
from telegram_scrapper import TelegramScraper

class TelegramDataIngestion:
    def __init__(self, api_id, api_hash, phone):
        self.scraper = TelegramScraper(api_id, api_hash, phone)
        
    def ingest_data(self, channels):
        print("Starting data ingestion...")
        self.scraper.run(channels)
        
    def preprocess_data(self):
        # Load data from CSV and preprocess it
        df = pd.read_csv('telegram_data.csv')
        
        processed_data = []
        for index, row in df.iterrows():
            tokens = self.tokenize(row['Message'])
            processed_data.append({
                'text': tokens,
                'timestamp': row['Date'],
                'sender': row['Channel Title']
            })
        
        return processed_data

    @staticmethod
    def tokenize(text):
        # Implement tokenization logic here (consider Amharic specifics)
        return text.split()  # Simple split for demonstration

    @staticmethod
    def save_preprocessed_data(processed_data):
        df = pd.DataFrame(processed_data)
        df.to_csv('processed_telegram_data.csv', index=False)