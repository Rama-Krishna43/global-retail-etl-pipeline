import pandas as pd
import requests
import os
import logging
from config import RAW_DATA_DIR, EXCHANGE_RATE_API_URL, LOG_FILE, LOG_FORMAT

# Setup logging
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])
logger = logging.getLogger("ExtractLayer")

def load_csv_data():
    """Loads all Olist CSV files into a dictionary of DataFrames."""
    datasets = {}
    files = {
        'customers': 'olist_customers_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv'
    }
    
    for key, filename in files.items():
        path = os.path.join(RAW_DATA_DIR, filename)
        if os.path.exists(path):
            logger.info(f"Loading {filename}...")
            datasets[key] = pd.read_csv(path)
        else:
            logger.error(f"File not found: {path}")
            raise FileNotFoundError(f"Missing essential file: {filename}")
            
    return datasets

def fetch_exchange_rates():
    """Fetches real-time exchange rates from the API."""
    logger.info("Fetching exchange rates from API...")
    try:
        response = requests.get(EXCHANGE_RATE_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        rates = data.get('rates', {})
        logger.info("Successfully fetched exchange rates.")
        return rates
    except Exception as e:
        logger.warning(f"API Fetch failed: {e}. Falling back to default rate (1 BRL = 16.5 INR).")
        # Fallback rates in case of network issues
        return {"INR": 16.5, "USD": 0.20}

def extract_all():
    """Main extraction coordinator."""
    logger.info("Starting Extraction Phase...")
    datasets = load_csv_data()
    rates = fetch_exchange_rates()
    logger.info("Extraction Phase Completed.")
    return datasets, rates

if __name__ == "__main__":
    extract_all()
