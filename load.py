import sqlite3
import pandas as pd
import os
import logging
from config import DB_PATH, PROCESSED_DATA_DIR, LOG_FILE, LOG_FORMAT

# Setup logging
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])
logger = logging.getLogger("LoadLayer")

def load_to_sqlite(df, table_name="processed_retail_data"):
    """Saves the DataFrame to a SQLite database."""
    logger.info(f"Connecting to database at {DB_PATH}...")
    try:
        conn = sqlite3.connect(DB_PATH)
        df.sort_values(by='order_purchase_timestamp', inplace=True) # Ensure chronological order
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Data successfully loaded into table '{table_name}'.")
    except Exception as e:
        logger.error(f"Failed to load data to SQLite: {e}")
        raise

def export_to_csv(df, filename="final_retail_data.csv"):
    """Exports the cleaned data to a CSV file."""
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DATA_DIR, filename)
    logger.info(f"Exporting data to CSV at {path}...")
    df.to_csv(path, index=False)
    logger.info("CSV Export completed.")

def load_all(df):
    """Main loading coordinator."""
    logger.info("Starting Loading Phase...")
    load_to_sqlite(df)
    export_to_csv(df)
    logger.info("Loading Phase Completed.")

if __name__ == "__main__":
    pass
