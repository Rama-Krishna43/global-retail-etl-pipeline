import os

# Base Directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Data Paths
RAW_DATA_DIR = os.path.join(BASE_DIR, "data")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "output")
DB_PATH = os.path.join(BASE_DIR, "retail.db")

# API Configuration
# Using open.er-api.com for free real-time rates (no key needed)
EXCHANGE_RATE_API_URL = "https://open.er-api.com/v6/latest/BRL" 

# Transformation Settings
TARGET_CURRENCY = "INR"

# Logging Configuration
LOG_FILE = os.path.join(BASE_DIR, "pipeline.log")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
