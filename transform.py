import pandas as pd
import logging
from config import TARGET_CURRENCY, LOG_FORMAT, LOG_FILE

# Setup logging
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])
logger = logging.getLogger("TransformLayer")

def validate_data(df):
    """
    Performs high-ROI data quality validation.
    This demonstrates data reliability thinking for technical interviews.
    """
    logger.info("--- Data Quality Check Starting ---")
    
    # 1. Null Checks
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Found missing values:\n{null_counts[null_counts > 0]}")
        # Option: df = df.dropna(subset=['critical_column'])
    
    # 2. Duplicate Checks
    dupes = df.duplicated().sum()
    if dupes > 0:
        logger.warning(f"Found {dupes} duplicate rows. Dropping...")
        df = df.drop_duplicates()
        
    # 3. Business Logic Validation (Invalid Prices)
    invalid_prices = df[df['price'] <= 0]
    if not invalid_prices.empty:
        logger.error(f"Found {len(invalid_prices)} rows with invalid prices (<= 0).")
        # Removing invalid data to ensure downstream analytics are clean
        df = df[df['price'] > 0]
    
    logger.info("--- Data Quality Check Completed ---")
    return df

def clean_data(df_dict):
    """Standardizes formats and removes duplicates."""
    logger.info("Cleaning datasets...")
    
    # Standardize types and remove duplicates
    df_dict['orders']['order_purchase_timestamp'] = pd.to_datetime(df_dict['orders']['order_purchase_timestamp'])
    
    for key, df in df_dict.items():
        original_count = len(df)
        df_dict[key] = df.drop_duplicates()
        dropped = original_count - len(df_dict[key])
        if dropped > 0:
            logger.info(f"Dropped {dropped} duplicates from {key}.")
            
    return df_dict

def integrate_data(df_dict):
    """Merges datasets into a single master table."""
    logger.info("Integrating datasets (Merging)...")
    
    # Start with Order Items
    master_df = df_dict['order_items']
    
    # Join Orders
    master_df = master_df.merge(df_dict['orders'], on='order_id', how='left')
    
    # Join Products
    master_df = master_df.merge(df_dict['products'], on='product_id', how='left')
    
    # Join Customers
    master_df = master_df.merge(df_dict['customers'], on='customer_id', how='left')
    
    logger.info(f"Integration complete. Master table size: {master_df.shape}")
    return master_df

def normalize_currency(df, rates):
    """Converts price and freight to target currency."""
    rate = rates.get(TARGET_CURRENCY, 1.0)
    logger.info(f"Normalizing currency to {TARGET_CURRENCY} using rate: {rate}")
    
    df[f'price_{TARGET_CURRENCY.lower()}'] = round(df['price'] * rate, 2)
    df[f'freight_{TARGET_CURRENCY.lower()}'] = round(df['freight_value'] * rate, 2)
    df[f'total_price_{TARGET_CURRENCY.lower()}'] = df[f'price_{TARGET_CURRENCY.lower()}'] + df[f'freight_{TARGET_CURRENCY.lower()}']
    
    return df

def transform_all(datasets, rates):
    """Main transformation coordinator."""
    logger.info("Starting Transformation Phase...")
    
    cleaned_datasets = clean_data(datasets)
    master_df = integrate_data(cleaned_datasets)
    master_df = validate_data(master_df)
    master_df = normalize_currency(master_df, rates)
    
    # Basic Feature Engineering
    master_df['order_month'] = master_df['order_purchase_timestamp'].dt.to_period('M').astype(str)
    
    logger.info("Transformation Phase Completed.")
    return master_df

if __name__ == "__main__":
    # Test would go here
    pass
