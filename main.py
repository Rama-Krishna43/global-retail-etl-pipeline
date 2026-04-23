import logging
from config import LOG_FILE, LOG_FORMAT
from extract import extract_all
from transform import transform_all
from load import load_all

# Setup root logging
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[
    logging.FileHandler(LOG_FILE),
    logging.StreamHandler()
])
logger = logging.getLogger("ETL_Orchestrator")

def run_pipeline():
    logger.info("=== GLOBAL RETAIL ETL PIPELINE STARTED ===")
    
    try:
        # 1. EXTRACT
        datasets, rates = extract_all()
        
        # 2. TRANSFORM
        master_df = transform_all(datasets, rates)
        
        # 3. LOAD
        load_all(master_df)
        
        logger.info("=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")
        print("\nPipeline finished! Check 'pipeline.log' and 'retail.db'.")
        
    except Exception as e:
        logger.critical(f"Pipeline failed with error: {e}", exc_info=True)
        print(f"\nPipeline failed! See logs for details. Error: {e}")

if __name__ == "__main__":
    run_pipeline()
