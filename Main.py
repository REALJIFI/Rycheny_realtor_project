
import pandas as pd
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

import logging
from Load import get_db_connection
from Load import (
    insert_location_dim,
    insert_sales_dim,
    insert_features_dim,
    insert_property_fact_table
)

from Extract import fetch_real_estate_data, save_data_to_json, load_data_to_dataframe
from Transform import clean_real_estate_data
from Helpers import create_location_dim, create_sales_dim, create_features_dim, create_property_fact

import os

# Configure logger
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def extract(api_key: str, json_file: str = 'real_estate.json') -> pd.DataFrame:
    """
    Extract data from API and save it to JSON, then load to DataFrame.
    """
    logger.info("Starting data extraction from API.")
    data = fetch_real_estate_data(api_key=api_key)
    if not data:
        logger.error("No data fetched from API.")
        return pd.DataFrame()
    save_data_to_json(data, filename=json_file)
    df = load_data_to_dataframe(filename=json_file)
    logger.info(f"Data extracted and saved to {json_file}.")
    return df

def transform(df: pd.DataFrame):
    """
    Transform the raw DataFrame to cleaned and dimension/fact tables.
    """
    logger.info("Starting data transformation.")
    clean_df = clean_real_estate_data(df)
    location_dim = create_location_dim(clean_df)
    sales_dim = create_sales_dim(clean_df)
    features_dim = create_features_dim(clean_df)
    property_fact_table = create_property_fact(clean_df, sales_dim, location_dim, features_dim)
    logger.info("Data transformation complete.")
    return location_dim, sales_dim, features_dim, property_fact_table

def load(location_dim, sales_dim, features_dim, property_fact_table):
    """
    Load all tables into the database using provided insert functions.
    """
    logger.info("Starting data load to database.")
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_location_dim(cursor, location_dim)
        insert_sales_dim(cursor, sales_dim)
        insert_features_dim(cursor, features_dim)
        insert_property_fact_table(cursor, property_fact_table)

        conn.commit()
        logger.info("Data loaded to database successfully.")
    except Exception as e:
        logger.error(f"Error loading data to database: {e}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    API_KEY = os.getenv("api_key")
    
    try:
        logger.info("Starting ETL pipeline...")
        
        # Extract
        df = extract(API_KEY)
        if df.empty:
            logger.error("Extraction failed or returned empty dataset. Exiting ETL.")
            return
        
        # Transform
        location_dim, sales_dim, features_dim, property_fact_table = transform(df)
        
        # Load
        load(location_dim, sales_dim, features_dim, property_fact_table)

        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.exception(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()




