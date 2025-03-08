"""
Collibra Bulk Exporter to PostgreSQL

This is the main entry point for the Collibra Bulk Exporter application.
It processes asset types from Collibra and exports them to PostgreSQL.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from collibra_exporter.utils.common import setup_logging, PerformanceLogger
from collibra_exporter.api.asset_types import get_asset_type_name
from collibra_exporter.api.processor import process_data, flatten_json
from collibra_exporter.db.postgres import save_to_postgres

def process_asset_type(asset_type_id):
    """
    Process a single asset type from Collibra and save it to PostgreSQL.
    
    Args:
        asset_type_id (str): The ID of the asset type to process
        
    Returns:
        float: The time taken to process the asset type in seconds
    """
    start_time = time.time()
    asset_type_name = get_asset_type_name(asset_type_id)
    logging.info(f"Processing asset type: {asset_type_name}")

    all_assets = process_data(asset_type_id)

    if all_assets:
        flattened_assets = [flatten_json(asset, asset_type_name) for asset in all_assets]
        save_to_postgres(asset_type_name, flattened_assets)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logging.info(f"Time taken to process {asset_type_name}: {elapsed_time:.2f} seconds")
        return elapsed_time
    else:
        logging.critical(f"No data to save for {asset_type_name}")
        return 0

def main():
    """
    Main execution function with improved error handling and logging.
    
    This function:
    1. Sets up logging
    2. Loads asset type IDs from configuration
    3. Processes each asset type in parallel using ThreadPoolExecutor
    4. Logs summary statistics
    """
    # Initialize logging
    setup_logging()
    
    with PerformanceLogger("main_execution"):
        try:
            total_start_time = time.time()
            success_count = 0
            error_count = 0
            
            logging.info("Starting Collibra Bulk Export process")
            
            # Load asset type IDs from configuration
            try:
                with open('Collibra_Asset_Type_Id_Manager.json', 'r', encoding='utf-8') as file:
                    data = json.load(file)
                
                asset_type_ids = data['ids']
                logging.info(f"Processing {len(asset_type_ids)} asset types")
            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                logging.critical(f"Error loading asset type IDs: {e}")
                return
            
            # Process asset types in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_asset = {
                    executor.submit(process_asset_type, asset_type_id): asset_type_id 
                    for asset_type_id in asset_type_ids
                }
                
                for future in as_completed(future_to_asset):
                    asset_type_id = future_to_asset[future]
                    try:
                        elapsed_time = future.result()
                        if elapsed_time:
                            success_count += 1
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Error processing asset type {asset_type_id}: {str(e)}")
            
            # Log summary statistics
            total_time = time.time() - total_start_time
            logging.info(f"""
Export Summary:
--------------
Total asset types: {len(asset_type_ids)}
Successful: {success_count}
Failed: {error_count}
Total time: {total_time:.2f} seconds
            """)
            
        except Exception as e:
            logging.critical(f"Critical error in main execution: {str(e)}")
            raise

if __name__ == "__main__":
    main()
