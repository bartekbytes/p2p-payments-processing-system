import sys
import os
import argparse
from loader import load_csv


# common packages
from common_config import settings
from common_logger import get_logger

def main():

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")
    SOURCE_SYSTEM_ID = int(os.getenv("SOURCE_SYSTEM_ID", 1)) # fallback to Id=1 (transactions_batch_loading)
    BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
    DB_CONNECTION_URL = os.getenv("DB_CONNECTION_URL")


    # Instantiate logger
    logger = get_logger("main", APP_NAME)

    logger.info(f"Start: {APP_NAME}")
    logger.info(f"ENVVAR:[APP_NAME:{APP_NAME}]")
    logger.info(f"ENVVAR:[SOURCE_SYSTEM_ID:{SOURCE_SYSTEM_ID}]")
    logger.info(f"ENVVAR:[BATCH_SIZE:{BATCH_SIZE}]")
    logger.info(f"ENVVAR:[DB_CONNECTION_URL:{DB_CONNECTION_URL}]")
    
    required_env_vars = [
        APP_NAME,
        SOURCE_SYSTEM_ID,
        BATCH_SIZE,
        DB_CONNECTION_URL
    ]

    if not all(required_env_vars):
        logger.error("One or more required Environment Variables have not been set up")
        exit(1)


    # Create the parser
    parser = argparse.ArgumentParser(description="Transactions CSV Loader")

    # Add arguments
    parser.add_argument("--file-name", "-f", type=str, required=True, help="Path to the csv file")
    parser.add_argument("--loading-mode", "-m", type=str, required=False,
                        choices=["all_or_nothing", "load_valid_report_invalid"],
                        default="load_valid_report_invalid", 
                        help="Loading mode: all_or_nothing or load_valid_report_invalid")
    parser.add_argument("--batch-size", "-b", type=int, required=False,
                        default=BATCH_SIZE,help="Override default Batch Size for this run")


    # Parse the arguments
    args = parser.parse_args()
    file_name = args.file_name
    loading_mode = args.loading_mode
    batch_size = args.batch_size

    # Log running args & start the app
    logger.info(f"ARGS:[file_name:{file_name},loading_mode:{loading_mode},batch_size:{batch_size}]")

    # Basic args check
    errors = []
    if batch_size < 1:
        errors.append("Argument [batch_size] must be >= 1")

    if errors:
        [logger.error(e) for e in errors]
        exit(1)

    try:
        load_csv(file_name=file_name, loading_mode=loading_mode, batch_size=batch_size) # Call the csv loader mechanism
    except FileNotFoundError as e:
        logger.error(f"CSV file not found: {file_name}", exc_info=True)
    finally:
        logger.info(f"End: {APP_NAME}")



if __name__ == "__main__":
    main()
