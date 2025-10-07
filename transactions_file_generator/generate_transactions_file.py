import argparse
import os
import time
from pathlib import Path

from generator import (
    generator_generate_transactions, generator_write_transactions,
    list_generate_transactions, list_write_transactions,
)

# common packages
from common_config import settings
from common_logger import get_logger


def main():

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")

    # Instantiate logger
    logger = get_logger("main", APP_NAME)

    logger.info(f"Start: {APP_NAME}")

    logger.info(f"ENVVAR:[APP_NAME:{APP_NAME}]")

    if not APP_NAME:
        logger.error("One or more required Environment Variables has not been set up")
        exit(1)


    # Create the parser
    parser = argparse.ArgumentParser(description="Generate a Transactions CSV file with a specific filename and generator mode.")

    # Add arguments - main
    parser.add_argument("--file-name", "-f", type=str, required=True, help="Path to the input file")
    parser.add_argument("--generator-mode", "-g", type=str, required=True, choices=["list", "generator"],
        default="list", help="Mode for the generator (default: balanced)")
    
    # Add arguments - parametrize Transaction
    parser.add_argument("--rows-number", "-r", type=int, required=True, help="Number of rows to generate")
    parser.add_argument("--min-amount", "-na", type=int, required=True, help="Minimum value of Amount field")
    parser.add_argument("--max-amount", "-xa", type=int, required=True, help="Maximum value of Amount field")
    parser.add_argument("--start-date", "-sd", type=str, required=True, help="Start Date for Transacion Date generation")
    parser.add_argument("--end-date", "-ed", type=str, required=True, help="End Date for Transacion Date generation")
    parser.add_argument("--people-number", "-p", type=int, required=True, help="Number of customers to generate")

    # Parse the arguments
    args = parser.parse_args()
    file_name = args.file_name
    generator_mode = args.generator_mode
    rows_number = args.rows_number
    min_amount = args.min_amount
    max_amount = args.max_amount
    start_date = args.start_date
    end_date = args.end_date
    people_number = args.people_number

    # Log running args & start the app
    logger.info(f"ARGS:[file_name:{file_name},generator_mode:{generator_mode},"
            f"rows:{rows_number},min_amount:{min_amount},"
            f"max_amount:{max_amount},people_number:{people_number}"
            f"start_date:{start_date},end_date:{end_date}]"
        )
    
    # Basic args check
    errors = []
    if rows_number < 1:
        errors.append("Argument [rows_number] must be >= 1")

    if min_amount <= 0:
        errors.append("Argument [min_amount] must be > 0")

    if max_amount < min_amount:
        errors.append("Argument [max_amount] must be greater than [min_amount]")

    if people_number < 1:
        errors.append("Argument [people_number] must be >= 1")

    if start_date > end_date:
        errors.append("Argument [end_date] must be greater than [start_date]")

    if errors:
        [logger.error(e) for e in errors]
        exit(1)



    if generator_mode == 'generator':
        start = time.time()

        transactions = generator_generate_transactions(
            rows_number=rows_number,
            min_amount=min_amount,
            max_amount=max_amount,
            start_date=start_date,
            end_date=end_date,
            people_number=people_number,
            #seed=42,
            amount_skewed=True,
            currency_skewed=True,
            status_skewed=True,
            people_skewed=True
        )
    
        # Because generate_transactions returns an iterator,
        # the generation + writing happen simultaneously
        file_name_path = Path(file_name)
        generator_write_transactions(file_name_path, rows_number, transactions)
        end = time.time()

        logger.info(f"Generated and wrote {rows_number} transactions to {file_name} in {end - start:.2f} seconds.")

    elif generator_mode == 'list':
        start = time.time()

        transactions = list_generate_transactions(
            rows_number=rows_number,
            min_amount=min_amount,
            max_amount=max_amount,
            start_date=start_date,
            end_date=end_date,
            people_number=people_number,
            #seed=42,
            amount_skewed=True,
            currency_skewed=True,
            status_skewed=True,
            people_skewed=True
        )
    
        file_name_path = Path(file_name)
        list_write_transactions(file_name_path, rows_number, transactions)
        end = time.time()

        logger.info(f"Generated and wrote {rows_number} transactions to {file_name} in {end - start:.2f} seconds.")

    else:
        logger.warning(f"Unsupported generator_mode {generator_mode}")


if __name__ == "__main__":
    main()


