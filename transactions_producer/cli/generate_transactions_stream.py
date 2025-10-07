import argparse
import os

# common packages
from common_config import settings
from common_logger import get_logger

from .generator import generate_tranactions

def main():

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")

    # Instantiate logger
    logger = get_logger("transactions_stream_cli", APP_NAME)

    logger.info(f"Start: {APP_NAME}")

    logger.info(f"ENVVAR:[APP_NAME:{APP_NAME}]")

    if not APP_NAME:
        logger.error("One or more required Environment Variables has not been set up")
        exit(1)

    # Create the parser
    parser = argparse.ArgumentParser(description="Generate a Transactions Stream based on Transaction parametrization.")

    # Add arguments - parametrize Transaction
    parser.add_argument("--min-amount", "-na", type=int, required=True, help="Minimum value of Amount field")
    parser.add_argument("--max-amount", "-xa", type=int, required=True, help="Maximum value of Amount field")
    parser.add_argument("--people-number", "-p", type=int, required=True, help="Number of customers to generate")
    parser.add_argument("--random-dates", "-rd", type=str, required=True, choices=["random", "current"],  help="Generate random date from [start-date, end-date] the or get current date")
    parser.add_argument("--start-date", "-sd", type=str, required=False, help="Start Date for Transacion Date generation")
    parser.add_argument("--end-date", "-ed", type=str, required=False, help="End Date for Transacion Date generation")
    parser.add_argument("--min-generate-lag", "-nl", type=int, required=False, help="Generation waiting time between Transactions (lower bound)")
    parser.add_argument("--max-generate-lag", "-xl", type=int, required=False, help="Generation waiting time between Transactions (upper bound)")
    

    # Parse the arguments
    args = parser.parse_args()
    min_amount = args.min_amount
    max_amount = args.max_amount
    people_number = args.people_number
    random_dates = args.random_dates
    start_date = args.start_date
    end_date = args.end_date
    min_generate_lag = args.min_generate_lag
    max_generate_lag = args.max_generate_lag


    # Log running args & start the app
    logger.info(f"ARGS:[min_amount:{min_amount},max_amount:{max_amount},"
            f"random_dates:{random_dates},start_date:{start_date},end_date:{end_date},"
            f"people_number:{people_number},"
            f"min_generate_lag:{min_generate_lag},max_generate_lag:{min_generate_lag}]"
        )
    
    # Basic args check
    errors = []

    #if min_amount <= 0:
    #    errors.append("Argument [min_amount] must be > 0")

    if max_amount < min_amount:
        errors.append("Argument [max_amount] must be greater than [min_amount]")

    if people_number < 1:
        errors.append("Argument [people_number] must be >= 1")

    if random_dates == "random":
        if start_date > end_date:
            errors.append("Argument [end_date] must be greater than [start_date]")

    if min_generate_lag and max_generate_lag:
        if min_generate_lag > max_generate_lag:
            errors.append("Argument [max_generate_lag] must be greater than [min_generate_lag]")

    if errors:
        [logger.error(e) for e in errors]
        exit(1) 

    generate_tranactions(
        min_amount, max_amount, random_dates, start_date, end_date,
        people_number, min_generate_lag, max_generate_lag)

if __name__ == "__main__":
    main()