from pydantic import BaseModel, Field, validator
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional
from enum import StrEnum
import random
import csv
import os
from uuid import uuid4
from pathlib import Path
from tqdm import tqdm

# common packages
from common_config import settings
from common_logger import get_logger
from common_models import Transaction, TransactionStatus, TransactionCurrency



APP_NAME = os.getenv("APP_NAME")
logger = get_logger("transaction_generator", APP_NAME)

class TransactionGenerateMode(StrEnum):
    GENERATOR = 'generator'
    LIST = 'list'



def _random_datetime_between(start_date: str, end_date: str) -> str:
    
    # Create a datetime from string, and make it UTC-aware.
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    
    # 1. Get difference in seconds (delta) between start_dt and end_dt
    # 2. Generate a random number between 0 and delta (from above)
    # 3. Generate a radom datetime (random_dt) with timedelta and generated offset (random_offset)
    # if random.uniform(0, delta) = 0 then random_date = start_date
    # if random.uniform(0, delta) = delta then random_date = end_date
    # if random.uniform(0, delta) = anything between [0, delta] then random_date = any date between [start_date, end_date]
    delta = (end_dt - start_dt).total_seconds()
    random_offset = random.uniform(0, delta)
    random_dt = start_dt + timedelta(seconds=random_offset)
    
    return random_dt.isoformat() # Return as ISO string, including UTC-aware zone


def _skewed_random_amount():
    if random.random() < 0.98: # 95% chance
        return round(random.uniform(1, 10000), 2)
    else: # 2% chance
        return round(random.uniform(10001, 20000), 2)
    
def _skewed_random_status():
    rnd = random.random()
    if rnd < 0.85: # 85% chance
        return TransactionStatus.COMPLETED.value
    elif rnd >= 0.85 and rnd < 0.95: # 10% chance
        return TransactionStatus.PENDING.value
    else: # 5% chance
        return TransactionStatus.FAILED.value

def _skewed_random_currency():
    rnd = random.random()
    if rnd < 0.4: # 40% chance
        return TransactionCurrency.PHP.value
    elif rnd >= 0.40 and rnd < 0.7: # 30% chance
        return TransactionCurrency.HKD.value
    elif rnd >= 0.7 and rnd < 0.9: # 20% chance
        return TransactionCurrency.USD.value
    elif rnd >= 0.9 and rnd < 0.95: # 5% chance
        return TransactionCurrency.GBP.value
    else: # 5% chance
        return TransactionCurrency.EUR.value

def _skewed_random_people():
    rnd = random.random()
    if rnd < 0.1: # 10% chance
        return 1
    elif rnd >= 0.1 and rnd < 0.26: # 16% chance
        return 2
    elif rnd >= 0.26 and rnd < 0.39: # 13% chance
        return 3
    elif rnd >= 0.39 and rnd < 0.48: # 9% chance
        return 4
    elif rnd >= 0.48 and rnd < 0.54: # 6% chance
        return 5
    elif rnd >= 0.54 and rnd < 0.64: # 10% chance
        return 6
    elif rnd >= 0.64 and rnd < 0.72: # 8% chance
        return 7
    elif rnd >= 0.72 and rnd < 0.78: # 6% chance
        return 8
    elif rnd >= 0.78 and rnd < 0.88: # 10% chance
        return 9
    else: # 12% chance
        return 10


def generate_transactions(
        generate_mode: TransactionGenerateMode,
        rows_number: int,
        min_amount: float,
        max_amount: float,
        start_date: str,
        end_date: str,
        people_number: int,
        seed: Optional[int] = None,
        amount_skewed: Optional[bool] = False,
        currency_skewed: Optional[bool] = False,
        status_skewed: Optional[bool] = False,
        people_skewed: Optional[bool] = False
                          ) -> list[Transaction] | Iterator[Transaction]:
    
    if seed is not None:
        random.seed(seed)

    if generate_mode == TransactionGenerateMode.LIST:

        transactions = []

        for i in range(1, rows_number + 1):
            transaction_id = f"tx{i}"
            sender_id = _skewed_random_people() if people_skewed else random.randint(1, people_number)
            receiver_id = random.choice([x for x in range(1,people_number+1) if x != sender_id]) # receiver_id can't be the same as sender_id
            amount = _skewed_random_amount() if amount_skewed else round(random.uniform(min_amount, max_amount),2)
            currency = _skewed_random_currency() if currency_skewed else random.choice(list(TransactionCurrency)).value # cast to list to make iterable
            timestamp = _random_datetime_between(start_date, end_date)
            status = _skewed_random_status() if status_skewed else random.choice(list(TransactionStatus)).value # cast to list to make iterable

            transaction = Transaction(
                transaction_id=transaction_id,
                sender_id=sender_id,
                receiver_id=receiver_id,
                amount=amount,
                currency=currency,
                timestamp=timestamp,
                status=status    
            )

            transactions.append(transaction)

        return transactions


    elif generate_mode == TransactionGenerateMode.GENERATOR:

        for i in range(1, rows_number+1):
            transaction_id = f"tx{i}"
            sender_id = _skewed_random_people() if people_skewed else random.randint(1, people_number)
            receiver_id = random.choice([x for x in range(1,people_number+1) if x != sender_id]) # receiver_id can't be the same as sender_id
            amount = _skewed_random_amount() if amount_skewed else round(random.uniform(min_amount, max_amount),2)
            currency = _skewed_random_currency() if currency_skewed else random.choice(list(TransactionCurrency)).value # cast to list to make iterable
            timestamp = _random_datetime_between(start_date, end_date)
            status = _skewed_random_status() if status_skewed else random.choice(list(TransactionStatus)) # cast to list to make iterable

            yield Transaction(
                transaction_id=transaction_id,
                sender_id=sender_id,
                receiver_id=receiver_id,
                amount=amount,
                currency=currency,
                timestamp=timestamp,
                status=status
            )
        
    else:
        logger.warning(f"Wrong generate_mode parameter {generate_mode}")


def write_transactions(generate_mode: TransactionGenerateMode, file_path: Path, total_rows: int, transactions: list[Transaction]) -> None:

    logger.info(f"Started writing transactions to {file_path}")

    if generate_mode == TransactionGenerateMode.LIST:

        with open(file_path, 'w', newline='') as f:
            fieldnames = Transaction.model_fields.keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for transaction in tqdm(transactions, total=total_rows, desc="Generating Transactions", dynamic_ncols=True, colour="green", unit="tx"):
                row = transaction.model_dump()

                # get timestamp field, make it timezone aware, then convert to ISO format
                row['timestamp'] = transaction.timestamp.replace(tzinfo=timezone.utc).isoformat()
                writer.writerow(row)

        logger.info(f"Finished writing transactions to {file_path}")

    elif generate_mode == TransactionGenerateMode.GENERATOR:

        with file_path.open('w', newline='') as csvfile:
            fieldnames = Transaction.model_fields.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        
            for transaction in tqdm(transactions, total=total_rows, desc="Generating Transactions", dynamic_ncols=True, colour="green", unit="tx"):            
                row = transaction.model_dump()
            
                # get timestamp field, make it timezone aware, then convert to ISO format 
                row['timestamp'] = transaction.timestamp.replace(tzinfo=timezone.utc).isoformat()
                writer.writerow(row)
    
        logger.info(f"Finished writing transactions to {file_path}")

    else:
        logger.warning(f"Wrong generate_mode parameter {generate_mode}")


def list_generate_transactions(
        rows_number: int,
        min_amount: float,
        max_amount: float,
        start_date: str,
        end_date: str,
        people_number: int,
        seed: Optional[int] = None,
        amount_skewed: Optional[bool] = False,
        currency_skewed: Optional[bool] = False,
        status_skewed: Optional[bool] = False,
        people_skewed: Optional[bool] = False
) -> list[Transaction]:
    
    if seed is not None:
        random.seed(seed)

    if min_amount > max_amount:
        raise ValueError("min_amount cannot be greater than max_amount")

    transactions = []

    for i in range(1, rows_number + 1):
        transaction_id = f"tx{i}"
        sender_id = _skewed_random_people() if people_skewed else random.randint(1, people_number)
        receiver_id = random.choice([x for x in range(1,people_number+1) if x != sender_id]) # receiver_id can't be the same as sender_id
        amount = _skewed_random_amount() if amount_skewed else round(random.uniform(min_amount, max_amount),2)
        currency = _skewed_random_currency() if currency_skewed else random.choice(list(TransactionCurrency)).value # cast to list to make iterable
        timestamp = _random_datetime_between(start_date, end_date)
        status = _skewed_random_status() if status_skewed else random.choice(list(TransactionStatus)).value # cast to list to make iterable

        transaction = Transaction(
            transaction_id=transaction_id,
            sender_id=sender_id,
            receiver_id=receiver_id,
            amount=amount,
            currency=currency,
            timestamp=timestamp,
            status=status    
        )

        transactions.append(transaction)

    return transactions


def list_write_transactions(file_path: Path, total_rows: int, transactions: list[Transaction]) -> None:
    
    logger.info(f"Started writing transactions to {file_path}")

    with open(file_path, 'w', newline='') as f:
        fieldnames = Transaction.model_fields.keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for transaction in tqdm(transactions, total=total_rows, desc="Generating Transactions", dynamic_ncols=True, colour="green", unit="tx"):
            row = transaction.model_dump()

            # get timestamp field, make it timezone aware, then convert to ISO format
            row['timestamp'] = transaction.timestamp.replace(tzinfo=timezone.utc).isoformat()
            writer.writerow(row)

    logger.info(f"Finished writing transactions to {file_path}")


def generator_generate_transactions(
        rows_number: int,
        min_amount: float,
        max_amount: float,
        start_date: str,
        end_date: str,
        people_number: int,
        seed: Optional[int] = None,
        amount_skewed: Optional[bool] = False,
        currency_skewed: Optional[bool] = False,
        status_skewed: Optional[bool] = False,
        people_skewed: Optional[bool] = False
) -> Iterator[Transaction]:
    
    if seed is not None:
        random.seed(seed)

    if min_amount > max_amount:
        raise ValueError("min_amount cannot be greater than max_amount")
    
    for i in range(1, rows_number+1):
        transaction_id = f"tx{i}"
        sender_id = _skewed_random_people() if people_skewed else random.randint(1, people_number)
        receiver_id = random.choice([x for x in range(1,people_number+1) if x != sender_id]) # receiver_id can't be the same as sender_id
        amount = _skewed_random_amount() if amount_skewed else round(random.uniform(min_amount, max_amount),2)
        currency = _skewed_random_currency() if currency_skewed else random.choice(list(TransactionCurrency)).value # cast to list to make iterable
        timestamp = _random_datetime_between(start_date, end_date)
        status = _skewed_random_status() if status_skewed else random.choice(list(TransactionStatus)) # cast to list to make iterable

        yield Transaction(
            transaction_id=transaction_id,
            sender_id=sender_id,
            receiver_id=receiver_id,
            amount=amount,
            currency=currency,
            timestamp=timestamp,
            status=status           
        )


def generator_write_transactions(file_path: Path, total_rows: int, transactions: Iterator[Transaction]) -> None:
    
    logger.info(f"Started writing transactions to {file_path}")

    with file_path.open('w', newline='') as csvfile:
        fieldnames = Transaction.model_fields.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for transaction in tqdm(transactions, total=total_rows, desc="Generating Transactions", dynamic_ncols=True, colour="green", unit="tx"):            
            row = transaction.model_dump()
            
            # get timestamp field, make it timezone aware, then convert to ISO format 
            row['timestamp'] = transaction.timestamp.replace(tzinfo=timezone.utc).isoformat()
            writer.writerow(row)
    
    logger.info(f"Finished writing transactions to {file_path}")