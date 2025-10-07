import os
import csv
from enum import StrEnum
from itertools import islice
from psycopg2.extras import execute_batch
from connection_pool import db_connection
from tqdm import tqdm

# common packages
from common_config import settings
from common_logger import get_logger
from common_models import Transaction

from db_operations import (
    log_errors_to_db,
    create_loading_process,
    update_loading_process,
    load_rules_from_db,
    log_suspicious_cases_to_db,
    insert_transactions_batch,
    insert_transactions_single
)


class LOADING_MODES(StrEnum):
    ALL_OR_NOTHING = "all_or_nothing"
    LOAD_VALID_REPORT_INVALID = "load_valid_report_invalid"

class PROCESS_STATUS(StrEnum):
    IN_PROGRESS = 'in_progress'
    COMPLETED_NO_ERRORS = 'completed_no_errors'
    COMPLETED_ERRORS_DETECTED = 'completed_errors_detected'
    REJECTED_ERROR_DETECTED = 'rejected_errors_detected'

class PROCESSING_ERROR_TYPE(StrEnum):
    SCHEMA_ERROR = "schema_error"
    INTEGRITY_ERROR = "integrity_error"


# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")
SOURCE_SYSTEM_ID = int(os.getenv("SOURCE_SYSTEM_ID", 1)) # fallback to Id=1 (transactions_batch_loading)

# Instantiate logger
logger = get_logger("csv_loader", APP_NAME)


def _chunked_iterator(iterator, batch_size):
    """Yield (get) successive 'batch_size' chunks from iterator."""
    while True:
        chunk = list(islice(iterator, batch_size))
        if not chunk:
            break
        yield chunk


def _fast_count_lines(file_name: str, buf_size: int = 1024*1024):
    """
    Fast counting lines method by reading the file in large chunks in a binary mode.
    buf_size: number of bytes to read at a time
    """
    lines = 0
    with open(file_name, "rb") as f:  # read in binary mode for speed
        while chunk := f.read(buf_size):
            lines += chunk.count(b"\n")
    return lines


def load_csv(file_name: str, loading_mode: str, batch_size: int):

    # #rows - means 'number of rows'
    total_rows = 0 # total #rows readed from the file
    total_inserted = 0 # #rows successfully inserted into database
    total_skipped = 0 # #rows skipped (not inserted into database) because of pydantic or db schema violation errors
    total_suspicious = 0  # #rows flagged as suspicious according to Validation Rules
    

    # Validation Rules reading and registering
    with db_connection() as c:
        engine = load_rules_from_db(c) # get Validation Rules from DB

    if not engine.rules:
        logger.info("No Validation Rules found in Database. Transactions will automatically be accepted as valid.")
    else:
        for rule in engine.rules:
            logger.info(f"Validation Rule Loaded:{rule.__qualname__.split('.')[0]}") # get the Rule name only


    if loading_mode == LOADING_MODES.ALL_OR_NOTHING:
        
        number_of_lines = _fast_count_lines(file_name)
        invalid_rows = []
        
        # 1st read - schema validation
        with open(file_name, newline="") as f:

            # Initiate a new Loading Process, inserting initial info into DB
            with db_connection() as c:
                load_id, start_time = create_loading_process(c, source_system_id=SOURCE_SYSTEM_ID, source_name=os.path.basename(file_name), mode=loading_mode)

            reader = csv.DictReader(f)

            # Total number of batches calculated by ceiling division.
            # Worth mentioning: header needs to be subtracted, only lines with data are relevant: (number_of_lines - 1)
            number_of_batches = ((number_of_lines - 1) + batch_size - 1) // batch_size

            logger.info(f"File:{file_name}, #lines:{number_of_lines}, #batches:{number_of_batches}, batch size:{batch_size}")
            
            with tqdm(total=number_of_batches, leave=True, dynamic_ncols=True, 
                        desc="Check Transactions", colour="green", unit="batches", ascii=False) as pbar:
            
                # Read input file in batches (with a given batch_size)
                for batch_num, batch in enumerate(_chunked_iterator(reader, batch_size), start=1):
                    
                    logger.info(f"Processing batch [{batch_num}/{number_of_batches}]")

                    for i, row in enumerate(batch, start= 2+(batch_num-1)*batch_size): # go through each row in the given batch
                        total_rows += 1

                        # Model validation (pydantic model) to check 
                        # if the structure and data types are correct
                        try:
                            Transaction(**row)             
                        except Exception as e:
                            row["error"] = str(e)
                            row["error_type"] = PROCESSING_ERROR_TYPE.SCHEMA_ERROR
                            row["line_number"] = i
                            invalid_rows.append(row) # Insert rows that did not pass pydantic model validation
                            total_skipped += 1
                            logger.warning(f"Invalid row {i}, error: {e.errors()[0]['msg']}") # make to show all errors not only 1st

                    pbar.update(1)  # increment progress bar by 1 batch

            # `all_or_nothing` mode, so if there is at least 1 invalid row,
            # the whole file is disregarded.
            if invalid_rows:
                
                with db_connection() as c:
                    log_errors_to_db(c, invalid_rows, load_id, batch_size)
                logger.warning(f"Skipped {len(invalid_rows)} invalid rows (structure/data types) in batch {batch_num}")
                logger.warning(f"File has been rejected. {total_skipped} invalid rows found.")
                
                with db_connection() as c:
                    update_loading_process(c, load_id, total_rows, 0, total_skipped, -1, start_time, PROCESS_STATUS.REJECTED_ERROR_DETECTED)
                
                return # finish after 1st read - errors detected, no proceed to 2nd read

                      
        # 2nd read - integrity validation
        with open(file_name, newline="") as f:
                    
            reader = csv.DictReader(f)

            with tqdm(total=number_of_batches, leave=True, dynamic_ncols=True, 
                        desc="Loading Transactions", colour="green", unit="batches", ascii=False) as pbar:

                try:
                    
                    # Wrapping all DB manipulations arount this DB context manager
                    # Will rollback everything once error occurs (according to `all or nothing` strategy)
                    with db_connection() as c:
                
                        # Read input file in batches (with a given batch_size)
                        for batch_num, batch in enumerate(_chunked_iterator(reader, batch_size), start=1):
                        
                            logger.info(f"Processing batch [{batch_num}/{number_of_batches}]")
                
                            valid_rows = [] # stores (within a batch) all structure & data types valid Transactions
                            error_rows_db = [] # stores (within a batch) all invalid Transaction (database integrity check)
                            suspicious_rows = [] # stores (within a batch) all suspicious (did not pass Validation Rules) Transactions

                            # Business Rule Engine application
                            # Model validation (pydantic model) was already done,
                            # but Tranaction class instance needs to be created for violations check 
                            for row in batch:
                                tx = Transaction(**row)
                                valid_rows.append(tx) # Insert correctly validated (we know they are)
                                violations = engine.apply_rules(tx) # apply Validation Rules against Transactions

                                for v in violations:
                                    suspicious_rows.append({
                                        "transaction_id": tx.transaction_id,
                                        "violation_reason": v["violation_reason"],
                                        "violation_severity": v["violation_severity"]})
                                    total_suspicious += 1
                                    logger.warning(f"Transaction {tx.transaction_id} violation: {v['violation_reason']}, severity: {v['violation_severity']}")
                        
                            if suspicious_rows: # Log any suspicious rows to DB
                                log_suspicious_cases_to_db(c, suspicious_rows, load_id, batch_size)

                            # Try to insert all valid Transaction as a batch insert to DB
                            insert_transactions_batch(c, valid_rows, load_id, batch_size)
                        
                            pbar.update(1)  # increment progress bar by 1 batch

                        # `all_or_nothing` loading mode completed successfully
                        # rows inserted = total rows, no rows rejected
                        update_loading_process(c, load_id, total_rows, total_rows, 0, total_suspicious, start_time, PROCESS_STATUS.COMPLETED_NO_ERRORS)

                except Exception as e:
                    with db_connection() as c2:
                        # `all_or_nothing` loading mode completed failed, at least one error detected
                        # rows inserted = 0 (full rejection of the file), rows rejected = total rows
                        update_loading_process(c2, load_id, total_rows, 0, total_rows, -1, start_time, PROCESS_STATUS.REJECTED_ERROR_DETECTED)
                    return # finish after 2nd read - errors detected


    elif loading_mode == LOADING_MODES.LOAD_VALID_REPORT_INVALID:
        
        number_of_lines = _fast_count_lines(file_name)
        
        with open(file_name, newline="") as f:

            # Initiate a new Loading Process, inserting initial info into DB
            with db_connection() as c:
                load_id, start_time = create_loading_process(c, source_system_id=SOURCE_SYSTEM_ID, source_name=os.path.basename(file_name), mode=loading_mode)

            reader = csv.DictReader(f)
            
            # Total number of batches calculated by ceiling division.
            # Worth mentioning: header needs to be subtracted, only lines with data are relevant: (number_of_lines - 1)
            number_of_batches = ((number_of_lines - 1) + batch_size - 1) // batch_size

            logger.info(f"File:{file_name}, #lines:{number_of_lines}, #batches:{number_of_batches}, batch size:{batch_size}")

            with tqdm(total=number_of_batches, leave=True, dynamic_ncols=True, 
                        desc="Loading Transactions", colour="green", unit="batches", ascii=False) as pbar:
            
                # Read input file in batches (with a given batch_size)
                for batch_num, batch in enumerate(_chunked_iterator(reader, batch_size), start=1):
                    
                    logger.info(f"Processing batch [{batch_num}/{number_of_batches}]")

                    valid_rows = [] # stores (within a batch) all structure & data types valid Transactions
                    error_rows = [] # stores (within a batch) all invalid Transactions (structure & data types errors)
                    error_rows_db = [] # stores (within a batch) all invalid Transaction (database integrity check)
                    suspicious_rows = [] # stores (within a batch) all suspicious (did not pass Validation Rules) Transactions
                    
                    for i, row in enumerate(batch, start= 2+(batch_num-1)*batch_size): # go through each row in the given batch
                        total_rows += 1
                        
                        # Model validation (pydantic model) to check 
                        # if the structure and data types are correct
                        try:
                            tx = Transaction(**row)
                            valid_rows.append(tx) # Insert correctly validated
                        except Exception as e:
                            row["error"] = str(e)
                            row["error_type"] = PROCESSING_ERROR_TYPE.SCHEMA_ERROR
                            row["line_number"] = i
                            error_rows.append(row) # Insert rows that did not pass pydantic model validation
                            total_skipped += 1
                            logger.warning(f"Invalid row {i}, error: {e.errors()[0]['msg']}") # make to show all errors not only 1st

                    if valid_rows:

                        # Business Rule Engine application
                        for tv in valid_rows:
                            violations = engine.apply_rules(tv) # apply Validation Rules against Transactions
                            for v in violations:
                                suspicious_rows.append({
                                    "transaction_id": tv.transaction_id,
                                    "violation_reason": v["violation_reason"],
                                    "violation_severity": v["violation_severity"]})
                                total_suspicious += 1
                                logger.warning(f"Transaction {tv.transaction_id} violation: {v['violation_reason']}, severity: {v['violation_severity']}")
                        if suspicious_rows: # Log any suspicious rows to DB
                            with db_connection() as c:
                                log_suspicious_cases_to_db(c, suspicious_rows, load_id, batch_size)
                        
                        # At first, try to insert all valid Transaction as a batch insert to DB
                        try:
                            with db_connection() as c:
                                insert_transactions_batch(c, valid_rows, load_id, batch_size) # batch insertion
                                total_inserted += len(valid_rows) # If success, increase number of inserted
                        except Exception as batch_error:
                            logger.warning(f"There are invalid rows in batch, error: {str(batch_error)}")
                            
                            # If batch insertion to DB didn't work because of errors,
                            # then try to insert data row by row (as the last resort solution)
                            for i, tx in enumerate(valid_rows, start=1):
                                try:
                                    with db_connection() as c:
                                        insert_transactions_single(c, tx, load_id) # 1 row insertion
                                        total_inserted += 1 # If success, increase by 1
                                except Exception as row_error:
                                    row = tx.model_dump()
                                    row["error"] = str(row_error)
                                    row["error_type"] = PROCESSING_ERROR_TYPE.INTEGRITY_ERROR
                                    row["line_number"] = i
                                    error_rows_db.append(row) # Insert rows that did not pass pydantic model validation
                                    total_skipped += 1
                                    logger.warning(f"Invalid row {i} db single loading, error: {str(row_error)}")

                    pbar.update(1)  # increment progress bar by 1 batch

                    if error_rows:
                        with db_connection() as c:
                            log_errors_to_db(c, error_rows, load_id, batch_size)
                        logger.warning(f"Skipped {len(error_rows)} invalid rows (structure/data types) in batch {batch_num}")

                    if error_rows_db:
                        with db_connection() as c:
                            log_errors_to_db(c, error_rows_db, load_id, batch_size)
                        logger.warning(f"Skipped {len(error_rows_db)} invalid rows (database integrity) in batch {batch_num}")

            # If any rows discarded, loading finishes with success bit errors were detected and those rows are not present in DB,
            # otherwise loading commpleted with no errors
            process_status = PROCESS_STATUS.COMPLETED_ERRORS_DETECTED if total_skipped > 0 else PROCESS_STATUS.COMPLETED_NO_ERRORS
            with db_connection() as c:
                update_loading_process(c, load_id, total_rows, total_inserted, total_skipped, total_suspicious, start_time, process_status)
            