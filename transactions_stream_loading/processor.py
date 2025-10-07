import os
import json
from enum import StrEnum
from pydantic import ValidationError
from connection_pool import db_connection
from datetime import datetime, timezone
import time

from kafka import KafkaConsumer, KafkaProducer, errors

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

class PROCESS_STATUS(StrEnum):
    IN_PROGRESS = 'in_progress'
    COMPLETED_NO_ERRORS = 'completed_no_errors'
    COMPLETED_ERRORS_DETECTED = 'completed_errors_detected'
    REJECTED_ERROR_DETECTED = 'rejected_errors_detected'

class LOADING_MODES(StrEnum):
    STREAM_LOADING = "stream_loading"

class PROCESSING_ERROR_TYPE(StrEnum):
    SCHEMA_ERROR = "schema_error"
    INTEGRITY_ERROR = "integrity_error"


MAX_RETRIES = 5 # Number of retries before failure of Producer
RETRY_BACKOFF_BASE = 2  # backoff base (in seconds)
SEND_TIMEOUT = 30  # Send and receive timout from Kafka Broker (in seconds)

RETRYABLE_ERRORS = (
    errors.NoBrokersAvailable, # No brokers responded (cluster down, misconfigured bootstrap server, firewall, DNS issue).
    errors.KafkaTimeoutError, # Request timed out (e.g., network hiccup, broker slow, GC pause).
    errors.RequestTimedOutError, # A specific request to broker timed out.
    errors.NotLeaderForPartitionError, # Broker leadership changed; try again and Kafka will route you correctly.
    errors.LeaderNotAvailableError, # Happens during leader election. Typically temporary.
    errors.KafkaConnectionError, # Sometimes bubbled from underlying network stack
)


# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")
SOURCE_SYSTEM_ID = int(os.getenv("SOURCE_SYSTEM_ID", 2)) # fallback to Id=2 (transactions_stream_loading)
TRANSACTION_DB_BATCH_SIZE = int(os.getenv("TRANSACTION_DB_BATCH_SIZE"))
KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS = int(os.getenv("KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS"))
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS")

 # Instantiate logger
logger = get_logger("transactions_consumer", APP_NAME)



def get_validation_rules():
    # Validation Rules reading and registering
    with db_connection() as c:
        engine = load_rules_from_db(c) # get Validation Rules from DB

    if not engine.rules:
        logger.info("No Validation Rules found in Database. Transactions will automatically be accepted as valid.")
        return False
    else:
        for rule in engine.rules:
            logger.info(f"Validation Rule Loaded:{rule.__qualname__.split('.')[0]}") # get the Rule name only

    return engine



def _create_producer(bootstrap_servers, retries: int = 5):

    logger.info(f"Trying to connect to Kafka server(s)")
    
    for attempt in range(1, retries+1):
        try:

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                linger_ms=10,
                acks='all',
                max_block_ms=10000,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            logger.info("Connection to Kafka has been established")
            return producer
        
        except RETRYABLE_ERRORS as e: # Kafka cluster is down, misconfiguration, firewall, DNS issue
            wait_time = (RETRY_BACKOFF_BASE ** attempt)  # exponential backoff
            logger.warning(f"Producer attempt {attempt} failed with error: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except errors.KafkaError as e:
            # the rest, non-retryable errors, like
            # errors.TopicAuthorizationFailedError # Wrong ACLs/permissions. Needs operator fix.
            # errors.GroupAuthorizationFailedError # Consumer group not allowed.
            # errors.UnknownTopicOrPartitionError # Topic doesn’t exist. You must create it.
            # errors.InvalidTopicError # Malformed topic name.
            # errors.RecordTooLargeError # Message exceeds broker limit. Must fix producer config or data.
            logger.error(f"Fatal Kafka error: {e}")
            raise
    raise RuntimeError("Could not connect to Kafka after several retries.")


def _send_transaction(producer, topic, message, tx_id=None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
        
            future = producer.send(topic, value=message)
            future.get(timeout=SEND_TIMEOUT)
            logger.info(f"Message sent successfully (tx_id={tx_id})")
            return True
        
        except RETRYABLE_ERRORS as e:
            logger.warning(f"[Retry {attempt}] Kafka send failed (Retryable Error): {e} (tx_id={tx_id})")
            time.sleep(RETRY_BACKOFF_BASE ** attempt)  # Exponential backoff
            logger.debug(f"Retry {attempt}, waiting {RETRY_BACKOFF_BASE ** attempt}s")
        except KafkaError as e:
            logger.warning(f"[Retry {attempt}] Kafka send failed (Other Kafka Error): {e} (tx_id={tx_id})")
            time.sleep(RETRY_BACKOFF_BASE ** attempt)  # Exponential backoff
            logger.debug(f"Retry {attempt}, waiting {RETRY_BACKOFF_BASE ** attempt}s")
        except Exception as e:
            logger.error(f"[Retry {attempt}] Unexpected send error: {e} (tx_id={tx_id})")
            time.sleep(RETRY_BACKOFF_BASE ** attempt)
            logger.debug(f"Retry {attempt}, waiting {RETRY_BACKOFF_BASE ** attempt}s")
    
    # All retries failed — log to audit/DB/DLQ
    logger.critical(f"Kafka send failed permanently after {MAX_RETRIES} attempts (tx_id={tx_id})")
    # Critical error happened (Kafka is unrecheable, message can't be sent, etc...),
    # but we don't want to loose any information about unsent messages
    # so the idea is to log this message to a Persistence Layer for further investigation/not loosing message
    # Stub:
    # log_unsent_message_to_audit_table(message, topic, reason=str(e), id=tx_id)
    # Then we can raise RuntimeError
    raise RuntimeError(f"Kafka send failure, unrecoverable after {MAX_RETRIES} retries.")


def create_consumer(bootstrap_servers, retries: int = 5):

    logger.info(f"Trying connect to Kafka broker(s)")

    for attempt in range(1, retries+1):
        try:
            
            consumer = KafkaConsumer(
                KAFKA_TOPIC_TRANSACTIONS,
                bootstrap_servers=bootstrap_servers,
                enable_auto_commit=False, # avoiding data loss in crash scenarios
                auto_offset_reset='latest', # for testing, I put 'latest', for PROD-like better to put 'none' for better control
                group_id='transaction_consumer_group',
                value_deserializer=lambda v: v.decode('utf-8')
            )
            
            logger.info("Connection to  Kafka broker(s) has been made")
            return consumer

        except RETRYABLE_ERRORS as e: # Kafka cluster is down, misconfiguration, firewall, DNS issue
            wait_time = (RETRY_BACKOFF_BASE ** attempt)  # exponential backoff
            logger.warning(f"Consumer attempt {attempt} failed with error: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
        except errors.KafkaError as e:
            # the rest, non-retryable errors, like
            # errors.TopicAuthorizationFailedError # Wrong ACLs/permissions. Needs operator fix.
            # errors.GroupAuthorizationFailedError # Consumer group not allowed.
            # errors.UnknownTopicOrPartitionError # Topic doesn’t exist. You must create it.
            # errors.InvalidTopicError # Malformed topic name.
            # errors.RecordTooLargeError # Message exceeds broker limit. Must fix producer config or data.
            logger.error(f"Fatal Kafka error: {e}")
            raise
    raise RuntimeError("Could not connect to Kafka after several retries.")




class TransactionProcessor:
    def __init__(self, rules_engine):
        self.rules_engine = rules_engine
        self.transaction_batch = []
        self.suspicious_rows = []
        self.error_rows = [] # stores (within a batch) all invalid Transactions (structure & data types errors)
        self.error_rows_db = [] # stores (within a batch) all invalid Transaction (database integrity check)
        self.last_flush_time = time.time()
        self.single_inserted_inserted = 0
        self.transactions_dlq_producer = None


    def create_transactions_dlq_producer(self, bootstrap_servers):
        transactions_dlq_producer  = _create_producer(bootstrap_servers=bootstrap_servers, retries=MAX_RETRIES)
        self.transactions_dlq_producer = transactions_dlq_producer

    def _send_to_dlq(self, topic, message, error, partition=None, offset=None):
        
        # Payload for DLQ
        dlq_message = {
            "original_topic": topic,
            "original_partition": partition,
            "original_offset": offset,
            "message": message.value.decode('utf-8') if isinstance(message.value, bytes) else str(message.value),
            "error": str(error),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        dlq_topic = f"{topic}.DLQ"
        
        try:
            #self.transactions_dlq_producer.send(dlq_topic, dlq_message)
            success = _send_transaction(self.transactions_dlq_producer, dlq_topic, dlq_message, tx_id=dlq_message.get("transaction_id"))
            
            if success:
                    logger.info(f"Message sent to Transactions DLQ: {dlq_topic}")
            else:
                    # handling logging of unsent message inside _send_transaction()
                    logger.warning(f"Message not sent to Transactions DLQ (logged to audit): {dlq_message}")

            self.transactions_dlq_producer.flush()
            
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")


    def process_message(self, message) -> bool:

        logger.info(f"Processing message from [{KAFKA_TOPIC_TRANSACTIONS}] topic")
        
        # Model validation (pydantic model) to check 
        # if the structure and data types are correct
        # Also JSON validation, as message could be delivered corrupted
        try:
            data = json.loads(message.value)
            tx = Transaction(**data)
            self.transaction_batch.append(tx)
            logger.debug(f"Message {tx.transaction_id} added to transaction_batch, size: {len(self.transaction_batch)}")

            violations = self.rules_engine.apply_rules(tx) # apply Validation Rules against Transaction
            for v in violations:
                self.suspicious_rows.append({
                    "transaction_id": tx.transaction_id,
                    "violation_reason": v["violation_reason"],
                    "violation_severity": v["violation_severity"]})
                logger.warning(f"Transaction {tx.transaction_id} violation: {v['violation_reason']}, severity: {v['violation_severity']}")
            
        except Exception as e:
            data["error"] = str(e)
            data["error_type"] = PROCESSING_ERROR_TYPE.SCHEMA_ERROR
            data["line_number"] = -1
            self.error_rows.append(data) # Insert rows that did not pass schema validation (pydantic model validation)

            # send this message to Transaction DLQ (didn't pass schema validation),
            # so that it can be used in any downstream processes aimed at capturing data quality (DQ) issues
            self._send_to_dlq(message.topic, message, e, message.partition, message.offset)

            # and log 
            logger.warning(f"Invalid data, error: {e.errors()[0]['msg']}") # make to show all errors not only 1st

        try:
            
            # conditions for starting DB batch loading
            flush_time = time.time()
            
            # length only of approved messages or approved+rejected?
            batch_is_full = len(self.transaction_batch) >= TRANSACTION_DB_BATCH_SIZE
            timeout_raised = (flush_time - self.last_flush_time) > KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS

            if batch_is_full or timeout_raised:
                if batch_is_full:
                    logger.debug(f"Flushing batch because batch size {TRANSACTION_DB_BATCH_SIZE} reached")
                if timeout_raised:
                    logger.debug(f"Flushing batch because flush interval {KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS} timed out")


                self.last_flush_time = time.time()

                # Initiate a new Loading Process, inserting initial info into DB
                with db_connection() as c:
                    load_id, start_time = create_loading_process(c, source_system_id=SOURCE_SYSTEM_ID, source_name=f"{message.topic}-{message.partition}-{message.offset}", mode=LOADING_MODES.STREAM_LOADING)
                

                logger.debug(f"Batch Loading start")
                
                try:
                    with db_connection() as c:
                        insert_transactions_batch(c, self.transaction_batch, load_id, TRANSACTION_DB_BATCH_SIZE) # batch insertion
                except Exception as batch_error:
                    logger.warning(f"There are invalid rows in batch, error: {str(batch_error)}")
                    self.single_inserted_inserted = 0
                    
                    # If batch insertion to DB didn't work because of errors,
                    # then try to insert data row by row (as the last resort solution)
                    for i, tx in enumerate(self.transaction_batch, start=1):
                        try:
                            with db_connection() as c:
                                insert_transactions_single(c, tx, load_id) # 1 row insertion
                                self.single_inserted_inserted += 1 # If success, increase by 1
                        except Exception as row_error:
                            row = tx.model_dump()
                            row["error"] = str(row_error)
                            row["error_type"] = PROCESSING_ERROR_TYPE.INTEGRITY_ERROR
                            row["line_number"] = i
                            self.error_rows_db.append(row) # Insert rows that didn't pass DB integrity validation
                            
                            # send this message to Transaction DLQ (didn't pass DB integrity validation),
                            # so that it can be used in any downstream processes aimed at capturing data quality (DQ) issues
                            self._send_to_dlq(message.topic, message, row_error, message.partition, message.offset)

                            logger.warning(f"Invalid row {i} db single loading, error: {str(row_error)}")


                if self.suspicious_rows: # Log any suspicious rows to DB
                    with db_connection() as c:
                        log_suspicious_cases_to_db(c, self.suspicious_rows, load_id, TRANSACTION_DB_BATCH_SIZE)

                if self.error_rows: # Log any schema errors rows to DB
                    with db_connection() as c:
                        log_errors_to_db(c, self.error_rows, load_id, TRANSACTION_DB_BATCH_SIZE)
                    logger.warning(f"Skipped {len(self.error_rows)} invalid rows (structure/data types)")

                if self.error_rows_db: # Log any schema errors rows to DB
                    with db_connection() as c:
                        log_errors_to_db(c, self.error_rows_db, load_id, TRANSACTION_DB_BATCH_SIZE)
                    logger.warning(f"Skipped {len(self.error_rows_db)} invalid rows (database integrity)")

                if self.single_inserted_inserted > 0:
                    total_rows = self.single_inserted_inserted + len(self.error_rows) + len(self.error_rows_db)
                    inserted_rows = self.single_inserted_inserted
                else:
                    total_rows = len(self.transaction_batch) + len(self.error_rows) + len(self.error_rows_db) # error_rows_db will be 0
                    inserted_rows = len(self.transaction_batch)
                process_status = PROCESS_STATUS.COMPLETED_ERRORS_DETECTED if len(self.error_rows) > 0 or len(self.error_rows_db) > 0 else PROCESS_STATUS.COMPLETED_NO_ERRORS
                with db_connection() as c:
                    update_loading_process(c, load_id, total_rows, inserted_rows, len(self.error_rows) + len(self.error_rows_db) , len(self.suspicious_rows), start_time, process_status)
                
                logger.info(f"Inserted transactions {len(self.transaction_batch)}")
                
                # cleanup of lists
                self.error_rows_db.clear()
                self.error_rows.clear()
                self.suspicious_rows.clear()
                self.transaction_batch.clear()
                
                logger.debug(f"Batch Loading finished")
                    
        except Exception as e:
            logger.error(f"Unexpected error: {e} | transaction: {tx.transaction_id} {tx}")

