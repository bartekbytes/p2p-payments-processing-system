import os
import json
import time
from typing import Any
from kafka import KafkaProducer, errors
from kafka.errors import KafkaError

# common packages
from common_config import settings
from common_logger import get_logger



MAX_RETRIES = 5 # Number of retries before failure of Producer
RETRY_BACKOFF_BASE = 2  # backoff base (in seconds)
SEND_TIMEOUT = 30  # Send and receive timout from Kafka Broker (in seconds)


# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")

# Instantiate logger
logger = get_logger("transactions_stream_api", APP_NAME)


RETRYABLE_ERRORS = (
    errors.NoBrokersAvailable, # No brokers responded (cluster down, misconfigured bootstrap server, firewall, DNS issue).
    errors.KafkaTimeoutError, # Request timed out (e.g., network hiccup, broker slow, GC pause).
    errors.RequestTimedOutError, # A specific request to broker timed out.
    errors.NotLeaderForPartitionError, # Broker leadership changed; try again and Kafka will route you correctly.
    errors.LeaderNotAvailableError, # Happens during leader election. Typically temporary.
    errors.KafkaConnectionError, # Sometimes bubbled from underlying network stack
)



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
            wait_time = (RETRY_BACKOFF_BASE ** attempt) # # Exponential backoff
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



def generate_transaction(tx: dict[str, Any]):

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
    KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS")

    # Instantiate logger
    logger = get_logger("transactions_stream_api", APP_NAME)

    brokers = os.getenv("KAFKA_BROKERS").split(",")  # split into list

    logger.info(f"Start: {APP_NAME}, Kafka brokers:{brokers}")

    producer = _create_producer(bootstrap_servers=brokers, retries=MAX_RETRIES)

    try:
        
        logger.info(f"Starting {KAFKA_TOPIC_TRANSACTIONS} producer...")

        success = _send_transaction(producer, KAFKA_TOPIC_TRANSACTIONS, tx, tx_id=tx.get("transaction_id"))

        if success:
                logger.info(f"Produced: {tx}")
        else:
                logger.warning(f"Produced with error (logged to audit): {tx}")
    
    except KeyboardInterrupt:
        logger.info(f"Stoping {KAFKA_TOPIC_TRANSACTIONS} producer... (requested by user)")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.debug(f"{KAFKA_TOPIC_TRANSACTIONS} flushing producer before exit...")
        producer.flush()
        producer.close()
        logger.info(f"{KAFKA_TOPIC_TRANSACTIONS} producer stopped")

