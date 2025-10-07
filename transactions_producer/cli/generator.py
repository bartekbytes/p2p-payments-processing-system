import os
import json
import time
import random
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from typing import Any, Optional
from kafka import KafkaProducer, errors
from kafka.errors import KafkaError
from common_models import TransactionStatus, TransactionCurrency

# common packages
from common_config import settings
from common_logger import get_logger



MAX_RETRIES = 5 # Number of retries before failure of Producer
RETRY_BACKOFF_BASE = 2  # backoff base (in seconds)
SEND_TIMEOUT = 30  # Send and receive timout from Kafka Broker (in seconds)

# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")

# Instantiate logger
logger = get_logger("transactions_stream_cli", APP_NAME)


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
    if random.random() < 0.95:
        return round(random.uniform(1, 18000), 2)
    else:
        return round(random.uniform(18001, 20000), 2)


def _skewed_random_status():
    rnd = random.random()
    if rnd < 0.8: # 80% chance
        return TransactionStatus.COMPLETED.value
    elif rnd >= 0.8 and rnd < 0.95: # 15% chance
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


def _generate_transaction(
        min_amount: int,
        max_amount: int,
        random_dates: bool,
        start_date: str,
        end_date: str,
        people_number: int,
        amount_skewed: Optional[bool] = False,
        currency_skewed: Optional[bool] = False,
        status_skewed: Optional[bool] = False,
        people_skewed: Optional[bool] = False
    ) -> dict[str, Any]:
    transaction_id = f"tx{uuid4().int % 1_000_000_000}"
    sender_id = _skewed_random_people() if people_skewed else random.randint(1, people_number)
    receiver_id = random.choice([x for x in range(1,people_number+1) if x != sender_id]) # receiver_id can't be the same as sender_id
    amount = _skewed_random_amount() if amount_skewed else round(random.uniform(min_amount, max_amount),2)
    currency = _skewed_random_currency() if currency_skewed else random.choice(list(TransactionCurrency)).value # cast to list to make iterable
    timestamp = _random_datetime_between(start_date, end_date) if random_dates == "random" else datetime.now(timezone.utc).isoformat() # If flag random dates the generate a random one, else get current datetime
    status = _skewed_random_status() if status_skewed else random.choice(list(TransactionStatus)).value # cast to list to make iterable
    
    transaction = {
        "transaction_id": transaction_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "amount": amount,
        "currency": currency,
        "timestamp": timestamp,
        "status": status
    }

    return transaction


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


def generate_tranactions(
        min_amount: float,
        max_amount: float,
        random_dates: bool,
        start_date: str,
        end_date: str,
        people_number: int,
        min_generate_lag: int,
        max_generate_lag: int,
        amount_skewed: bool = False,
        status_skewed: bool = False
):

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
    KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS")

    # Instantiate logger
    logger = get_logger("transactions_stream_cli", APP_NAME)

    brokers = os.getenv("KAFKA_BROKERS").split(",")  # split into list

    logger.info(f"Start: {APP_NAME}, Kafka brokers:{brokers}")

    producer = _create_producer(bootstrap_servers=brokers, retries=MAX_RETRIES)

    try:
        logger.info(f"Starting {KAFKA_TOPIC_TRANSACTIONS} producer...")
        while True:

            tx = _generate_transaction(min_amount, max_amount, random_dates, start_date, end_date, people_number)
            success = _send_transaction(producer, KAFKA_TOPIC_TRANSACTIONS, tx, tx_id=tx.get("transaction_id"))

            if success:
                    logger.info(f"Produced: {tx}")
            else:
                    logger.warning(f"Produced with error (logged to audit): {tx}")

            if not min_generate_lag and not max_generate_lag:
                time.sleep(int(random.uniform(5,10)))
            else:
                time.sleep(int(random.uniform(min_generate_lag,max_generate_lag)))
    except KeyboardInterrupt:
        logger.info(f"Stoping {KAFKA_TOPIC_TRANSACTIONS} producer... (requested by user)")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.debug(f"{KAFKA_TOPIC_TRANSACTIONS} flushing producer before exit...")
        producer.flush()
        producer.close()
        logger.info(f"{KAFKA_TOPIC_TRANSACTIONS} producer stopped")

        

