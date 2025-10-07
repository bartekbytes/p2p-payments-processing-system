import os
import time
from kafka import KafkaConsumer, errors

from processor import create_consumer, get_validation_rules, TransactionProcessor


# common packages
from common_config import settings
from common_logger import get_logger


def main():

    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")
    SOURCE_SYSTEM_ID = int(os.getenv("SOURCE_SYSTEM_ID", 2)) # fallback to Id=1 (transactions_stream_loading)
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
    KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS")
    TRANSACTION_DB_BATCH_SIZE = int(os.getenv("TRANSACTION_DB_BATCH_SIZE"))
    KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS = int(os.getenv("KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS"))
    KAFKA_CONSUMER_RETRIES = int(os.getenv("KAFKA_CONSUMER_RETRIES", 5)) # non-required env variable, fallback to default
    KAFKA_CONSUMER_INITIAL_DELAY = int(os.getenv("KAFKA_CONSUMER_INITIAL_DELAY", 1)) # non-required env variable, fallback to default
    DB_CONNECTION_URL = os.getenv("DB_CONNECTION_URL") #?

    
    # Instantiate logger
    logger = get_logger("main", APP_NAME)

    logger.info(f"Start: {APP_NAME}")
    logger.info(f"ENVVAR:[APP_NAME:{APP_NAME}]")
    logger.info(f"ENVVAR:[SOURCE_SYSTEM_ID:{SOURCE_SYSTEM_ID}]")
    logger.info(f"ENVVAR:[KAFKA_BROKERS:{KAFKA_BROKERS}]")
    logger.info(f"ENVVAR:[KAFKA_TOPIC_TRANSACTIONS:{KAFKA_TOPIC_TRANSACTIONS}]")
    logger.info(f"ENVVAR:[TRANSACTION_DB_BATCH_SIZE:{TRANSACTION_DB_BATCH_SIZE}]")
    logger.info(f"ENVVAR:[KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS:{KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS}]")
    logger.info(f"ENVVAR:[KAFKA_CONSUMER_RETRIES:{KAFKA_CONSUMER_RETRIES}]")
    logger.info(f"ENVVAR:[KAFKA_CONSUMER_INITIAL_DELAY:{KAFKA_CONSUMER_INITIAL_DELAY}]")
    logger.info(f"ENVVAR:[DB_CONNECTION_URL:{DB_CONNECTION_URL}]")
    
    required_env_vars = [
        APP_NAME,
        SOURCE_SYSTEM_ID,
        KAFKA_BROKERS,
        KAFKA_TOPIC_TRANSACTIONS,
        TRANSACTION_DB_BATCH_SIZE,
        KAFKA_CONSUMER_TRANSACTION_FLUSH_INTERVAL_SECONDS,
        KAFKA_CONSUMER_RETRIES,
        KAFKA_CONSUMER_INITIAL_DELAY,
        DB_CONNECTION_URL
    ]

    #enable_auto_commit=True ??? what is this
    
    if not all(required_env_vars):
        logger.error("One or more required Environment Variables have not been set up")
        exit(1)

    brokers = os.getenv("KAFKA_BROKERS").split(",")  # split into list of brokers

    consumer = create_consumer(
        bootstrap_servers=brokers,
        retries=KAFKA_CONSUMER_RETRIES
    )

    try:
        logger.info(f"Starting {KAFKA_TOPIC_TRANSACTIONS} consumer...")
    
        rules = get_validation_rules()
        processor = TransactionProcessor(rules)
        processor.create_transactions_dlq_producer(bootstrap_servers=brokers)

        # processing incoming messages
        # Here some tweaks could be added like: 
        # 1) pooling (consumer.pool()) to pool messages first and 
        #  send a collection and process in a loop: 
        #  for m im msg.items())
        # 2) retry mechanism, let's say (pseudo code): 
        #       while number_of_failed_retries < 3 times (RETRY_LIMIT): 
        #           try: 
        #               ... 
        #               processor.process_message(msg) ... 
        #           except 
        #               number_of_failed_retries += 1
        #   
        #       if not success after 3 times -> send to DLQ
        for msg in consumer:
            processor.process_message(msg)
            consumer.commit() # commit only fully processed and inserted into DB messages in batch

    except KeyboardInterrupt:
        logger.info(f"Stoping {KAFKA_TOPIC_TRANSACTIONS} consumer... (requested by user)")
    finally:
        logger.info(f"{KAFKA_TOPIC_TRANSACTIONS} consumer stopping...")
        consumer.close()
        logger.info(f"{KAFKA_TOPIC_TRANSACTIONS} consumer stopped")


if __name__ == "__main__":
    main()



    

    
    
