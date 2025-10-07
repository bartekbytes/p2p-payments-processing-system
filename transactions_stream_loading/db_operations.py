from psycopg2.extras import execute_batch, execute_values
from datetime import datetime, timezone
import os

# common packages
from common_config import settings
from common_logger import get_logger

from rules_engine import RulesEngine
from rules import amount_threshold, sender_blacklist, RULE_TYPE_MAPPING


# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")

# Instantiate logger
logger = get_logger("db_operations", APP_NAME)



def create_loading_process(db_connection, source_system_id: int, source_name: str, mode: str) -> tuple[int, datetime]:
    """Insert a new loading_process row and return (load_id, start_time)."""
    start_time = datetime.now(timezone.utc)
    query = """
        INSERT INTO fc_audit.loading_processes (source_system_id, file_name, mode, process_status, start_time)
        VALUES (%s, %s,%s,%s,%s) RETURNING load_id
    """

    with db_connection.cursor() as cur:
        cur.execute(query, (source_system_id, source_name, mode, "in_progress", start_time))
        load_id = cur.fetchone()[0]

    logger.debug(f"Started loading process [{load_id}] source '{source_name}' in mode '{mode}'")
    return load_id, start_time


def update_loading_process(
    db_connection, load_id: int, total_rows: int, approved_rows: int, rejected_rows: int,
    suspicious_cases: int, start_time: datetime, process_status: str,
) -> None:
    """Finalize a loading_process with statistics and duration."""
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()

    query = """
        UPDATE fc_audit.loading_processes
        SET end_time=%s, process_status=%s, total_rows=%s, approved_rows=%s,
            rejected_rows=%s, suspicious_cases=%s, duration_seconds=%s
        WHERE load_id=%s
    """

    with db_connection.cursor() as cur:
        cur.execute(
            query,
            (end_time, process_status, total_rows, approved_rows, rejected_rows, suspicious_cases, duration, load_id),
        )

    logger.debug(
        f"[load_id={load_id}] Completed loading process: {total_rows} rows | "
        f"{approved_rows} approved | {rejected_rows} rejected | "
        f"{suspicious_cases} suspicious | {duration:.2f}s"
    )


def load_rules_from_db(db_connection) -> RulesEngine:
    """Load dynamic rules from DB and construct a RulesEngine."""
    query = "SELECT rule_type, params, violation_severity FROM fc_mdm.transaction_validation_rules"

    rules = []
    
    # Get all definitions of rules stored in the DB
    with db_connection.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    # Based on the Validation Rule type, create appropriate instanc eof the Rule
    # with the parametrization coming from the DB
    # then create a Rules Engine and return.
    for rule_type, params, violation_severity in rows:
        # This could be impoved with using a `Registry Pattern`` to avoid a spaghetti if-elif
        if rule_type == "amount_threshold":
            rules.append(amount_threshold(params.get("threshold", 5000), violation_severity))
        elif rule_type == "sender_blacklist":
            rules.append(sender_blacklist(params.get("blacklist", []), violation_severity))
        else:
            logger.warning(f"Unknown rule type in DB: {rule_type}")

    return RulesEngine(rules)


def insert_transactions_batch(db_connection, transactions: list[dict], load_id: int, batch_size: int = 100) -> None:
    """Batch insert valid transaction into fc.transactions."""
    if not transactions:
        return

    query = """
        INSERT INTO fc.transactions 
        (transaction_id, sender_id, receiver_id, amount, currency, timestamp, status, load_id)
        VALUES %s
    """

    values = [
        (
            tx.transaction_id,
            tx.sender_id,
            tx.receiver_id,
            tx.amount,
            tx.currency,
            tx.timestamp,
            tx.status,
            load_id
        )
        for tx in transactions
    ]

    with db_connection.cursor() as cur:
        execute_values(cur, query, values, page_size=batch_size)

    logger.debug(f"[load_id={load_id}] Logged {len(transactions)} transactions.")


def insert_transactions_single(db_connection, transaction, load_id: int) -> None:
    """Single insert a valid transactio into fc.transactions."""
    if not transaction:
        return

    query = """
        INSERT INTO fc.transactions 
        (transaction_id, sender_id, receiver_id, amount, currency, timestamp, status, load_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    value = (
        transaction.transaction_id,
        transaction.sender_id,
        transaction.receiver_id,
        transaction.amount,
        transaction.currency,
        transaction.timestamp,
        transaction.status,
        load_id
    )

    with db_connection.cursor() as cur:
        cur.execute(query, value)
        
    logger.debug(f"[load_id={load_id}] Logged 1 transaction.")


def log_suspicious_cases_to_db(db_connection, suspicious_cases: list[dict], load_id: int, batch_size: int = 100) -> None:
    """Batch insert suspicious cases into fc_audit.suspicious_transactions."""
    if not suspicious_cases:
        return

    query = """
        INSERT INTO fc_audit.suspicious_transactions
        (load_id, transaction_id, violation_reason, violation_severity)
        VALUES (%s,%s,%s,%s)
    """
    values = [
        (
            load_id,
            row.get("transaction_id"),
            row.get("violation_reason"),
            row.get("violation_severity"),
        )
        for row in suspicious_cases
    ]

    with db_connection.cursor() as cur:
        execute_batch(cur, query, values, page_size=batch_size)
    
    logger.debug(f"[load_id={load_id}] Logged {len(suspicious_cases)} suspicious cases.")


def log_errors_to_db(db_connection, errors: list[dict], load_id: int, batch_size: int = 100) -> None:
    """Batch insert invalid Transactions into fc_audit.transaction_errors."""
    if not errors:
        return

    query = """
        INSERT INTO fc_audit.transaction_errors
        (load_id, line_number, transaction_id, sender_id, receiver_id, amount, currency, timestamp, status, error, error_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    values = [
        (
            load_id,
            row.get("line_number"),
            row.get("transaction_id"),
            row.get("sender_id"),
            row.get("receiver_id"),
            row.get("amount"),
            row.get("currency"),
            row.get("timestamp"),
            row.get("status"),
            row.get("error"),
            row.get("error_type"),
        )
        for row in errors
    ]

    with db_connection.cursor() as cur:
        execute_batch(cur, query, values, page_size=batch_size)
    
    logger.debug(f"[load_id={load_id}] Logged {len(errors)} invalid rows to DB.")
