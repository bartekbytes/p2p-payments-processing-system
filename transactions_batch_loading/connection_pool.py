import os
from psycopg2 import pool,  OperationalError
from contextlib import contextmanager

# common packages
from common_config import settings
from common_logger import get_logger

APP_NAME = os.getenv("APP_NAME")
DB_CONNECTION_URL = os.getenv("DB_CONNECTION_URL")
DB_CONNECTION_POOL_MIN = int(os.getenv("DB_CONNECTION_POOL_MIN", 1)) # defaults to 1 if not set up
DB_CONNECTION_POOL_MAX = int(os.getenv("DB_CONNECTION_POOL_MAX", 10)) # defaults to 10 if not set up

# Instantiate logger
logger = get_logger("connection_pool", APP_NAME)

#####
# FUTURE IMPROVEMENTS:
# Currently it works in an "eager-mode", once imported, it automatically executes the below code:
# Consequence is that if DB is not reachable at import time, app will crash immediately
# Improvement can be to make it "lazy-mode" by doing lazy initialization - create the DB Pool when first used.
# Propositien can be to wrap the logic into a Singleton class
#####
try:
    connection_pool = pool.SimpleConnectionPool(
        DB_CONNECTION_POOL_MIN, DB_CONNECTION_POOL_MAX,
        DB_CONNECTION_URL, connect_timeout=10
    )
    logger.info(f"Initialized DB connection pool for {DB_CONNECTION_URL} ({DB_CONNECTION_POOL_MIN}-{DB_CONNECTION_POOL_MAX})")
except OperationalError as e:
    logger.exception("Failed to initialize connection pool")
    raise


def _get_connection():
    """Borrow a connection from the pool."""
    return connection_pool.getconn()


def _release_connection(conn):
    """Return a connection back to the pool."""
    if conn:
        connection_pool.putconn(conn)


@contextmanager
def db_connection():
    """
    Context manager for DB connection usage.
    Gets connection from the Connection Pool and 
    automatically releases connection back to the pool aster usage.
    Commits transaction(s) when no Exceptions or 
    rollback transaction(s) when any Exception ocurred.
    
    Example of usage:
        with db_connection() as c:
            with c.cursor() as cur:
                cur.execute("SELECT 1")
    """
    c = None
    try:
        c = _get_connection()
        yield c
        c.commit()
    except Exception as e:
        if c:
            c.rollback()
        logger.exception("DB operation failed, rolling back.")
        raise
    finally:
        if c:
            _release_connection(c)