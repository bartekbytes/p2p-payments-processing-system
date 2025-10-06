import logging
import os
import sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv


class ElasticsearchLogHandler(logging.Handler):
    """
    Custom-built LogHandler for Elasticsearch.
    It sends logs directly to Elasticsearch via HTTP POST request.
    Added retry mechanism for transient issues.
    Added async support (thread)
    """

    _executor = ThreadPoolExecutor(max_workers=3)

    def __init__(self, elastic_url: str, index_name: str):
        super().__init__()
        self.elastic_url = elastic_url.rstrip("/")
        self.index_name = index_name
        self.session = self._create_retry_session()

    # retry mechanism in case of transient issues
    def _create_retry_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    

    def _build_payload(self, record):
        
        # prepare a payload for Elasticsearch
        payload = {
            "@timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "app_name": getattr(record, "app_name", None),
            "logger": record.name,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }

        return payload


    def emit(self, record):
        try:
            payload = self._build_payload(record)
            self._executor.submit(self._send_to_elasticsearch, payload)
        except Exception as e:
            import sys
            sys.stderr.write(f"Log handling error: {e}\n")


    def _send_to_elasticsearch(self, payload):
        try:
            
            self.session.post(
                f"{self.elastic_url}/{self.index_name}/_doc",
                json=payload,
                timeout=5
            )
        except Exception as e:
            sys.stderr.write(f"Failed to send log to Elasticsearch: {e}\n")



def get_logger(name: str, app_name: str):

    load_dotenv()

    logger = logging.getLogger(name=name)
    
    if logger.hasHandlers():
        return logger

    # Environment settings
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()) # Fallback to INFO (just in case)
    
    log_to_console = os.getenv("LOG_TO_CONSOLE", "true").lower() == "true"
    json_console = os.getenv("LOG_JSON_CONSOLE", "false").lower() == "true"

    log_to_elk = os.getenv("LOG_TO_ELK", "true").lower() == "true"

    log_to_file = os.getenv("LOG_TO_FILE", "true").lower() == "true"
    log_file = os.getenv("LOG_FILE_PATH", "logs/csv_loader.log")
    json_file = os.getenv("LOG_JSON_FILE", "true").lower() == "true"

    elk_url = os.getenv("ELK_URL", "http://localhost:9200") # fallback to default if not found
    elk_index = os.getenv("ELK_INDEX", "logs") # fallback to default in fot found 
    

    logger.setLevel(log_level)

    # Console handler
    if log_to_console:
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        if json_console:
            fmt = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(app_name)s %(name)s %(message)s')
            ch.setFormatter(fmt)
        else:
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(app_name)s - %(name)s - %(message)s")
            ch.setFormatter(fmt)
        ch.addFilter(lambda record: setattr(record, "app_name", app_name) or True)
        logger.addHandler(ch)

    # ELK stack handler
    if log_to_elk:
        es_handler = ElasticsearchLogHandler(elk_url, index_name=elk_index)

        json_fmt = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(levelname)s %(app_name)s %(name)s %(message)s '
            '%(module)s %(funcName)s %(lineno)d',
            rename_fields={'asctime': '@timestamp'}
        )
        es_handler.setFormatter(json_fmt)
        es_handler.addFilter(lambda record: setattr(record, "app_name", app_name) or True)

        logger.addHandler(es_handler)

    # File handler
    if log_to_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=50_000_000, backupCount=5)
        fh.setLevel(log_level)
        if json_file:
            fmt = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(app_name)s %(name)s %(message)s')
            fh.setFormatter(fmt)
        else:
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(app_name)s - %(name)s - %(message)s")
            fh.setFormatter(fmt)
        fh.addFilter(lambda record: setattr(record, "app_name", app_name) or True)
        logger.addHandler(fh)

    # Wrap logger in LoggerAdapter to inject app_name
    adapter = logging.LoggerAdapter(logger, {"app_name": app_name})
    return adapter
