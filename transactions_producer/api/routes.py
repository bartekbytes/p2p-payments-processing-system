import os
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from decimal import Decimal

# common packages
from common_config import settings
from common_logger import get_logger

# local
from api.generator import generate_transaction

# Get all relevant Environment Variables
APP_NAME = os.getenv("APP_NAME")

# Instantiate logger
logger = get_logger("transactions_stream_api", APP_NAME)


router = APIRouter()


@router.post("/transaction")
def produce_transaction(tx: dict = Body(...)):
    try:
        
        # tx is already a prepared JSON payload for Kafka
        # If we want more robust and secured, we could declare this as
        # def produce_transaction(tx: Transaction)
        # so consistency checking would be done
        # and then convert to dictionary
        generate_transaction(tx)

        logger.debug(f"FastAPI got: {tx}")
    
        return {"status": "success", "transaction": tx}
    
    except Exception as e:
        logger.error(f"Error producing transaction: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )
