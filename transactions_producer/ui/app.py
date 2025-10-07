import requests
import os
from datetime import datetime, timezone
from uuid import uuid4

import streamlit as st

#from config import settings
#from logger.logger_config import get_logger

# common packages
from common_config import settings
from common_logger import get_logger
from common_models import Transaction, TransactionCurrency, TransactionStatus

@st.dialog("Transaction Status")
def popup_window(message: str, success: bool):
    if success:
        st.success(message)
    else:
        st.error(message)
    
    if st.button("OK"):
        st.rerun()

    
def main():
    
    # Get all relevant Environment Variables
    APP_NAME = os.getenv("APP_NAME")
    API_URL = os.getenv("API_URL")

    # Instantiate logger
    logger = get_logger("transactions_ui", APP_NAME)

    if not APP_NAME or not API_URL:
        logger.error("Application cannot start. One or more required Environment Variables were not loaded")
        st.error("Application cannot start. One or more required Environment Variables were not loaded")

    # Prepare Layout
    col1, col2 = st.columns([0.1, 0.9])  # adjust ratio as needed

    file_directory = os.path.dirname(__file__)
    with col1:
        st.image(os.path.join(file_directory, "pps_circle_logo.png"))

    with col2:
        st.title("PPS - Transaction")

    st.text(f"API URL: {API_URL}")

    transaction_id = f"tx{uuid4().int % 1_000_000_000}"

    col1, col2 = st.columns([1, 3])
    col1.markdown("**Sender**")
    sender_id = col2.selectbox("Sender", [x for x in range(1,10+1)], label_visibility="collapsed", key="sender")

    col1, col2 = st.columns([1, 3])
    col1.markdown("**Receiver**")
    receiver_id = col2.selectbox("Receiver", [x for x in range(1,10+1)], label_visibility="collapsed", key="receiver")

    col1, col2 = st.columns([1, 3])
    col1.markdown("**Amount**")
    amount = col2.number_input("Amount", min_value=0.01, format="%.2f", label_visibility="collapsed", key="amount")

    col1, col2 = st.columns([1, 3])
    col1.markdown("**Currency**")
    currency = col2.selectbox("Currency", list(TransactionCurrency), label_visibility="collapsed", key="currency")

    col1, col2 = st.columns([1, 3])
    col1.markdown("Status")
    status = col2.selectbox("Status", list(TransactionStatus), label_visibility="collapsed", key="status")


    if st.button("Send Transaction"):
        
        if sender_id == receiver_id:
            popup_window(f"sender can't be the same as receiver", success=False)
        else:
            
            resp = None
            
            try:

                # creating payload in JSON directly,
                # without creation Transaction instance first and then transforming it to dict (model_dump())
                payload = {
                    "transaction_id": transaction_id,
                    "sender_id": sender_id,
                    "receiver_id": receiver_id,
                    "amount": amount,
                    "currency": str(currency), # get a value from selected enum value
                    "timestamp": datetime.now(timezone.utc).isoformat(), # always gets the current time
                    "status": str(status) # get a value from selected enum value
                }

                logger.info(f"Submitting Transaction {transaction_id} (values - sender_id:{sender_id}, recevier_id:{receiver_id}, amount:{amount}, currency:{currency}, status:{status})")

                API_URL_TRANSACTION = API_URL + "transaction" 

                with st.spinner("Sending Transaction... please wait"):
                    resp = requests.post(API_URL_TRANSACTION, json=payload, timeout=15)
            
                if resp.status_code == 200:
                    popup_window(f"Transaction {transaction_id} submitted. Code {resp.status_code}", success=True)
                    logger.info(f"Transaction {transaction_id} submitted.")
                else:
                    popup_window(f"Could not process your transaction. Please try again. (code: {resp.status_code} reason: {resp.reason})", success=False)
                    logger.error(f"Backend error: {resp.text}")
            
            except Exception as e:
                if resp is not None:
                    popup_window(f"Unexpected error. (code: {resp.status_code} reason: {resp.reason})", success=False)
                else:
                    popup_window(f"Unexpected error before request could be sent: {e}", success=False)
                logger.exception(f"Unexpected error during transaction submission: {e}.")




if __name__ == "__main__":
    main()