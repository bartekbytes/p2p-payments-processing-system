import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from common_models import Transaction, TransactionStatus, TransactionCurrency
from pydantic import ValidationError


@pytest.fixture
def valid_transaction():
    """
    A proper, valid Transaction. Fixture for building various tests.
    """
    return {
        "transaction_id": "tx100",
        "sender_id": 1,
        "receiver_id": 2,
        "amount": Decimal("123.45"),
        "currency": TransactionCurrency.USD,
        "timestamp": datetime.now(timezone.utc),
        "status": TransactionStatus.pending       
    }

def test_valid_transaction(valid_transaction):
    """
    Test valid Transation
    """
    tx = Transaction(**valid_transaction)
    
    assert tx.transaction_id == "tx100"
    assert tx.sender_id == 1
    assert tx.receiver_id == 2
    assert str(tx.amount) == "123.45"
    assert tx.currency == TransactionCurrency.USD
    assert tx.timestamp != None
    assert tx.status == TransactionStatus.pending


def test_transaction_serialization_deserialization(valid_transaction):
    """
    Test serialization / deserialization of a valid Transaction.
    """
    tx = Transaction(**valid_transaction)
    
    # serialization
    json_data = tx.model_dump()
    
    assert json_data["transaction_id"] == "tx100"
    assert json_data["sender_id"] == 1
    assert json_data["receiver_id"] == 2
    assert str(json_data["amount"]) == "123.45"
    assert json_data["currency"] == TransactionCurrency.USD
    assert json_data["timestamp"] != None
    assert json_data["status"] == TransactionStatus.pending

    # deserialization
    json_str = tx.model_dump_json()
    
    assert '"tx100"' in json_str
    assert "123.45" in json_str
    assert TransactionCurrency.USD in json_str
    assert TransactionStatus.pending in json_str



def test_sender_receiver_same_raises(valid_transaction):
    """
    Test that Sender and Receiver can't be the same.
    """
    valid_transaction["receiver_id"] = valid_transaction["sender_id"]
    
    with pytest.raises(ValidationError) as ve:
        Transaction(**valid_transaction)

    assert "Sender and Receiver cannot be the same User" in str(ve.value)


@pytest.mark.parametrize("invalid_currency", ["ABC", "usd", "EURO", "Hong Kong Dollar"])
def test_invalid_currency_raises(valid_transaction, invalid_currency):

    valid_transaction["currency"] = invalid_currency # invalid currency code

    with pytest.raises(ValidationError) as ve:
        Transaction(**valid_transaction)
    
    errors = ve.value.errors()
    assert len(errors) == 1

    #print(ve.value.errors())

    error = errors[0]
    assert error['type'] == 'value_error'
    assert error['loc'] == ('currency',)    
    assert error['msg'] == f'Value error, Currency {invalid_currency} is not supported.'
    
    

def test_negative_amount_raises(valid_transaction):
    invalid_amount = Decimal("-10.00") # negative amount
    valid_transaction["amount"] = invalid_amount

    with pytest.raises(ValidationError) as ve:
        Transaction(**valid_transaction)
    
    errors = ve.value.errors()
    assert len(errors) == 1

    print(ve.value.errors())

    error = errors[0]
    assert error['type'] == 'greater_than'
    assert error['loc'] == ('amount',)    
    assert error['msg'] == f'Input should be greater than 0'



# -------------------------------
# Zero amount
# -------------------------------

def test_zero_amount_raises(valid_transaction):
    invalid_amount = Decimal("0") # zero amount
    valid_transaction["amount"] = invalid_amount
    
    with pytest.raises(ValidationError) as ve:
        Transaction(**valid_transaction)

    errors = ve.value.errors()
    assert len(errors) == 1

    print(ve.value.errors())

    error = errors[0]
    assert error['type'] == 'greater_than'
    assert error['loc'] == ('amount',)    
    assert error['msg'] == f'Input should be greater than 0'





@pytest.mark.parametrize("invalid_id", ["100", "txABC", "TX123"])
def test_invalid_transaction_id_pattern(valid_transaction, invalid_id):
    valid_transaction["transaction_id"] = invalid_id
    
    with pytest.raises(ValidationError) as ve:
        Transaction(**valid_transaction)
    
    errors = ve.value.errors()
    err = errors[0]

    print(ve.value.errors())

    assert err['loc'] == ('transaction_id',)
    assert err['type'] == 'string_pattern_mismatch'
    assert "String should match pattern" in err['msg']




# -------------------------------
# Large decimal
# -------------------------------

def test_large_decimal_amount(valid_transaction):

    currect_amount = Decimal("1234567890123456.12")  # 18 digits
    valid_transaction["amount"] = currect_amount
    
    tx = Transaction(**valid_transaction)
    assert str(tx.amount) == str(currect_amount)

def test_too_large_decimal_amount_raises(valid_transaction):
    incorrect_amount = Decimal("12345678901234567.12")  # 19 digits
    valid_transaction["amount"] = incorrect_amount
    
    with pytest.raises(ValidationError):
        Transaction(**valid_transaction) 


def test_future_timestamp(valid_transaction):
    valid_transaction["timestamp"] = datetime.now(timezone.utc) + timedelta(days=1)
    tx = Transaction(**valid_transaction)
    
    assert tx.timestamp > datetime.now(timezone.utc)