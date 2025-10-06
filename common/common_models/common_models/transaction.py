from datetime import datetime
from enum import StrEnum
from pydantic import BaseModel, Field, condecimal, field_validator, model_validator



class TransactionStatus(StrEnum):
    """
    Enum for all possible Transacion statuses.
    """
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class TransactionCurrency(StrEnum):
    """
    Enum for all allowed Currencies in the system.
    """
    HKD = "HKD"
    PHP = "PHP"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


# Define a constrained decimal type
Amount = condecimal(gt=0, max_digits=18, decimal_places=2) # 16 digits before dot, 2 digits after

class Transaction(BaseModel):
    transaction_id: str = Field(..., pattern=r'^tx[0-9]+$',description="Unique Transaction Id. Must start with 'tx' followed by digits.")
    sender_id: int = Field(..., ge=1, description="Sender User Id")
    receiver_id: int = Field(..., ge=1, description="Receiver User Id")
    amount: Amount = Field(..., description="Transaction Amount. Positive number with up to 2 decimal places.") # pyright: ignore[reportInvalidTypeForm]
    currency: str = Field(..., description="3-letter Currency Code according to ISO-3611 standards.")
    timestamp: datetime = Field(..., description="Date and Time when Transaction ocurred.")
    status: str = Field(..., description="Status of Transaction.")

    @field_validator('currency')
    @classmethod
    def currency_validation(cls, v: str) -> str:
        """
        Custom 'currency' field validator to overwrite the default error message.
        """
        if v not in TransactionCurrency:
            raise ValueError(f"Currency {v} is not supported.")
        return v

    @field_validator('status')
    @classmethod
    def status_validation(cls, v):
        """
        Custom 'status' field validator to overwrite the default error message.
        """
        if v not in TransactionStatus:
            raise ValueError(f"Invalid Transaction Status: {v}")
        return v

    @model_validator(mode="after")
    def sender_receiver_cannot_be_same(self):
        """
        Check if sender and receiver is the same.
        """
        if self.sender_id == self.receiver_id:
            raise ValueError("Sender and Receiver cannot be the same User.")
        return self

