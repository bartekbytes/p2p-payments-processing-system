from typing import Optional
from decimal import Decimal

from common_models import Transaction

# Important
# In this file all the Transaction Validation Rules are defined.
# Each rule is a separate function.
# New rules with definitions can be added here by creating a new fule definition
# and adding to a mapping dictionary (RULE_TYPE_MAPPING)

def amount_threshold(threshold: Decimal, violation_severity: str):
    """
    Transaction Validation Rule: Amount Threshold
    Rule definition: Flag a Transaction if Transaction `amount` is over a `threshold`
    """
    def rule(tx: Transaction) -> Optional[tuple[str, str]]:
        if tx.amount > threshold:
            return (f"Amount {tx.amount} exceeds threshold {threshold}", violation_severity)
        return None
    return rule


def sender_blacklist(blacklist: list[str], violation_severity: str):
    """
    Transaction Validation Rule: Sender Blacklist
    Rule definition: Flag a Transaction if Transaction `sender` is in `blacklist` list
    """
    def rule(tx: Transaction) -> Optional[tuple[str, str]]:
        if tx.sender_id in blacklist:
            return (f"Sender {tx.sender_id} is blacklisted", violation_severity)
        return None
    return rule


# Mapper - rule name to rule validation function
RULE_TYPE_MAPPING = {
    "amount_threshold": amount_threshold,
    "sender_blacklist": sender_blacklist,
}