from common_models import Transaction

class RulesEngine:
    """
    Store all the Validation Rules
    """
    def __init__(self, rules: list):
        self.rules = rules

    def apply_rules(self, tx: Transaction) -> list[str]:
        """
        For each of the rule coming from the defined `rules` list:
        Apply the rule against the given Transaction `tx`.
        If the Transaction violates the rule, add into `violations` list,
        After checking all the rules, return this list.
        """
        violations = []
        for rule in self.rules:
            result = rule(tx)
            if result:
                violation_reason, violation_severity = result
                violations.append({"violation_reason": violation_reason, "violation_severity": violation_severity})
        return violations