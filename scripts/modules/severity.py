#!/usr/bin/env python3
def evaluate_severity_rules(rules, context):
    """
    Evaluates a list of severity rules (from YAML) against a context dictionary.

    Each rule has:
    - match: a Python expression to evaluate (e.g., "loss > 50")
    - tag: a label to attach if matched (e.g., "HIGH_LOSS")
    - level: the logging level to use (e.g., "warning", "error")

    Parameters:
    - rules: List of dictionaries with match/tag/level keys
    - context: A dictionary of values to test (e.g., {"loss": 75, "hop_changed": True})

    Returns:
    - (tag, level): If a rule matches, returns its tag and level. If none match, returns (None, None).
    """

    for rule in rules:
        try:
            # Evaluate the rule condition using the context
            if eval(rule["match"], {}, context):
                return rule["tag"], rule["level"]
        except Exception as e:
            # If the rule is malformed or fails, skip it silently
            pass

    return None, None  # No rule matched


def hops_changed(prev, curr):
    """
    Compares two lists of hops to check if the traceroute path changed.

    Parameters:
    - prev: List of previous hop dictionaries (e.g., [{"host": "1.1.1.1"}, ...])
    - curr: List of current hop dictionaries

    Returns:
    - True if the sequence of hop IPs/hosts changed, False otherwise.
    """

    prev_hosts = [h.get("host") for h in prev]
    curr_hosts = [h.get("host") for h in curr]
    return prev_hosts != curr_hosts
