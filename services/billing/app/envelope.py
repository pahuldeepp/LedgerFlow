from typing import Any, Dict

def event_type(evt: Dict[str, Any]) -> str:
    return evt.get("event_type", "") or evt.get("type", "")


def transaction_id(evt: Dict[str, Any]) -> str:
    data = evt.get("data", {})
    tx_id = data.get("transaction_id") or evt.get("transaction_id")
    if not tx_id:
        raise ValueError("Missing transaction_id in event data")
    return str(tx_id)


def amount_from_event(evt: Dict[str, Any]) -> int:
    entries = evt.get("entries")
    if entries:
        credit_sum = sum(
            int(e["amount"])
            for e in entries
            if e.get("direction") == "credit"
        )
        return credit_sum

    # fallback for enveloped events
    data = evt.get("data") or {}
    if "amount" in data:
        return int(data["amount"])

    raise ValueError("Cannot extract amount from event")