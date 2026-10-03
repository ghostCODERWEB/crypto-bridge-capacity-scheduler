"""Replay token-bucket bridge throughput with burst caps, refill intervals and deterministic request rejection."""

from .common import (
    number,
    integer,
    identity,
    records,
    chronology,
    allocate,
    result,
    validate_document,
)
from decimal import Decimal


def _analyze(d):
    capacity = integer(d["capacity"], 1)
    tokens = integer(d["initial"], maximum=capacity)
    refill = integer(d["refill_per_second"])
    last = integer(d["start"])
    trace = []
    accepted = 0
    for e in chronology(d["requests"]):
        t = e["timestamp"]
        amount = integer(e["amount"], 1)
        if t < last:
            raise ValueError("request before start")
        tokens = min(capacity, tokens + (t - last) * refill)
        last = t
        ok = amount <= tokens
        if ok:
            tokens -= amount
            accepted += amount
        trace.append(
            {
                "timestamp": t,
                "amount": amount,
                "accepted": ok,
                "remaining_capacity": tokens,
            }
        )
    return {
        "trace": trace,
        "accepted_volume": accepted,
        "remaining_capacity": tokens,
        "rejected_requests": sum(not r["accepted"] for r in trace),
    }


def analyze(document):
    validate_document(document)
    try:
        return result(_analyze(document))
    except (KeyError, TypeError, ArithmeticError, RecursionError) as error:
        raise ValueError("invalid domain input: " + str(error)) from error
