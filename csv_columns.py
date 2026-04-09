"""Heuristic detection of date, merchant, and amount columns from CSV headers."""

from __future__ import annotations

from typing import TypedDict


class FullColumnMap(TypedDict):
    """Column indices for the three reconciliation fields (``None`` if no good match)."""

    date: int | None
    merchant: int | None
    amount: int | None


def _norm(header: str) -> str:
    return header.lower().strip()


def _score_date(header: str) -> float:
    h = _norm(header)
    weight = 0.0
    patterns = (
        ("posting date", 12.0),
        ("post date", 12.0),
        ("trans date", 10.0),
        ("transaction date", 10.0),
        ("value date", 9.0),
        ("booked date", 9.0),
        ("settlement date", 9.0),
        ("date", 6.0),
        ("posted", 5.0),
        ("dt", 2.0),
    )
    for needle, w in patterns:
        if needle in h:
            weight += w
    return weight


def _score_merchant(header: str) -> float:
    h = _norm(header)
    weight = 0.0
    patterns = (
        ("transaction description", 14.0),
        ("description", 8.0),
        ("merchant", 11.0),
        ("payee", 10.0),
        ("counterparty", 9.0),
        ("narration", 8.0),
        ("vendor", 8.0),
        ("beneficiary", 7.0),
        ("memo", 5.0),
        ("details", 5.0),
    )
    for needle, w in patterns:
        if needle in h:
            weight += w
    if h == "name" or h.endswith(" name"):
        weight += 4.0
    return weight


def _score_amount(header: str) -> float:
    h = _norm(header)
    weight = 0.0
    patterns = (
        ("debit", 11.0),
        ("credit", 11.0),
        ("amount", 12.0),
        ("withdrawal", 8.0),
        ("deposit", 7.0),
        ("total", 5.0),
        ("sum", 4.0),
        ("value", 3.0),
        ("balance", 2.0),
    )
    for needle, w in patterns:
        if needle in h:
            weight += w
    if h.startswith("$") or " $" in h:
        weight += 3.0
    return weight


def role_score(header: str, role: str) -> float:
    """Score how well ``header`` matches ``role`` (``date`` | ``merchant`` | ``amount``)."""
    if role == "date":
        return _score_date(header)
    if role == "merchant":
        return _score_merchant(header)
    if role == "amount":
        return _score_amount(header)
    raise ValueError(f"Unknown role: {role!r}")


def detect_columns(headers: list[str]) -> FullColumnMap:
    """Pick the best column index for each role using keyword scores.

    Each column is assigned to at most one role. Roles are filled in order
    date → merchant → amount by taking the highest remaining score for that role.
    """
    n = len(headers)
    if n == 0:
        return {"date": None, "merchant": None, "amount": None}

    date_scores = [_score_date(h) for h in headers]
    merchant_scores = [_score_merchant(h) for h in headers]
    amount_scores = [_score_amount(h) for h in headers]

    used: set[int] = set()
    result: FullColumnMap = {"date": None, "merchant": None, "amount": None}

    def take_best(scores: list[float]) -> int | None:
        best_i: int | None = None
        best_v = 0.0
        for i in range(n):
            if i in used:
                continue
            if scores[i] > best_v:
                best_v = scores[i]
                best_i = i
        if best_i is None or best_v <= 0:
            return None
        used.add(best_i)
        return best_i

    di = take_best(date_scores)
    result["date"] = di
    mi = take_best(merchant_scores)
    result["merchant"] = mi
    ai = take_best(amount_scores)
    result["amount"] = ai

    # Fallback: if a role missed but columns remain, assign by leftover order
    leftover = [i for i in range(n) if i not in used]
    for key in ("date", "merchant", "amount"):
        if result[key] is None and leftover:
            result[key] = leftover.pop(0)

    return result
