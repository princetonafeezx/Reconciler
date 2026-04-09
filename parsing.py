"""Shared parsing for dates and currency amounts used across CSV loaders."""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation


def parse_date(date_text: str) -> date:
    """Parse several common CSV date formats.

    Slash-separated dates try **US month/day/year first** (``%m/%d/%Y``). When that
    fails—e.g. day is 13–31—**day/month/year** (``%d/%m/%Y``) is tried so exports like
    ``31/01/2024`` still parse. If both day and month are ≤12, the US interpretation wins
    (``01/02/2024`` → 2 January, not 1 February).
    """
    raw = date_text.strip()
    patterns = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d/%m/%y",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {date_text}")


def parse_amount(amount_text: str) -> float:
    """Parse a bank-style amount string into a non-negative float (two decimal places).

    Strips ``$``, commas, and common space-like separators used as grouping (ASCII and
    narrow no-break space). Parentheses or a leading minus denote debits; the result is
    always ``>= 0``. Values are rounded **half-up** to cents before converting to
    ``float``, rounded half-up to cents before conversion.

    Scientific notation (``1e2``) is rejected for consistency with interactive amount entry.
    """
    original = (amount_text or "").strip()
    cleaned = (
        original.replace("$", "")
        .replace(",", "")
        .replace("\xa0", "")
        .replace("\u202f", "")
        .strip()
    )
    if not cleaned:
        raise ValueError("Blank amount")
    if "e" in cleaned.lower():
        raise ValueError(
            "Scientific notation is not supported. Use a plain amount like 12.34 or $1,234.56."
        )

    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1].strip()
    if cleaned.startswith("+"):
        cleaned = cleaned[1:].strip()
    if cleaned.startswith("-"):
        negative = True
        cleaned = cleaned[1:].strip()
    if not cleaned:
        raise ValueError("Blank amount")

    try:
        decimal_amount = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid amount: {amount_text!r}") from exc

    rounded = decimal_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    amount = float(rounded)
    if negative:
        amount = -amount
    return abs(amount)
