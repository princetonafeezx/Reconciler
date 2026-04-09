"""Tests for shared date and amount parsing."""

from __future__ import annotations

from datetime import date

import pytest
from parsing import parse_amount, parse_date


def test_parse_date_iso_and_slashes() -> None:
    assert parse_date("2024-01-15") == date(2024, 1, 15)
    assert parse_date("01/15/2024") == date(2024, 1, 15)
    assert parse_date("01/15/24") == date(2024, 1, 15)


def test_parse_date_year_month_day_slashes() -> None:
    assert parse_date("2024/03/15") == date(2024, 3, 15)


def test_parse_date_european_when_us_invalid() -> None:
    assert parse_date("31/01/2024") == date(2024, 1, 31)
    assert parse_date("13/01/2024") == date(2024, 1, 13)


def test_parse_date_ambiguous_slash_uses_us_month_first() -> None:
    assert parse_date("01/02/2024") == date(2024, 1, 2)


def test_parse_date_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unsupported date format"):
        parse_date("not-a-date")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("12.34", 12.34),
        ("$1,234.56", 1234.56),
        ("(10.00)", 10.0),
        ("-5.00", 5.0),
        ("+5.25", 5.25),
        ("1\xa0234.56", 1234.56),
    ],
)
def test_parse_amount_variants(text: str, expected: float) -> None:
    assert parse_amount(text) == expected


def test_parse_amount_round_half_up_to_cents() -> None:
    assert parse_amount("1.005") == pytest.approx(1.01)
    assert parse_amount("2.675") == pytest.approx(2.68)


def test_parse_amount_rejects_scientific_notation() -> None:
    with pytest.raises(ValueError, match="Scientific notation"):
        parse_amount("1e2")


def test_parse_amount_invalid_numeric() -> None:
    with pytest.raises(ValueError, match="Invalid amount"):
        parse_amount("12.34.56")


def test_parse_amount_blank() -> None:
    with pytest.raises(ValueError, match="Blank amount"):
        parse_amount("")
