"""Tests for reconciliation matching."""

from __future__ import annotations

from pathlib import Path

import pytest
import reconciler


def test_detect_columns_uses_shared_csv_scoring() -> None:
    mapping = reconciler.detect_columns(["Posting Date", "Transaction Description", "Debit"])
    assert mapping["date"] == 0
    assert mapping["merchant"] == 1
    assert mapping["amount"] == 2


def test_detect_columns_partial_override_merges_with_auto_map() -> None:
    headers = ["date", "merchant", "amount"]
    mapping = reconciler.detect_columns(headers, column_map={"amount": 0})
    assert mapping["amount"] == 0
    assert mapping["date"] is not None
    assert mapping["merchant"] is not None


def test_load_transactions_minimal_csv(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("Date,Description,Amount\n2026-01-15,Store A,12.34\n", encoding="utf-8")
    rows, warnings = reconciler.load_transactions(path, "Test")
    assert not warnings
    assert len(rows) == 1
    assert rows[0]["merchant"] == "Store A"
    assert rows[0]["amount"] == pytest.approx(12.34)


def test_load_transactions_skips_bad_row_with_warning(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("date,merchant,amount\n2026-01-01,OK,10.00\nnot-a-date,X,1\n", encoding="utf-8")
    rows, warnings = reconciler.load_transactions(path, "Src")
    assert len(rows) == 1
    assert any("line 3" in w for w in warnings)


def test_detect_duplicates_exact_cluster() -> None:
    src, _ = reconciler.mock_transaction_sets()
    detect = reconciler.detect_duplicates(src)
    assert any(item["count"] >= 2 for item in detect["exact"])


def test_build_report_text_smoke() -> None:
    src, ref = reconciler.mock_transaction_sets()
    report = reconciler.reconcile(src, ref)
    dup_s = reconciler.detect_duplicates(src)
    dup_r = reconciler.detect_duplicates(ref)
    text = reconciler.build_report_text(report, dup_s, dup_r)
    assert "Reconciliation Report" in text
    assert "Match rate (coverage vs larger file)" in text
    assert "Matched" in text and "Duplicates" in text


def test_reconcile_mock_sets_finds_matches() -> None:
    source, reference = reconciler.mock_transaction_sets()
    report = reconciler.reconcile(source, reference, fuzzy_threshold=0.72, date_tolerance=2, amount_tolerance=0.50)
    assert len(report["matched"]) >= 1
    assert report["source_count"] == len(source)
    assert report["reference_count"] == len(reference)


def test_run_reconciliation_mock_mode() -> None:
    result = reconciler.run_reconciliation(use_mock=True)
    assert "Reconciliation Report" in result["report_text"]
    assert result["report"]["match_rate"] >= 0.0


def test_exact_match_pass_pairs_identical_rows() -> None:
    source, _ = reconciler.mock_transaction_sets()
    dup = source[0]
    twin = {
        "date": dup["date"],
        "merchant": dup["merchant"],
        "amount": dup["amount"],
        "merchant_key": dup["merchant_key"],
        "amount_cents": dup["amount_cents"],
        "source_label": "ref",
        "line_number": 1,
    }
    matches, used_s, used_r = reconciler.exact_match_pass(source[:1], [twin])
    assert len(matches) == 1
    assert 0 in used_s and 0 in used_r
