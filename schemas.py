"""Typed shapes for reconciliation data and optional categorized CSV rows."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TypedDict

from typing_extensions import NotRequired


class CategorizedRecord(TypedDict):
    """Single row when loading/saving categorized_transactions.csv via storage helpers."""

    date: str | date
    merchant: str
    amount: float
    category: str
    subcategory: NotRequired[str]
    confidence: NotRequired[float]
    match_type: NotRequired[str]


class ReconciliationRecord(TypedDict):
    """One normalized row from a reconciliation CSV (or mock data)."""

    date: date
    merchant: str
    merchant_key: str
    amount: float
    amount_cents: int
    source_label: str
    line_number: int


class ReconciliationPair(TypedDict):
    """A source row paired with a reference row and match metadata."""

    source: ReconciliationRecord
    reference: ReconciliationRecord
    confidence: float
    reason: str
    amount_delta: float
    date_gap: int


class ReconciliationSetSummary(TypedDict):
    """Counts of (date, merchant_key) keys across source vs reference."""

    shared_keys: int
    source_only_keys: int
    reference_only_keys: int
    symmetric_difference: int


class ReconciliationReport(TypedDict):
    """Return value of :func:`reconciler.reconcile`."""

    matched: list[ReconciliationPair]
    amount_mismatch: list[ReconciliationPair]
    date_mismatch: list[ReconciliationPair]
    suspicious: list[ReconciliationPair]
    unmatched_source: list[ReconciliationRecord]
    unmatched_reference: list[ReconciliationRecord]
    set_summary: ReconciliationSetSummary
    source_total: float
    reference_total: float
    net_difference: float
    match_rate: float
    source_count: int
    reference_count: int


class DuplicateExactItem(TypedDict):
    """One exact-duplicate cluster inside a single file."""

    record: ReconciliationRecord
    count: int


class DuplicateNearItem(TypedDict):
    """Two rows with same merchant/amount and dates within the near window."""

    record: ReconciliationRecord
    next_record: ReconciliationRecord
    gap: int


class DuplicateDetectionResult(TypedDict):
    """Output of :func:`reconciler.detect_duplicates`."""

    exact: list[DuplicateExactItem]
    near: list[DuplicateNearItem]


class RunReconciliationResult(TypedDict):
    """Return value of :func:`reconciler.run_reconciliation`."""

    report: ReconciliationReport
    report_text: str
    warnings: list[str]
    duplicate_source: DuplicateDetectionResult
    duplicate_reference: DuplicateDetectionResult
    output_path: Path | None
