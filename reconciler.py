from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any, cast

from csv_columns import detect_columns as auto_detect_columns, role_score
from parsing import parse_amount, parse_date
from schemas import (
    DuplicateDetectionResult,
    ReconciliationPair,
    ReconciliationRecord,
    ReconciliationReport,
    RunReconciliationResult,
)
from storage import format_money, write_text_report
from textutil import clean_text, similarity_ratio

logger = logging.getLogger(__name__)

ColumnMap = dict[str, int | None]


def cents(amount: float) -> int:
    return int(round(amount * 100))

def detect_columns(headers: list[str], column_map: dict[str, Any] | None = None) -> ColumnMap:
    if not column_map:
        full = auto_detect_columns(headers)
        return {"date": full["date"], "merchant": full["merchant"], "amount": full["amount"]}

    n = len(headers)
    resolved: ColumnMap = {"date": None, "merchant": None, "amount": None}
    lowered = [header.lower().strip() for header in headers]

    for key in ("date", "merchant", "amount"):
        candidate = column_map.get(key)
        if candidate is None:
            continue
        if isinstance(candidate, int):
            if 0 <= candidate < n:
                resolved[key] = candidate
        else:
            candidate_lower = str(candidate).lower().strip()
            if candidate_lower in lowered:
                resolved[key] = lowered.index(candidate_lower)

    used = {idx for idx in resolved.values() if idx is not None}
    for role in ("date", "merchant", "amount"):
        if resolved[role] is not None:
            continue
        best_i: int | None = None
        best_v = -1.0
        for i in range(n):
            if i in used:
                continue
            score = role_score(headers[i], role)
            if score > best_v:
                best_v = score
                best_i = i
        if best_i is not None and best_v > 0:
            resolved[role] = best_i
            used.add(best_i)

    for role in ("date", "merchant", "amount"):
        if resolved[role] is None:
            for i in range(n):
                if i not in used:
                    resolved[role] = i
                    used.add(i)
                    break

    return resolved

def load_transactions(
    file_path: str | Path, label: str, column_map: dict[str, Any] | None = None
) -> tuple[list[ReconciliationRecord], list[str]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Could not find file: {path}")

    warnings: list[str] = []
    records: list[ReconciliationRecord] = []

    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            headers = next(reader)
        except StopIteration:
            return [], [f"{label} file was empty."]

        mapping = detect_columns(headers, column_map=column_map)

        for line_number, row in enumerate(reader, start=2):
            if not row or not any(cell.strip() for cell in row):
                continue
            try:
                di = mapping["date"]
                mi = mapping["merchant"]
                ai = mapping["amount"]
                if di is None or mi is None or ai is None:
                    raise ValueError("Row did not contain the mapped columns")
                if max(di, mi, ai) >= len(row):
                    raise ValueError("Row did not contain the mapped columns")
                raw_merchant = row[mi].strip()
                raw_amount = row[ai].strip()
                amount = parse_amount(raw_amount)
                record: ReconciliationRecord = {
                    "date": parse_date(row[di]),
                    "merchant": raw_merchant,
                    "merchant_key": clean_text(raw_merchant),
                    "amount": amount,
                    "amount_cents": cents(amount),
                    "source_label": label,
                    "line_number": line_number,
                }
                records.append(record)
            except (ValueError, TypeError, KeyError, IndexError) as error:
                warnings.append(f"{label} line {line_number} skipped: {error}")
    return records, warnings

def detect_duplicates(records: list[ReconciliationRecord], near_duplicate_days: int = 2) -> DuplicateDetectionResult:
    exact_groups: dict[tuple[Any, ...], list[ReconciliationRecord]] = {}
    near_groups: dict[tuple[Any, ...], list[ReconciliationRecord]] = {}
    exact_duplicates: list[dict[str, Any]] = []
    near_duplicates: list[dict[str, Any]] = []

    for record in records:
        exact_key = (record["merchant_key"], record["amount_cents"], record["date"])
        exact_groups.setdefault(exact_key, []).append(record)

        near_key = (record["merchant_key"], record["amount_cents"])
        near_groups.setdefault(near_key, []).append(record)

    for group in exact_groups.values():
        if len(group) > 1:
            exact_duplicates.append({"record": group[0], "count": len(group)})

    for group in near_groups.values():
        ordered = sorted(group, key=lambda item: item["date"])
        if len(ordered) < 2:
            continue
        for left_index in range(len(ordered) - 1):
            gap = abs((ordered[left_index + 1]["date"] - ordered[left_index]["date"]).days)
            if gap <= near_duplicate_days:
                near_duplicates.append({"record": ordered[left_index], "next_record": ordered[left_index + 1], "gap": gap})

    return cast(DuplicateDetectionResult, {"exact": exact_duplicates, "near": near_duplicates})

def build_confidence(name_similarity: float, date_gap: int, amount_gap_cents: int, date_tolerance: int, amount_tolerance_cents: int) -> float:
    date_component = 1.0 if date_tolerance == 0 and date_gap == 0 else max(0.0, 1.0 - (date_gap / max(1, date_tolerance + 1)))
    if amount_tolerance_cents == 0 and amount_gap_cents == 0:
        amount_component = 1.0
    else:
        amount_component = max(0.0, 1.0 - (amount_gap_cents / max(1, amount_tolerance_cents * 3)))
    score = (name_similarity * 0.5) + (date_component * 0.25) + (amount_component * 0.25)
    return round(score, 3)

def pair_result(
    source_record: ReconciliationRecord, reference_record: ReconciliationRecord, confidence: float, reason: str
) -> ReconciliationPair:
    return cast(
        ReconciliationPair,
        {
            "source": source_record,
            "reference": reference_record,
            "confidence": confidence,
            "reason": reason,
            "amount_delta": round(source_record["amount"] - reference_record["amount"], 2),
            "date_gap": abs((source_record["date"] - reference_record["date"]).days),
        },
    )




















if __name__ == "__main__":
    main()
