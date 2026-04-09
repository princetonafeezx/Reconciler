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

def exact_match_pass(
    source_records: list[ReconciliationRecord], reference_records: list[ReconciliationRecord]
) -> tuple[list[ReconciliationPair], set[int], set[int]]:
    reference_lookup: dict[tuple[Any, ...], list[int]] = {}
    used_reference: set[int] = set()
    used_source: set[int] = set()
    matches: list[ReconciliationPair] = []

    for index, record in enumerate(reference_records):
        key = (record["date"], record["merchant_key"], record["amount_cents"])
        reference_lookup.setdefault(key, []).append(index)

    for source_index, source_record in enumerate(source_records):
        key = (source_record["date"], source_record["merchant_key"], source_record["amount_cents"])
        candidates = reference_lookup.get(key, [])
        while candidates and candidates[0] in used_reference:
            candidates.pop(0)
        if candidates:
            reference_index = candidates.pop(0)
            used_source.add(source_index)
            used_reference.add(reference_index)
            matches.append(pair_result(source_record, reference_records[reference_index], 1.0, "exact"))

    return matches, used_source, used_reference

def exact_merchant_pass(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    used_source: set[int],
    used_reference: set[int],
    date_tolerance: int,
    amount_tolerance_cents: int,
) -> dict[str, Any]:
    results: dict[str, list[Any]] = {"matched": [], "amount_mismatch": [], "date_mismatch": [], "suspicious": []}
    merchant_lookup: dict[str, list[int]] = {}
    for index, record in enumerate(reference_records):
        if index in used_reference:
            continue
        merchant_lookup.setdefault(record["merchant_key"], []).append(index)

    for source_index, source_record in enumerate(source_records):
        if source_index in used_source:
            continue
        candidates = merchant_lookup.get(source_record["merchant_key"], [])
        best = None
        best_type = None
        best_index = None

        for reference_index in candidates:
            if reference_index in used_reference:
                continue
            reference_record = reference_records[reference_index]
            date_gap = abs((source_record["date"] - reference_record["date"]).days)
            amount_gap_cents = abs(source_record["amount_cents"] - reference_record["amount_cents"])
            confidence = build_confidence(1.0, date_gap, amount_gap_cents, date_tolerance, amount_tolerance_cents)

            if date_gap <= date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "matched"
            elif date_gap <= date_tolerance and amount_gap_cents > amount_tolerance_cents:
                candidate_type = "amount_mismatch"
            elif date_gap > date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "date_mismatch"
            else:
                candidate_type = "suspicious"

            if best is None or confidence > best["confidence"]:
                best = pair_result(source_record, reference_record, confidence, "exact merchant")
                best_type = candidate_type
                best_index = reference_index

        if best is not None and best_type is not None and best_index is not None:
            results[best_type].append(best)
            used_source.add(source_index)
            used_reference.add(best_index)

    return results

def fuzzy_match_pass(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    used_source: set[int],
    used_reference: set[int],
    fuzzy_threshold: float,
    date_tolerance: int,
    amount_tolerance_cents: int,
) -> dict[str, Any]:
    results: dict[str, list[Any]] = {"matched": [], "amount_mismatch": [], "date_mismatch": [], "suspicious": []}

    for source_index, source_record in enumerate(source_records):
        if source_index in used_source:
            continue

        best_pair = None
        best_type = None
        best_reference_index = None

        for reference_index, reference_record in enumerate(reference_records):
            if reference_index in used_reference:
                continue

            if not source_record["merchant_key"] or not reference_record["merchant_key"]:
                name_similarity = 0.75 if (
                    abs((source_record["date"] - reference_record["date"]).days) <= date_tolerance
                    and abs(source_record["amount_cents"] - reference_record["amount_cents"]) <= amount_tolerance_cents
                ) else 0.0
            else:
                name_similarity = similarity_ratio(
                    source_record["merchant_key"],
                    reference_record["merchant_key"],
                    min_ratio=fuzzy_threshold,
                )

            if name_similarity < fuzzy_threshold:
                continue

            date_gap = abs((source_record["date"] - reference_record["date"]).days)
            amount_gap_cents = abs(source_record["amount_cents"] - reference_record["amount_cents"])
            confidence = build_confidence(name_similarity, date_gap, amount_gap_cents, date_tolerance, amount_tolerance_cents)

            if date_gap <= date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "matched"
            elif date_gap <= date_tolerance and amount_gap_cents > amount_tolerance_cents:
                candidate_type = "amount_mismatch"
            elif date_gap > date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "date_mismatch"
            else:
                candidate_type = "suspicious"

            if best_pair is None or confidence > best_pair["confidence"]:
                best_pair = pair_result(source_record, reference_record, confidence, "fuzzy merchant")
                best_type = candidate_type
                best_reference_index = reference_index

        if best_pair is not None and best_type is not None and best_reference_index is not None:
            results[best_type].append(best_pair)
            used_source.add(source_index)
            used_reference.add(best_reference_index)

    return results

def unmatched_records(records: list[ReconciliationRecord], used_indexes: set[int]) -> list[ReconciliationRecord]:
    leftovers: list[ReconciliationRecord] = []
    for index, record in enumerate(records):
        if index not in used_indexes:
            leftovers.append(record)
    return leftovers

def reconcile(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    fuzzy_threshold: float = 0.80,
    date_tolerance: int = 2,
    amount_tolerance: float = 0.50,
) -> ReconciliationReport:

    amount_tolerance_cents = cents(amount_tolerance)

    exact_matches, used_source, used_reference = exact_match_pass(source_records, reference_records)
    exact_merchant_results = exact_merchant_pass(
        source_records,
        reference_records,
        used_source,
        used_reference,
        date_tolerance=date_tolerance,
        amount_tolerance_cents=amount_tolerance_cents,
    )
    fuzzy_results = fuzzy_match_pass(
        source_records,
        reference_records,
        used_source,
        used_reference,
        fuzzy_threshold=fuzzy_threshold,
        date_tolerance=date_tolerance,
        amount_tolerance_cents=amount_tolerance_cents,
    )

    unmatched_source = unmatched_records(source_records, used_source)
    unmatched_reference = unmatched_records(reference_records, used_reference)

    source_keys = {(record["date"], record["merchant_key"]) for record in source_records}
    reference_keys = {(record["date"], record["merchant_key"]) for record in reference_records}
    shared_keys = source_keys & reference_keys
    source_only_keys = source_keys - reference_keys
    reference_only_keys = reference_keys - source_keys
    symmetric_difference_keys = source_keys ^ reference_keys

    matched_pairs = exact_matches + exact_merchant_results["matched"] + fuzzy_results["matched"]
    amount_mismatches = exact_merchant_results["amount_mismatch"] + fuzzy_results["amount_mismatch"]
    date_mismatches = exact_merchant_results["date_mismatch"] + fuzzy_results["date_mismatch"]
    suspicious = exact_merchant_results["suspicious"] + fuzzy_results["suspicious"]

    source_total = round(sum(record["amount"] for record in source_records), 2)
    reference_total = round(sum(record["amount"] for record in reference_records), 2)
    baseline_size = max(len(source_records), len(reference_records), 1)
    match_rate = (len(matched_pairs) / baseline_size) * 100

    logger.debug(
        "Reconcile summary: matched=%d amount_mismatch=%d date_mismatch=%d suspicious=%d "
        "unmatched_src=%d unmatched_ref=%d",
        len(matched_pairs),
        len(amount_mismatches),
        len(date_mismatches),
        len(suspicious),
        len(unmatched_source),
        len(unmatched_reference),
    )

    return cast(
        ReconciliationReport,
        {
            "matched": matched_pairs,
            "amount_mismatch": amount_mismatches,
            "date_mismatch": date_mismatches,
            "suspicious": suspicious,
            "unmatched_source": unmatched_source,
            "unmatched_reference": unmatched_reference,
            "set_summary": {
                "shared_keys": len(shared_keys),
                "source_only_keys": len(source_only_keys),
                "reference_only_keys": len(reference_only_keys),
                "symmetric_difference": len(symmetric_difference_keys),
            },
            "source_total": source_total,
            "reference_total": reference_total,
            "net_difference": round(source_total - reference_total, 2),
            "match_rate": round(match_rate, 1),
            "source_count": len(source_records),
            "reference_count": len(reference_records),
        },
    )

def mock_transaction_sets() -> tuple[list[ReconciliationRecord], list[ReconciliationRecord]]:
    source_rows: list[dict[str, Any]] = [
        {"date": date(2026, 3, 1), "merchant": "Whole Foods", "amount": 83.21},
        {"date": date(2026, 3, 2), "merchant": "Shell", "amount": 48.10},
        {"date": date(2026, 3, 4), "merchant": "Netflix", "amount": 15.49},
        {"date": date(2026, 3, 5), "merchant": "Starbucks", "amount": 6.25},
        {"date": date(2026, 3, 8), "merchant": "Amazon Marketplace", "amount": 120.00},
        {"date": date(2026, 3, 10), "merchant": "Starbucks", "amount": 6.25},
        {"date": date(2026, 3, 10), "merchant": "Starbucks", "amount": 6.25},
        {"date": date(2026, 3, 12), "merchant": "Walgreens", "amount": 18.45},
    ]
    reference_rows: list[dict[str, Any]] = [
        {"date": date(2026, 3, 1), "merchant": "Whole Foods", "amount": 83.21},
        {"date": date(2026, 3, 3), "merchant": "Shell Oil", "amount": 48.10},
        {"date": date(2026, 3, 4), "merchant": "Netflixx", "amount": 15.49},
        {"date": date(2026, 3, 5), "merchant": "Starbucks", "amount": 5.95},
        {"date": date(2026, 3, 8), "merchant": "Amazon Marketplace", "amount": 118.50},
        {"date": date(2026, 3, 12), "merchant": "Walgreens", "amount": 18.45},
        {"date": date(2026, 3, 15), "merchant": "Trader Joes", "amount": 64.88},
    ]

    for row in source_rows:
        row["merchant_key"] = clean_text(str(row["merchant"]))
        row["amount_cents"] = cents(float(row["amount"]))
        row["source_label"] = "source"
        row["line_number"] = 0
    for row in reference_rows:
        row["merchant_key"] = clean_text(str(row["merchant"]))
        row["amount_cents"] = cents(float(row["amount"]))
        row["source_label"] = "reference"
        row["line_number"] = 0
    return cast(list[ReconciliationRecord], source_rows), cast(list[ReconciliationRecord], reference_rows)

























if __name__ == "__main__":
    main()
