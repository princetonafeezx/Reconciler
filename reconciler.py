"""Expense discrepancy detector — compare two CSV ledgers and report differences."""

# Enable postponed evaluation of type annotations
from __future__ import annotations

# Import standard library modules for CLI arguments, file handling, logging, and CSV operations
import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any, cast

# Import logic from shared internal modules for column detection, parsing, and storage
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

# Set up logger for this module
logger = logging.getLogger(__name__)

# Type alias for mapping column names to their integer indices in a CSV row
ColumnMap = dict[str, int | None]

# Helper to convert a float amount to an integer number of cents for easier exact comparisons and grouping.
def cents(amount: float) -> int:
    # Round to the nearest cent and convert to integer to avoid floating point precision issues
    return int(round(amount * 100))


def detect_columns(headers: list[str], column_map: dict[str, Any] | None = None) -> ColumnMap:
    """Auto-detect or apply a custom map for date, merchant, and amount.

    When ``column_map`` is omitted, uses keyword scoring on headers (see :mod:`csv_columns`).
    When provided, those keys override auto-detection; any role still unmapped is chosen
    from remaining columns by score, then by first free column so indices stay unique.
    """
    # If no manual map is provided, use the automated detection logic from csv_columns
    if not column_map:
        full = auto_detect_columns(headers)
        return {"date": full["date"], "merchant": full["merchant"], "amount": full["amount"]}

    n = len(headers)
    # Initialize dictionary for results
    resolved: ColumnMap = {"date": None, "merchant": None, "amount": None}
    # Prepare lowercase headers for case-insensitive matching
    lowered = [header.lower().strip() for header in headers]

    # First Pass: Map indices or names provided in the user's custom column_map
    for key in ("date", "merchant", "amount"):
        candidate = column_map.get(key)
        if candidate is None:
            continue
        # If the candidate is an integer, treat it as a direct index
        if isinstance(candidate, int):
            if 0 <= candidate < n:
                resolved[key] = candidate
        else:
            # Otherwise, treat it as a string header name and find its index
            candidate_lower = str(candidate).lower().strip()
            if candidate_lower in lowered:
                resolved[key] = lowered.index(candidate_lower)

    # Keep track of indices already assigned to prevent duplicate column usage
    used = {idx for idx in resolved.values() if idx is not None}
    
    # Second Pass: For unmapped roles, try to find the best match using keyword scoring
    for role in ("date", "merchant", "amount"):
        if resolved[role] is not None:
            continue
        best_i: int | None = None
        best_v = -1.0
        # Check every available column
        for i in range(n):
            if i in used:
                continue
            # Score this specific header against the current role requirements
            score = role_score(headers[i], role)
            if score > best_v:
                best_v = score
                best_i = i
        # Assign if a valid match was found
        if best_i is not None and best_v > 0:
            resolved[role] = best_i
            used.add(best_i)

    # Third Pass: Greedy fallback for any role still missing (assign first available unused column)
    for role in ("date", "merchant", "amount"):
        if resolved[role] is None:
            for i in range(n):
                if i not in used:
                    resolved[role] = i
                    used.add(i)
                    break

    # Return the finalized mapping of roles to CSV indices
    return resolved


def load_transactions(
    file_path: str | Path, label: str, column_map: dict[str, Any] | None = None
) -> tuple[list[ReconciliationRecord], list[str]]:
    """Load transactions from a CSV and skip malformed rows with warnings."""
    path = Path(file_path)
    # Validate file existence
    if not path.exists():
        raise FileNotFoundError(f"Could not find file: {path}")

    warnings: list[str] = []
    records: list[ReconciliationRecord] = []

    # Open CSV with UTF-8-SIG to handle potential Excel BOM
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            # Extract header row
            headers = next(reader)
        except StopIteration:
            return [], [f"{label} file was empty."]

        # Determine column structure for this specific file
        mapping = detect_columns(headers, column_map=column_map)

        # Process data rows starting from line 2
        for line_number, row in enumerate(reader, start=2):
            # Skip empty lines
            if not row or not any(cell.strip() for cell in row):
                continue
            try:
                # Retrieve detected indices
                di = mapping["date"]
                mi = mapping["merchant"]
                ai = mapping["amount"]
                # Validate that we have all required columns for this row
                if di is None or mi is None or ai is None:
                    raise ValueError("Row did not contain the mapped columns")
                if max(di, mi, ai) >= len(row):
                    raise ValueError("Row did not contain the mapped columns")
                
                # Extract and sanitize values
                raw_merchant = row[mi].strip()
                raw_amount = row[ai].strip()
                amount = parse_amount(raw_amount)
                
                # Construct the record dictionary
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
                # Log a warning for malformed rows instead of crashing the process
                warnings.append(f"{label} line {line_number} skipped: {error}")
    return records, warnings


def detect_duplicates(records: list[ReconciliationRecord], near_duplicate_days: int = 2) -> DuplicateDetectionResult:
    """Find exact duplicates and near-duplicates inside one file."""
    # Dictionaries to group records by identifying keys
    exact_groups: dict[tuple[Any, ...], list[ReconciliationRecord]] = {}
    near_groups: dict[tuple[Any, ...], list[ReconciliationRecord]] = {}
    exact_duplicates: list[dict[str, Any]] = []
    near_duplicates: list[dict[str, Any]] = []

    # Iterate through all loaded records
    for record in records:
        # Define the key for an exact match: same merchant, same cent amount, same date
        exact_key = (record["merchant_key"], record["amount_cents"], record["date"])
        exact_groups.setdefault(exact_key, []).append(record)

        # Define the key for a near match: same merchant and amount, but potentially different date
        near_key = (record["merchant_key"], record["amount_cents"])
        near_groups.setdefault(near_key, []).append(record)

    # Identify groups where the exact same transaction appears more than once
    for group in exact_groups.values():
        if len(group) > 1:
            exact_duplicates.append({"record": group[0], "count": len(group)})

    # Identify groups where the same transaction happens within a tight time window
    for group in near_groups.values():
        # Sort by date to compare consecutive transactions
        ordered = sorted(group, key=lambda item: item["date"])
        if len(ordered) < 2:
            continue
        # Check the gap between chronological entries
        for left_index in range(len(ordered) - 1):
            gap = abs((ordered[left_index + 1]["date"] - ordered[left_index]["date"]).days)
            # If the transactions are too close in time, flag them
            if gap <= near_duplicate_days:
                near_duplicates.append({"record": ordered[left_index], "next_record": ordered[left_index + 1], "gap": gap})

    # Return grouped duplicate results
    return cast(DuplicateDetectionResult, {"exact": exact_duplicates, "near": near_duplicates})


def build_confidence(name_similarity: float, date_gap: int, amount_gap_cents: int, date_tolerance: int, amount_tolerance_cents: int) -> float:
    """Combine field closeness into one confidence score."""
    # Calculate score component based on date proximity
    date_component = 1.0 if date_tolerance == 0 and date_gap == 0 else max(0.0, 1.0 - (date_gap / max(1, date_tolerance + 1)))
    # Calculate score component based on amount proximity
    if amount_tolerance_cents == 0 and amount_gap_cents == 0:
        amount_component = 1.0
    else:
        # Scale the penalty relative to the allowed amount tolerance
        amount_component = max(0.0, 1.0 - (amount_gap_cents / max(1, amount_tolerance_cents * 3)))
    # Weighted average: merchant name is most important (50%), date and amount share the rest (25% each)
    score = (name_similarity * 0.5) + (date_component * 0.25) + (amount_component * 0.25)
    return round(score, 3)


def pair_result(
    source_record: ReconciliationRecord, reference_record: ReconciliationRecord, confidence: float, reason: str
) -> ReconciliationPair:
    """Create a standard paired-entry result dict."""
    # Return a structured pair with calculated differences
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
    """Pair exact matches with dictionary lookups."""
    # Mapping of match keys to indices in the reference file
    reference_lookup: dict[tuple[Any, ...], list[int]] = {}
    # Keep track of indices consumed in this pass
    used_reference: set[int] = set()
    used_source: set[int] = set()
    matches: list[ReconciliationPair] = []

    # Build a lookup table for the reference file
    for index, record in enumerate(reference_records):
        key = (record["date"], record["merchant_key"], record["amount_cents"])
        reference_lookup.setdefault(key, []).append(index)

    # Attempt to find exact matches for each source record
    for source_index, source_record in enumerate(source_records):
        key = (source_record["date"], source_record["merchant_key"], source_record["amount_cents"])
        candidates = reference_lookup.get(key, [])
        # Ensure we don't double-count a reference row
        while candidates and candidates[0] in used_reference:
            candidates.pop(0)
        # If a match exists, link them
        if candidates:
            reference_index = candidates.pop(0)
            used_source.add(source_index)
            used_reference.add(reference_index)
            matches.append(pair_result(source_record, reference_records[reference_index], 1.0, "exact"))

    # Return matches and the sets of indices that were consumed
    return matches, used_source, used_reference

# Second pass: for anything still unmatched, try to pair by exact merchant key and close date/amount, categorizing the type of mismatch.
def exact_merchant_pass(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    used_source: set[int],
    used_reference: set[int],
    date_tolerance: int,
    amount_tolerance_cents: int,
) -> dict[str, Any]:
    """Compare remaining records with the same merchant key."""
    # Categorized results for this matching pass
    results: dict[str, list[Any]] = {"matched": [], "amount_mismatch": [], "date_mismatch": [], "suspicious": []}
    # Group available reference indices by their merchant key
    merchant_lookup: dict[str, list[int]] = {}
    for index, record in enumerate(reference_records):
        if index in used_reference:
            continue
        merchant_lookup.setdefault(record["merchant_key"], []).append(index)

    # Search for matches among remaining source records
    for source_index, source_record in enumerate(source_records):
        if source_index in used_source:
            continue
        candidates = merchant_lookup.get(source_record["merchant_key"], [])
        best = None
        best_type = None
        best_index = None

        # Compare with each reference candidate sharing the same merchant key
        for reference_index in candidates:
            if reference_index in used_reference:
                continue
            reference_record = reference_records[reference_index]
            # Calculate gaps between the two records
            date_gap = abs((source_record["date"] - reference_record["date"]).days)
            amount_gap_cents = abs(source_record["amount_cents"] - reference_record["amount_cents"])
            # Generate a score for this specific pairing
            confidence = build_confidence(1.0, date_gap, amount_gap_cents, date_tolerance, amount_tolerance_cents)

            # Determine the classification of the match based on gaps
            if date_gap <= date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "matched"
            elif date_gap <= date_tolerance and amount_gap_cents > amount_tolerance_cents:
                candidate_type = "amount_mismatch"
            elif date_gap > date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "date_mismatch"
            else:
                candidate_type = "suspicious"

            # Keep the highest confidence candidate for this source record
            if best is None or confidence > best["confidence"]:
                best = pair_result(source_record, reference_record, confidence, "exact merchant")
                best_type = candidate_type
                best_index = reference_index

        # If a candidate was found, record the match and consume the indices
        if best is not None and best_type is not None and best_index is not None:
            results[best_type].append(best)
            used_source.add(source_index)
            used_reference.add(best_index)

    return results

# Final pass: fuzzy match on anything still unmatched, with a confidence score and categorized by which fields were close vs not.
def fuzzy_match_pass(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    used_source: set[int],
    used_reference: set[int],
    fuzzy_threshold: float,
    date_tolerance: int,
    amount_tolerance_cents: int,
) -> dict[str, Any]:
    """Try fuzzy matching on anything still unmatched."""
    # Categorized results for this matching pass
    results: dict[str, list[Any]] = {"matched": [], "amount_mismatch": [], "date_mismatch": [], "suspicious": []}

    # Iterate through unmatched source records
    for source_index, source_record in enumerate(source_records):
        if source_index in used_source:
            continue

        best_pair = None
        best_type = None
        best_reference_index = None

        # Compare against every unmatched reference record (n^2 complexity)
        for reference_index, reference_record in enumerate(reference_records):
            if reference_index in used_reference:
                continue

            # If either merchant name is missing, only allow a match if date and amount are perfect
            if not source_record["merchant_key"] or not reference_record["merchant_key"]:
                name_similarity = 0.75 if (
                    abs((source_record["date"] - reference_record["date"]).days) <= date_tolerance
                    and abs(source_record["amount_cents"] - reference_record["amount_cents"]) <= amount_tolerance_cents
                ) else 0.0
            else:
                # Perform fuzzy string comparison on the merchant names
                name_similarity = similarity_ratio(
                    source_record["merchant_key"],
                    reference_record["merchant_key"],
                    min_ratio=fuzzy_threshold,
                )

            # Skip if names aren't similar enough
            if name_similarity < fuzzy_threshold:
                continue

            # Calculate date and amount discrepancies
            date_gap = abs((source_record["date"] - reference_record["date"]).days)
            amount_gap_cents = abs(source_record["amount_cents"] - reference_record["amount_cents"])
            # Generate total confidence score
            confidence = build_confidence(name_similarity, date_gap, amount_gap_cents, date_tolerance, amount_tolerance_cents)

            # Determine match category
            if date_gap <= date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "matched"
            elif date_gap <= date_tolerance and amount_gap_cents > amount_tolerance_cents:
                candidate_type = "amount_mismatch"
            elif date_gap > date_tolerance and amount_gap_cents <= amount_tolerance_cents:
                candidate_type = "date_mismatch"
            else:
                candidate_type = "suspicious"

            # Select the strongest candidate for this source record
            if best_pair is None or confidence > best_pair["confidence"]:
                best_pair = pair_result(source_record, reference_record, confidence, "fuzzy merchant")
                best_type = candidate_type
                best_reference_index = reference_index

        # Store the match and consume indices
        if best_pair is not None and best_type is not None and best_reference_index is not None:
            results[best_type].append(best_pair)
            used_source.add(source_index)
            used_reference.add(best_reference_index)

    return results

# Helper to collect all records that were never paired in any pass, for the final "unmatched" buckets in the report.
def unmatched_records(records: list[ReconciliationRecord], used_indexes: set[int]) -> list[ReconciliationRecord]:
    """Return every record that never got paired."""
    leftovers: list[ReconciliationRecord] = []
    # Identify records whose index was not recorded in the 'used' sets
    for index, record in enumerate(records):
        if index not in used_indexes:
            leftovers.append(record)
    return leftovers

# Main reconciliation function that runs all passes and compiles the final report.
def reconcile(
    source_records: list[ReconciliationRecord],
    reference_records: list[ReconciliationRecord],
    fuzzy_threshold: float = 0.80,
    date_tolerance: int = 2,
    amount_tolerance: float = 0.50,
) -> ReconciliationReport:
    """Run exact, exact-merchant, then fuzzy matching.

    ``match_rate`` is ``100 * len(matched_pairs) / max(len(source), len(reference), 1)`` —
    a *coverage* fraction of the larger file that ended in the "matched" bucket, not precision,
    recall, or dollar accuracy.
    """
    # Convert dollar tolerance to cents for internal logic
    amount_tolerance_cents = cents(amount_tolerance)

    # Pass 1: Find transactions that are identical across both files
    exact_matches, used_source, used_reference = exact_match_pass(source_records, reference_records)
    
    # Pass 2: Find transactions with same merchant but small discrepancies in date or amount
    exact_merchant_results = exact_merchant_pass(
        source_records,
        reference_records,
        used_source,
        used_reference,
        date_tolerance=date_tolerance,
        amount_tolerance_cents=amount_tolerance_cents,
    )
    
    # Pass 3: Find transactions with typos or different naming conventions (e.g. "Shell" vs "Shell Oil")
    fuzzy_results = fuzzy_match_pass(
        source_records,
        reference_records,
        used_source,
        used_reference,
        fuzzy_threshold=fuzzy_threshold,
        date_tolerance=date_tolerance,
        amount_tolerance_cents=amount_tolerance_cents,
    )

    # Identify transactions that were never matched in any of the above passes
    unmatched_source = unmatched_records(source_records, used_source)
    unmatched_reference = unmatched_records(reference_records, used_reference)

    # Calculate set-based summary statistics for metadata
    source_keys = {(record["date"], record["merchant_key"]) for record in source_records}
    reference_keys = {(record["date"], record["merchant_key"]) for record in reference_records}
    shared_keys = source_keys & reference_keys
    source_only_keys = source_keys - reference_keys
    reference_only_keys = reference_keys - source_keys
    symmetric_difference_keys = source_keys ^ reference_keys

    # Consolidate results from different passes into final buckets
    matched_pairs = exact_matches + exact_merchant_results["matched"] + fuzzy_results["matched"]
    amount_mismatches = exact_merchant_results["amount_mismatch"] + fuzzy_results["amount_mismatch"]
    date_mismatches = exact_merchant_results["date_mismatch"] + fuzzy_results["date_mismatch"]
    suspicious = exact_merchant_results["suspicious"] + fuzzy_results["suspicious"]

    # Calculate financial totals
    source_total = round(sum(record["amount"] for record in source_records), 2)
    reference_total = round(sum(record["amount"] for record in reference_records), 2)
    baseline_size = max(len(source_records), len(reference_records), 1)
    # Match rate indicates how many items from the larger file were successfully reconciled
    match_rate = (len(matched_pairs) / baseline_size) * 100

    # Log summary of current run to debug stream
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

    # Compile the comprehensive reconciliation report dictionary
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

# Helper to create mock datasets with intentional discrepancies for testing the reconciliation logic without needing real files.
def mock_transaction_sets() -> tuple[list[ReconciliationRecord], list[ReconciliationRecord]]:
    """Create two sample datasets with intentional discrepancies."""
    # Source data representing a bank statement or export
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
    # Reference data representing a personal tracker or budget app
    reference_rows: list[dict[str, Any]] = [
        {"date": date(2026, 3, 1), "merchant": "Whole Foods", "amount": 83.21},
        {"date": date(2026, 3, 3), "merchant": "Shell Oil", "amount": 48.10},
        {"date": date(2026, 3, 4), "merchant": "Netflixx", "amount": 15.49},
        {"date": date(2026, 3, 5), "merchant": "Starbucks", "amount": 5.95},
        {"date": date(2026, 3, 8), "merchant": "Amazon Marketplace", "amount": 118.50},
        {"date": date(2026, 3, 12), "merchant": "Walgreens", "amount": 18.45},
        {"date": date(2026, 3, 15), "merchant": "Trader Joes", "amount": 64.88},
    ]

    # Pre-process source rows into ReconciliationRecord format
    for row in source_rows:
        row["merchant_key"] = clean_text(str(row["merchant"]))
        row["amount_cents"] = cents(float(row["amount"]))
        row["source_label"] = "source"
        row["line_number"] = 0
    # Pre-process reference rows into ReconciliationRecord format
    for row in reference_rows:
        row["merchant_key"] = clean_text(str(row["merchant"]))
        row["amount_cents"] = cents(float(row["amount"]))
        row["source_label"] = "reference"
        row["line_number"] = 0
    # Cast and return the simulated records
    return cast(list[ReconciliationRecord], source_rows), cast(list[ReconciliationRecord], reference_rows)


# Helper to write the mock datasets to CSV files for testing the file loading and reconciliation end-to-end.
def export_mock_csvs(output_dir: str | Path | None = None) -> tuple[Path, Path]:
    """Write mock source/reference CSVs to the working directory."""
    # Generate the simulated data
    source_rows, reference_rows = mock_transaction_sets()
    folder = Path(output_dir) if output_dir else Path.cwd()
    # Create directory if it doesn't exist
    folder.mkdir(parents=True, exist_ok=True)
    source_path = folder / "mock_source.csv"
    reference_path = folder / "mock_reference.csv"

    # Write source and reference records to separate CSV files
    for target_path, rows in ((source_path, source_rows), (reference_path, reference_rows)):
        with target_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            # Write standard transaction headers
            writer.writerow(["date", "merchant", "amount"])
            for row in rows:
                writer.writerow([row["date"].isoformat(), row["merchant"], f"{row['amount']:.2f}"])
    return source_path, reference_path


# Helper to format a paired entry line for the text report, showing source and reference details side by side with confidence.
def format_pair_line(pair: ReconciliationPair) -> str:
    # Unpack records
    source = pair["source"]
    reference = pair["reference"]
    # Return a double-column string representation of the match
    return (
        f"{source['date'].isoformat()} {source['merchant']} {format_money(source['amount'])}  ||  "
        f"{reference['date'].isoformat()} {reference['merchant']} {format_money(reference['amount'])}  "
        f"[confidence {pair['confidence']:.2f}]"
    )


# Build a readable text report from the reconciliation results, including summaries and details for each category.
def build_report_text(
    report: ReconciliationReport, duplicate_source: DuplicateDetectionResult, duplicate_reference: DuplicateDetectionResult
) -> str:
    """Turn reconciliation results into a readable text report."""
    lines = []
    # Report Header
    lines.append("Reconciliation Report")
    lines.append("=" * 40)
    # High-level summary stats
    lines.append(
        f"Source transactions: {report['source_count']} | "
        f"Reference transactions: {report['reference_count']} | "
        f"Match rate (coverage vs larger file): {report['match_rate']:.1f}%"
    )
    # Calculate and display total discrepancies count
    lines.append(
        f"Discrepancies: {len(report['amount_mismatch']) + len(report['date_mismatch']) + len(report['suspicious']) + len(report['unmatched_source']) + len(report['unmatched_reference'])}"
    )
    # Set-theory metadata
    lines.append(
        f"Set summary - shared: {report['set_summary']['shared_keys']}, "
        f"source only: {report['set_summary']['source_only_keys']}, "
        f"reference only: {report['set_summary']['reference_only_keys']}, "
        f"symmetric diff: {report['set_summary']['symmetric_difference']}"
    )
    # Financial reconciliation summary
    lines.append(
        f"Grand totals - source: {format_money(report['source_total'])}, "
        f"reference: {format_money(report['reference_total'])}, "
        f"net difference: {format_money(report['net_difference'])}"
    )
    lines.append("")

    # Section for Perfect Matches
    lines.append("Matched")
    lines.append("-" * 40)
    if report["matched"]:
        for pair in report["matched"]:
            lines.append(format_pair_line(pair))
    else:
        lines.append("No confirmed matches.")
    lines.append("")

    # Section for matching Merchants with different amounts
    lines.append("Amount Mismatches")
    lines.append("-" * 40)
    if report["amount_mismatch"]:
        for pair in report["amount_mismatch"]:
            lines.append(f"{format_pair_line(pair)} | delta {format_money(pair['amount_delta'])}")
    else:
        lines.append("No amount mismatches.")
    lines.append("")

    # Section for matching Merchants/Amounts with different dates
    lines.append("Date Mismatches")
    lines.append("-" * 40)
    if report["date_mismatch"]:
        for pair in report["date_mismatch"]:
            lines.append(f"{format_pair_line(pair)} | date gap {pair['date_gap']} day(s)")
    else:
        lines.append("No date mismatches.")
    lines.append("")

    # Section for low-confidence fuzzy matches
    lines.append("Suspicious Entries")
    lines.append("-" * 40)
    if report["suspicious"]:
        for pair in report["suspicious"]:
            lines.append(format_pair_line(pair))
    else:
        lines.append("No suspicious fuzzy matches.")
    lines.append("")

    # List items only present in the source file
    lines.append("Unmatched Source Only")
    lines.append("-" * 40)
    if report["unmatched_source"]:
        for row in report["unmatched_source"]:
            lines.append(f"{row['date'].isoformat()} {row['merchant']} {format_money(row['amount'])}")
    else:
        lines.append("None.")
    lines.append("")

    # List items only present in the reference file
    lines.append("Unmatched Reference Only")
    lines.append("-" * 40)
    if report["unmatched_reference"]:
        for row in report["unmatched_reference"]:
            lines.append(f"{row['date'].isoformat()} {row['merchant']} {format_money(row['amount'])}")
    else:
        lines.append("None.")
    lines.append("")

    # Report on internal file duplicates
    lines.append("Duplicates")
    lines.append("-" * 40)
    if duplicate_source["exact"] or duplicate_reference["exact"] or duplicate_source["near"] or duplicate_reference["near"]:
        for label, duplicate_report in (("Source", duplicate_source), ("Reference", duplicate_reference)):
            # List identical repeats
            for exact_item in duplicate_report["exact"]:
                row = exact_item["record"]
                lines.append(
                    f"{label} exact duplicate: {row['merchant']} {format_money(row['amount'])} x{exact_item['count']}"
                )
            # List temporal clusters
            for near_item in duplicate_report["near"]:
                row = near_item["record"]
                lines.append(
                    f"{label} near duplicate: {row['merchant']} {format_money(row['amount'])} within {near_item['gap']} day(s)"
                )
    else:
        lines.append("No duplicates flagged.")

    # Combine all segments into one string
    return "\n".join(lines)


def run_reconciliation(
    source_file: str | Path | None = None,
    reference_file: str | Path | None = None,
    fuzzy_threshold: float = 0.80,
    date_tolerance: int = 2,
    amount_tolerance: float = 0.50,
    use_mock: bool = False,
    export_report: bool = False,
    output_dir: str | Path | None = None,
) -> RunReconciliationResult:
    """Public helper used by the unified CLI."""
    warnings: list[str] = []
    # Source data from mock generation
    if use_mock:
        source_records, reference_records = mock_transaction_sets()
    else:
        # Or source data from external CSV files
        if source_file is None or reference_file is None:
            raise ValueError("Both source and reference files are required unless mock mode is enabled.")
        source_records, source_warnings = load_transactions(source_file, "Source")
        reference_records, reference_warnings = load_transactions(reference_file, "Reference")
        warnings.extend(source_warnings)
        warnings.extend(reference_warnings)

    # Perform internal file integrity checks
    duplicate_source = detect_duplicates(source_records)
    duplicate_reference = detect_duplicates(reference_records)
    
    # Execute the cross-file comparison logic
    report = reconcile(
        source_records,
        reference_records,
        fuzzy_threshold=fuzzy_threshold,
        date_tolerance=date_tolerance,
        amount_tolerance=amount_tolerance,
    )
    # Convert data results into a string report
    report_text = build_report_text(report, duplicate_source, duplicate_reference)

    output_path = None
    # Optionally save the report to a file
    if export_report:
        output_path = write_text_report(report_text, Path(output_dir or Path.cwd()) / "reconciliation_report.txt")

    # Return comprehensive result package
    return cast(
        RunReconciliationResult,
        {
            "report": report,
            "report_text": report_text,
            "warnings": warnings,
            "duplicate_source": duplicate_source,
            "duplicate_reference": duplicate_reference,
            "output_path": output_path,
        },
    )


def menu() -> None:
    """Interactive reconciliation menu."""
    valid_choices = {"1", "2", "3", "4", "5"}
    current_source = None
    current_reference = None
    fuzzy_threshold = 0.80
    date_tolerance = 2
    amount_tolerance = 0.50
    last_result = None

    # Application loop for the terminal interface
    while True:
        print()
        print("Expense Reconciliation")
        print("1. Load two files")
        print("2. Generate mock data")
        print("3. Run reconciliation")
        print("4. Adjust thresholds")
        print("5. Quit")
        choice = input("Choose an option: ").strip()
        # Input validation
        if choice not in valid_choices:
            print("Please choose one of the listed options.")
            continue

        # Logic for Option 1: Store file paths for processing
        if choice == "1":
            current_source = input("Source CSV path: ").strip()
            current_reference = input("Reference CSV path: ").strip()
            print("File paths saved.")

        # Logic for Option 2: Create local test CSVs
        elif choice == "2":
            source_path, reference_path = export_mock_csvs()
            current_source = str(source_path)
            current_reference = str(reference_path)
            print(f"Mock files written to {source_path} and {reference_path}.")

        # Logic for Option 3: Perform comparison and print report
        elif choice == "3":
            try:
                # Use current files or fallback to internal mock data
                if current_source and current_reference:
                    last_result = run_reconciliation(
                        source_file=current_source,
                        reference_file=current_reference,
                        fuzzy_threshold=fuzzy_threshold,
                        date_tolerance=date_tolerance,
                        amount_tolerance=amount_tolerance,
                    )
                else:
                    last_result = run_reconciliation(
                        use_mock=True,
                        fuzzy_threshold=fuzzy_threshold,
                        date_tolerance=date_tolerance,
                        amount_tolerance=amount_tolerance,
                    )
                    print("No files were loaded, so mock data was used.")
                # Print any parsing warnings
                for warning in last_result["warnings"]:
                    print(warning)
                # Display the full report text
                print(last_result["report_text"])
                # Offer to save the report to disk
                export = input("Export this report to a text file too? (y/n): ").strip().lower()
                if export == "y":
                    rerun = run_reconciliation(
                        source_file=current_source,
                        reference_file=current_reference,
                        fuzzy_threshold=fuzzy_threshold,
                        date_tolerance=date_tolerance,
                        amount_tolerance=amount_tolerance,
                        use_mock=(current_source is None or current_reference is None),
                        export_report=True,
                    )
                    print(f"Report exported to {rerun['output_path']}")
            except (ValueError, FileNotFoundError, OSError, UnicodeDecodeError, KeyError, TypeError, IndexError) as error:
                # Handle application errors gracefully
                print(f"Could not run reconciliation: {error}")

        # Logic for Option 4: Modify matching sensitivity parameters
        elif choice == "4":
            fuzzy_text = input(f"Fuzzy threshold 0-100 [{int(fuzzy_threshold * 100)}]: ").strip()
            date_text = input(f"Date tolerance days [{date_tolerance}]: ").strip()
            amount_text = input(f"Amount tolerance dollars [{amount_tolerance}]: ").strip()
            try:
                # Update settings if user provides new values
                if fuzzy_text:
                    fuzzy_threshold = float(fuzzy_text) / 100
                if date_text:
                    date_tolerance = int(date_text)
                if amount_text:
                    amount_tolerance = float(amount_text)
                print("Thresholds updated.")
            except ValueError:
                print("One of those values was invalid, so the old settings stayed in place.")

        # Logic for Option 5: Termination
        elif choice == "5":
            print("Exiting reconciler.")
            break


def _build_arg_parser() -> argparse.ArgumentParser:
    # Set up command line interface arguments and descriptions
    parser = argparse.ArgumentParser(
        description="Compare two transaction CSVs and report matches, mismatches, and duplicates.",
    )
    # Menu flag
    parser.add_argument(
        "--menu",
        action="store_true",
        help="Open the interactive menu (default when no other CLI flags are used).",
    )
    # File inputs
    parser.add_argument("--source", type=Path, metavar="PATH", help="Source CSV (e.g. bank export).")
    parser.add_argument("--reference", type=Path, metavar="PATH", help="Reference CSV (e.g. personal ledger).")
    # Behavior flags
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use built-in sample data instead of files.",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Write reconciliation_report.txt to --output-dir or the current directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for --export (default: current working directory).",
    )
    # Tuning parameters
    parser.add_argument(
        "--fuzzy",
        type=float,
        default=80.0,
        metavar="PCT",
        help="Merchant fuzzy-match threshold as percent 0–100 (default: 80).",
    )
    parser.add_argument(
        "--date-tolerance",
        type=int,
        default=2,
        metavar="DAYS",
        help="Match if dates are within this many days (default: 2).",
    )
    parser.add_argument(
        "--amount-tolerance",
        type=float,
        default=0.50,
        metavar="USD",
        help="Treat amounts within this dollar spread as compatible (default: 0.50).",
    )
    # Logging controls
    parser.add_argument("-v", "--verbose", action="store_true", help="Log debug details to stderr.")
    parser.add_argument("-q", "--quiet", action="store_true", help="Less logging to stderr (warnings and errors only).")
    parser.add_argument(
        "--quiet-report",
        action="store_true",
        help="Do not print the report to stdout (still writes a file when using --export).",
    )
    return parser


def _configure_logging(verbose: bool, quiet: bool) -> None:
    # Set global logging levels based on user preference
    if quiet:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s", force=True)
        return
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s", force=True)


def _fuzzy_threshold_from_percent(value: float) -> float:
    # Convert user percentage input (e.g. 85) to decimal (0.85) for matching logic
    if value > 1.0:
        return max(0.0, min(1.0, value / 100.0))
    return max(0.0, min(1.0, value))


def run_cli_args(args: argparse.Namespace) -> int:
    """Run one reconciliation from parsed CLI args; return process exit code."""
    # Apply logging settings
    _configure_logging(args.verbose, args.quiet)
    if args.quiet and args.verbose:
        logger.warning("Both --quiet and --verbose; using quiet logging levels.")

    # Determine if user wants to use interactive mode or command line mode
    use_cli = args.source is not None or args.reference is not None or args.mock or args.export
    if args.menu or not use_cli:
        menu()
        return 0

    # Validate CLI arguments combination
    if args.mock:
        if args.source is not None or args.reference is not None:
            logger.error("Do not combine --mock with --source/--reference.")
            return 2
    else:
        if args.source is None or args.reference is None:
            logger.error("Provide both --source and --reference, or use --mock.")
            return 2

    # Process and execute reconciliation
    fuzzy = _fuzzy_threshold_from_percent(args.fuzzy)
    try:
        result = run_reconciliation(
            source_file=args.source,
            reference_file=args.reference,
            fuzzy_threshold=fuzzy,
            date_tolerance=args.date_tolerance,
            amount_tolerance=args.amount_tolerance,
            use_mock=args.mock,
            export_report=args.export,
            output_dir=args.output_dir,
        )
    except (ValueError, FileNotFoundError, OSError, UnicodeDecodeError) as exc:
        logger.error("%s", exc)
        return 1

    # Output warnings to the user
    for warning in result["warnings"]:
        logger.warning("%s", warning)

    # Output the report text unless quieted
    if not args.quiet_report:
        print(result["report_text"])

    # Log report file location if exported
    if args.export and result["output_path"] is not None:
        logger.info("Report file: %s", result["output_path"])

    # Success code
    return 0


def main(argv: list[str] | None = None) -> None:
    """Entry point: CLI when arguments are present, otherwise interactive menu."""
    # Grab command line arguments if not explicitly provided
    if argv is None:
        argv = sys.argv[1:]
    # Default to menu if no flags are passed
    if not argv:
        menu()
        return

    # Parse and run
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    code = run_cli_args(args)
    # Exit with appropriate status code
    raise SystemExit(code)


# Trigger main if run as a script
if __name__ == "__main__":
    main()