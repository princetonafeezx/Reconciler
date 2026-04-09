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
























if __name__ == "__main__":
    main()
