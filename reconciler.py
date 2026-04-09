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

























if __name__ == "__main__":
    main()
