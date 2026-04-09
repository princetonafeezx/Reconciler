"""File helpers for this app.

* **Reconciliation:** :func:`format_money`, :func:`write_text_report`, and the shared
  data directory used by default report paths.
* **Optional extras:** categorized-transaction CSV plus generic JSON load/save (same
  atomic-write pattern). Nothing else in this package depends on those paths.
"""

from __future__ import annotations

import csv
import json
import os
import uuid
import warnings
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

from parsing import parse_amount
from schemas import CategorizedRecord

CATEGORIZED_FIELDS = [
    "date",
    "merchant",
    "amount",
    "category",
    "subcategory",
    "confidence",
    "match_type",
]


def _atomic_write_file(path: Path, write: Callable[[Path], None]) -> None:
    """Write to a unique temp file in the same directory, then replace ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.tmp.{uuid.uuid4().hex}"
    try:
        write(tmp)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    try:
        os.replace(tmp, path)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise


def get_data_dir(base_dir: str | Path | None = None) -> Path:
    """Return the data directory and create it if needed.

    If ``base_dir`` is omitted, uses the ``RECONCILER_DATA_DIR`` environment
    variable when set; otherwise ``./reconciler_data`` under the current
    working directory.
    """
    if base_dir is not None:
        root = Path(base_dir)
    else:
        env = (os.environ.get("RECONCILER_DATA_DIR") or "").strip()
        if env:
            root = Path(env).expanduser()
        else:
            root = Path.cwd() / "reconciler_data"
    root.mkdir(parents=True, exist_ok=True)
    return root


def get_categorized_path(base_dir: str | Path | None = None) -> Path:
    return get_data_dir(base_dir) / "categorized_transactions.csv"


def get_report_path(base_dir: str | Path | None = None) -> Path:
    return get_data_dir(base_dir) / "reconciliation_summary.txt"


def format_money(amount: float) -> str:
    """Format a number as dollars."""
    return f"${amount:,.2f}"


def save_categorized_transactions(
    records: Sequence[CategorizedRecord],
    path: str | Path | None = None,
) -> Path:
    """Write categorized transactions to CSV."""
    output_path = Path(path) if path else get_categorized_path()

    def write_csv(tmp: Path) -> None:
        with tmp.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CATEGORIZED_FIELDS)
            writer.writeheader()
            for record in records:
                row: dict[str, Any] = {}
                for field in CATEGORIZED_FIELDS:
                    row[field] = record.get(field, "")
                writer.writerow(row)

    _atomic_write_file(output_path, write_csv)
    return output_path


def load_categorized_transactions(path: str | Path | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    """Read categorized transactions back from CSV.

    Returns ``(records, warnings)``. Warnings describe rows where numeric
    fields could not be parsed (values default to ``0.0``). Amounts use the
    same rules as :func:`parsing.parse_amount`.
    """
    input_path = Path(path) if path else get_categorized_path()
    if not input_path.exists():
        return [], []

    records: list[dict[str, Any]] = []
    load_warnings: list[str] = []
    with input_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row_index, row in enumerate(reader, start=2):
            if not row:
                continue
            if not any((v or "").strip() for v in row.values()):
                continue
            amount_cell = (row.get("amount") or "").strip()
            if not amount_cell:
                amount = 0.0
            else:
                try:
                    amount = parse_amount(amount_cell)
                except ValueError:
                    load_warnings.append(
                        f"CSV row {row_index}: could not parse amount {amount_cell!r}; using 0.00."
                    )
                    amount = 0.0
            confidence_text = row.get("confidence", "") or "0"
            try:
                confidence = float(confidence_text)
            except ValueError:
                load_warnings.append(
                    f"CSV row {row_index}: could not parse confidence {confidence_text!r}; using 0.0."
                )
                confidence = 0.0

            records.append(
                {
                    "date": row.get("date", ""),
                    "merchant": row.get("merchant", ""),
                    "amount": amount,
                    "category": row.get("category", "Unknown"),
                    "subcategory": row.get("subcategory", "Unknown"),
                    "confidence": confidence,
                    "match_type": row.get("match_type", "unknown"),
                }
            )
    return records, load_warnings


def save_json(data: dict[str, Any], path: str | Path) -> Path:
    """Save JSON data with readable indentation."""
    output_path = Path(path)

    def write_json(tmp: Path) -> None:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)

    _atomic_write_file(output_path, write_json)
    return output_path


def load_json(path: str | Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    """Load JSON object data or return a default if missing, invalid, or not a JSON object."""
    input_path = Path(path)
    if not input_path.exists():
        return {} if default is None else default
    fallback = {} if default is None else default
    try:
        with input_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except json.JSONDecodeError as exc:
        warnings.warn(f"Invalid JSON in {input_path}: {exc}; using default.", stacklevel=2)
        return fallback.copy()
    if not isinstance(raw, dict):
        warnings.warn(
            f"JSON in {input_path} is not an object (got {type(raw).__name__}); using default.",
            stacklevel=2,
        )
        return fallback.copy()
    return cast(dict[str, Any], raw)


def write_text_report(text: str, path: str | Path | None = None) -> Path:
    """Persist a text report to disk."""
    output_path = Path(path) if path else get_report_path()

    def write_txt(tmp: Path) -> None:
        tmp.write_text(text, encoding="utf-8")

    _atomic_write_file(output_path, write_txt)
    return output_path
