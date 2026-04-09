# ll_reconciler

Standalone **expense reconciliation**: compare two CSV transaction files (for example bank export vs personal ledger), run exact and fuzzy matching, detect duplicates, classify amount/date mismatches, and print or save a text report.

## Install

From this directory:

```bash
pip install -e ".[dev]"
```

Runtime dependency: `typing-extensions` (declared in `pyproject.toml`). Editable install registers the **`ll-reconciler`** console script and keeps imports working from the project root.

Requires **Python 3.10+**.

## Usage

### Interactive menu

```bash
python reconciler.py
# or, after install:
ll-reconciler
```

With no arguments, the interactive menu runs (load CSVs, mock data, thresholds, run, quit).

### Command line

```bash
python reconciler.py --source path/to/bank.csv --reference path/to/ledger.csv
python reconciler.py --mock
python reconciler.py --source a.csv --reference b.csv --export --output-dir ./out
```

| Option | Meaning |
|--------|---------|
| `--source` / `--reference` | Paths to the two CSVs (both required unless `--mock`). |
| `--mock` | Built-in sample data instead of files. |
| `--export` | Write `reconciliation_report.txt` (under `--output-dir` or the current directory). |
| `--fuzzy` | Merchant similarity threshold: **0–100 = percent** (default `80`), or a ratio in **(0, 1]** e.g. `0.72`. |
| `--date-tolerance` | Days apart still treated as matchable (default `2`). |
| `--amount-tolerance` | Dollar spread for compatible amounts (default `0.50`). |
| `-v` / `--verbose` | DEBUG logging (including reconcile counts). |
| `-q` / `--quiet` | Less log noise on stderr. |
| `--quiet-report` | Do not print the report to stdout (useful with `--export`). |
| `--menu` | Force the interactive menu even if other flags are present. |

### Python API

```python
from reconciler import run_reconciliation, reconcile, load_transactions

result = run_reconciliation(source_file="a.csv", reference_file="b.csv")
print(result["report_text"])
```

### Column detection

Headers are guessed from common names (`date`, `description`, `debit`, …). If a file is odd, pass a map into `load_transactions(..., column_map={"amount": 2, "date": 0, ...})` (indices or header strings). See `reconciler.detect_columns`.

## Performance

Fuzzy merchant matching uses a **capped Levenshtein** search when checking against your `--fuzzy` threshold, so pairs that cannot reach that similarity skip full edit-distance work. Results match the previous naive implementation for pairing decisions.

Very large files are still dominated by the **exact-merchant** pass (per-merchant candidate lists); fuzzy pass remains the heaviest stage for unmatched rows.

## Data directory (`storage` helpers)

Default data directory: `./reconciler_data`. Override with the environment variable **`RECONCILER_DATA_DIR`**. Optional helpers can read/write categorized CSV and JSON there; reconciliation reports from `--export` use the path you pass or the current working directory.

## Development

```bash
pip install -e ".[dev]"
python -m pytest tests -q
python -m mypy reconciler.py csv_columns.py parsing.py textutil.py storage.py schemas.py
python -m black .
python -m flake8 .
```

## Tests

```bash
python -m pytest tests -q
```

`pyproject.toml` sets `pythonpath = ["."]` so tests discover local modules without installing.
