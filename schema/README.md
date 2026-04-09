# Schema folder for `princetonafeezx/Reconciler`

This folder contains simple JSON Schema files based on the repository's typed shapes in `schemas.py`
and the CSV/report behavior described in `reconciler.py`, `storage.py`, and `README.md`.

## Included schemas

- `TransactionInputRow.schema.json` — inferred minimal CSV-style row (`date`, `merchant`, `amount`)
- `CategorizedRecord.schema.json`
- `ReconciliationRecord.schema.json`
- `ReconciliationPair.schema.json`
- `ReconciliationSetSummary.schema.json`
- `ReconciliationReport.schema.json`
- `DuplicateExactItem.schema.json`
- `DuplicateNearItem.schema.json`
- `DuplicateDetectionResult.schema.json`
- `RunReconciliationResult.schema.json`
- `index.json` — manifest of the files above

## Notes

- The repository primarily uses Python `TypedDict` types, so these JSON Schemas are a documentation/
  interoperability layer rather than native runtime validation built into the app.
- `TransactionInputRow.schema.json` is intentionally simple and reflects the common CSV columns the tool
  expects to detect (`date`, `merchant`, `amount`).
- `output_path` in `RunReconciliationResult` is modeled as either a string path or `null`.
