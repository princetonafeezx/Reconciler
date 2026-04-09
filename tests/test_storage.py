"""Tests for data directory resolution and JSON loading."""

from __future__ import annotations

import warnings

import storage


def test_get_data_dir_env_override(tmp_path, monkeypatch) -> None:
    data = tmp_path / "mydata"
    monkeypatch.setenv("RECONCILER_DATA_DIR", str(data))
    resolved = storage.get_data_dir()
    assert resolved == data
    assert resolved.is_dir()


def test_get_data_dir_explicit_base(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("RECONCILER_DATA_DIR", raising=False)
    explicit = tmp_path / "explicit"
    resolved = storage.get_data_dir(base_dir=explicit)
    assert resolved == explicit


def test_load_categorized_transactions_warns_bad_amount(tmp_path) -> None:
    csv_path = tmp_path / "categorized_transactions.csv"
    csv_path.write_text(
        "date,merchant,amount,category,subcategory,confidence,match_type\n"
        "2024-01-01,Store,not-a-number,Shopping,Shopping,1.0,exact\n",
        encoding="utf-8",
    )
    records, warnings = storage.load_categorized_transactions(csv_path)
    assert len(records) == 1
    assert records[0]["amount"] == 0.0
    assert len(warnings) == 1
    assert "could not parse amount" in warnings[0]


def test_load_json_corrupt_returns_default(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")
    default = {"ok": True}
    with warnings.catch_warnings(record=True) as wrec:
        warnings.simplefilter("always")
        out = storage.load_json(path, default=default)
    assert out == default
    assert len(wrec) == 1
    assert "Invalid JSON" in str(wrec[0].message)


def test_load_json_array_returns_default(tmp_path) -> None:
    path = tmp_path / "list.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")
    default = {"x": 1}
    with warnings.catch_warnings(record=True) as wrec:
        warnings.simplefilter("always")
        out = storage.load_json(path, default=default)
    assert out == default
    assert len(wrec) == 1
    assert "not an object" in str(wrec[0].message)


def test_load_categorized_parses_amount_like_parse_amount(tmp_path) -> None:
    csv_path = tmp_path / "categorized_transactions.csv"
    csv_path.write_text(
        "date,merchant,amount,category,subcategory,confidence,match_type\n"
        "2024-01-01,Store,$(10.00),Shopping,Shopping,1.0,exact\n",
        encoding="utf-8",
    )
    records, warns = storage.load_categorized_transactions(csv_path)
    assert not warns
    assert records[0]["amount"] == 10.0


def test_save_json_atomic_no_leftover_tmp(tmp_path) -> None:
    path = tmp_path / "data.json"
    storage.save_json({"a": 1}, path)
    assert path.read_text(encoding="utf-8").strip().startswith("{")
    assert not list(tmp_path.glob(".*.tmp.*"))
