"""CLI entry and argparse wiring."""

from __future__ import annotations

import pytest

import reconciler


def test_main_help_exits_zero() -> None:
    with pytest.raises(SystemExit) as exc:
        reconciler.main(["--help"])
    assert exc.value.code == 0


def test_cli_missing_reference_exits_2() -> None:
    with pytest.raises(SystemExit) as exc:
        reconciler.main(["--source", "only.csv"])
    assert exc.value.code == 2


def test_cli_mock_quiet_report_suppresses_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        reconciler.main(["--mock", "--quiet-report"])
    assert exc.value.code == 0
    assert "Reconciliation Report" not in capsys.readouterr().out


def test_cli_mock_prints_report_by_default(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        reconciler.main(["--mock"])
    assert exc.value.code == 0
    assert "Reconciliation Report" in capsys.readouterr().out


def test_fuzzy_threshold_from_percent() -> None:
    assert reconciler._fuzzy_threshold_from_percent(80.0) == pytest.approx(0.8)
    assert reconciler._fuzzy_threshold_from_percent(0.72) == pytest.approx(0.72)
