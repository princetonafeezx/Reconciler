"""Tests for text normalization and similarity helpers."""

from __future__ import annotations

import pytest

from textutil import clean_text, similarity_ratio


def test_clean_text_strips_noise() -> None:
    assert clean_text("Starbucks & Co.") == "starbucks co"


def test_clean_text_hyphen_becomes_space() -> None:
    assert clean_text("Foo-Bar Shop") == "foo bar shop"


def test_clean_text_unicode_letters() -> None:
    assert clean_text("Café Müller") == "café müller"


def test_clean_text_empty() -> None:
    assert clean_text("") == ""
    assert clean_text("   ") == ""


def test_similarity_identical_and_fuzzy() -> None:
    assert similarity_ratio("starbucks", "starbucks") == 1.0
    assert similarity_ratio("starbuks", "starbucks") > 0.7


def test_similarity_both_empty() -> None:
    assert similarity_ratio("", "") == 1.0
    assert similarity_ratio("  ", " \t ") == 1.0


def test_similarity_one_empty() -> None:
    assert similarity_ratio("a", "") == 0.0
    assert similarity_ratio("", "b") == 0.0


def test_similarity_substring_shortcut() -> None:
    assert similarity_ratio("amazon", "amazon marketplace") == len("amazon") / len("amazon marketplace")
    assert similarity_ratio("amazon marketplace", "amazon") == len("amazon") / len("amazon marketplace")


def test_similarity_edit_distance_path() -> None:
    assert similarity_ratio("abc", "abx") < 1.0
    assert similarity_ratio("abc", "abx") > 0.0


def test_similarity_min_ratio_matches_full_when_high() -> None:
    left, right = "netflix", "netflx"
    full = similarity_ratio(left, right)
    guided = similarity_ratio(left, right, min_ratio=0.75)
    assert full >= 0.75
    assert guided == pytest.approx(full)


def test_similarity_min_ratio_zero_when_cannot_reach_threshold() -> None:
    left, right = "aaaaaaa", "bbbbbbb"
    full = similarity_ratio(left, right)
    assert full < 0.8
    assert similarity_ratio(left, right, min_ratio=0.8) == 0.0
