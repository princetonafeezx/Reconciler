"""Text normalization and fuzzy string similarity for merchant matching."""

from __future__ import annotations

import math


def clean_text(text: str) -> str:
    """Normalize text so fuzzy matching is more stable.

    Lowercases, keeps letters and digits, turns whitespace and punctuation (anything
    that is not alphanumeric) into a single ASCII space, then collapses runs of
    spaces. ``&`` and ``/`` are treated like punctuation (word separators), not kept
    as symbols.
    """
    parts: list[str] = []
    for char in (text or "").lower():
        if char.isalnum():
            parts.append(char)
        else:
            parts.append(" ")
    return " ".join("".join(parts).split())


def _levenshtein_distance(left: str, right: str) -> int:
    """Compute edit distance (internal helper for :func:`similarity_ratio`)."""
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)

    previous_row = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current_row = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            insert_cost = current_row[right_index - 1] + 1
            delete_cost = previous_row[right_index] + 1
            replace_cost = previous_row[right_index - 1] + (0 if left_char == right_char else 1)
            current_row.append(min(insert_cost, delete_cost, replace_cost))
        previous_row = current_row
    return previous_row[-1]


def _levenshtein_distance_capped(left: str, right: str, max_dist: int) -> int:
    """Edit distance when it is at most ``max_dist``; otherwise return ``max_dist + 1``."""
    if max_dist < 0:
        return max_dist + 1
    if left == right:
        return 0
    if not left:
        return len(right) if len(right) <= max_dist else max_dist + 1
    if not right:
        return len(left) if len(left) <= max_dist else max_dist + 1
    if abs(len(left) - len(right)) > max_dist:
        return max_dist + 1

    previous_row = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current_row = [left_index]
        row_min = left_index
        for right_index, right_char in enumerate(right, start=1):
            insert_cost = current_row[right_index - 1] + 1
            delete_cost = previous_row[right_index] + 1
            replace_cost = previous_row[right_index - 1] + (0 if left_char == right_char else 1)
            cost = min(insert_cost, delete_cost, replace_cost)
            current_row.append(cost)
            if cost < row_min:
                row_min = cost
        if row_min > max_dist:
            return max_dist + 1
        previous_row = current_row
    dist = previous_row[-1]
    return dist if dist <= max_dist else max_dist + 1


def similarity_ratio(left: str, right: str, *, min_ratio: float | None = None) -> float:
    """Return a 0–1 similarity score after applying :func:`clean_text` to both sides.

    * Both empty after cleaning → ``1.0``; exactly one empty → ``0.0``.
    * Identical strings → ``1.0``.
    * **Substring shortcut:** if one cleaned string is a contiguous substring of the
      other, the score is ``len(shorter) / len(longer)`` (high when a short merchant
      name appears inside a longer one). This is separate from the edit-distance path.
    * Otherwise the score is ``max(0, 1 - distance / max(len(left), len(right)))``
      using Levenshtein distance on the **cleaned** strings.

    If ``min_ratio`` is set, the edit-distance path may short-circuit when the ratio
    cannot reach ``min_ratio`` (same decisions as the full ratio for threshold checks).
    """
    left = clean_text(left)
    right = clean_text(right)
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if left in right or right in left:
        shorter = min(len(left), len(right))
        longer = max(len(left), len(right))
        return shorter / longer

    largest = max(len(left), len(right))
    if min_ratio is not None and min_ratio > 0.0:
        max_dist = int(math.floor((1.0 - min_ratio) * largest))
        if abs(len(left) - len(right)) > max_dist:
            return 0.0
        distance = _levenshtein_distance_capped(left, right, max_dist)
        if distance > max_dist:
            return 0.0
    else:
        distance = _levenshtein_distance(left, right)
    return max(0.0, 1.0 - (distance / largest))
