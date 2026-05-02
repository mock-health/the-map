"""Stable, content-addressed identifiers for citable Map rows.

Every entry in `ehrs/{ehr}/overlay.json#element_deviations` carries a `row_id`
derived deterministically from `(profile_id, path, deviation_category)`. The
identifier is what external citations anchor to — a Brendan-Keeler-style essay
linking to a specific finding cites the row_id, not the array index.

Stability rules:
  - Same `(profile_id, path, deviation_category)` → same `row_id` forever.
  - Insertion order in element_deviations[] does not affect row_id.
  - Re-running the pipeline does not regenerate row_ids; existing rows keep theirs.
  - 12 hex chars (48 bits) — birthday collision risk negligible at expected scale.

If a row needs to change identity (e.g. deviation_category re-categorized after
re-analysis), the row gets a new row_id. The old row_id, if previously cited, is
preserved in `previous_row_ids[]` on the new row so external links can be 301'd.
"""
from __future__ import annotations

import hashlib


def compute_row_id(profile_id: str, path: str, deviation_category: str) -> str:
    """Return the 12-char hex row_id for a deviation row."""
    key = f"{profile_id}|{path}|{deviation_category}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


def ensure_row_id(deviation: dict) -> dict:
    """Mutate `deviation` in-place to carry a row_id; return it for chaining."""
    if "row_id" in deviation:
        return deviation
    pid = deviation.get("profile_id")
    path = deviation.get("path")
    cat = deviation.get("deviation_category")
    if not (pid and path and cat):
        raise ValueError(
            f"cannot compute row_id — deviation missing profile_id/path/deviation_category: {deviation!r}"
        )
    deviation["row_id"] = compute_row_id(pid, path, cat)
    return deviation
