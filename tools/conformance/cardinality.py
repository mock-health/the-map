"""Axis 2: cardinality checks.

Walks a path in the response, counts matched values, and compares to the SD's
`min..max`. Emits a finding only when the observed count violates the bound; if
cardinality is satisfied, the presence finding (`matches`) already records that.

Notes:
  - Required absence (`min=1, max=1`) overlaps with the presence axis. We only
    emit a cardinality-min-violated row when the path IS present but with fewer
    children than required (impossible for primitives, possible for repeating
    elements like `Patient.identifier`).
  - `max=*` means unbounded; never violates max.
"""
from __future__ import annotations

from .presence import count_at_path


def cardinality_finding(*, profile_id: str, must_support: dict, resource: dict, ehr: str, today: str) -> dict | None:
    path = must_support["path"]
    min_v = must_support.get("min", 0)
    max_v = must_support.get("max", "*")

    try:
        min_int = int(min_v)
    except (TypeError, ValueError):
        min_int = 0
    max_int: int | None
    if max_v == "*":
        max_int = None
    else:
        try:
            max_int = int(max_v)
        except (TypeError, ValueError):
            max_int = None

    count = count_at_path(resource, path)

    if count == 0:
        # Presence axis already covered the missing case; nothing to add here unless min>0
        if min_int > 0:
            # Note: presence said "missing" already; we don't double-report.
            return None
        return None

    if count < min_int:
        return {
            "profile_id": profile_id,
            "path": path,
            "deviation_category": "cardinality-min-violated",
            "expected_per_us_core": f"cardinality {must_support.get('cardinality', '?')} (min={min_int})",
            "observed_in_ehr": f"count={count} children at {path}",
            "deviation": (
                f"Element {path} is present but its child count ({count}) is below the US Core minimum ({min_int})."
            ),
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"Counted {count} value(s) at {path}; expected ≥{min_int}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }

    # Cardinality-max enforcement is per-parent in FHIR; our global count_at_path
    # over-counts when an ancestor in the path is repeating. We only run max
    # enforcement when the path is a top-level singleton (e.g. `Patient.gender`)
    # so we don't emit a stream of false positives for `Patient.identifier.system`.
    is_top_level = path.count(".") == 1 and ":" not in path and "(" not in path
    if is_top_level and max_int is not None and count > max_int:
        return {
            "profile_id": profile_id,
            "path": path,
            "deviation_category": "cardinality-max-violated",
            "expected_per_us_core": f"cardinality {must_support.get('cardinality', '?')} (max={max_int})",
            "observed_in_ehr": f"count={count} at top-level {path}",
            "deviation": (
                f"Element {path} returned {count} values; US Core caps this element at {max_int}. "
                "Likely an EHR conformance bug or a stale profile constraint."
            ),
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"Counted {count} value(s) at {path}; expected ≤{max_int}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }
    return None
