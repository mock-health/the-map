"""Axis 1: presence checks.

Walks an SD path against a returned resource and decides whether the path resolves
to at least one value. Inherited from `tools.measure_phase_b.evaluate_path` with
slice-aware enhancements for non-extension slices (e.g. `Observation.category:us-core`).
"""
from __future__ import annotations


def evaluate_path(resource: dict, path: str) -> tuple[bool, list]:
    """Return (present, sample_values) for a US Core MUST-SUPPORT path against a returned resource.

    Recognized path forms:
      - Plain dotted:      `Patient.name.family`
      - Choice type:       `Patient.deceased[x]`  (matches any deceased* key)
      - Extension slice:   `Patient.extension(us-core-race)`  (matches by extension url substring)
      - Non-extension slice: `Observation.category:us-core` (resolves to all category[]; slice-specific
        membership is checked downstream by the value_set axis)
    """
    parts = path.split(".", 1)
    if len(parts) < 2:
        return False, []
    rest = parts[1]

    # Strip slice suffix; we treat sliced parents as their unsliced parent for presence
    if ":" in rest and not rest.startswith("extension("):
        rest = rest.split(":", 1)[0]

    # Extension(slug) syntax
    if rest.startswith("extension(") and rest.endswith(")"):
        slug = rest[len("extension(") : -1]
        for ext in resource.get("extension", []) or []:
            if slug in (ext.get("url") or ""):
                return True, [ext.get("url")]
        return False, []

    cur = [resource]
    for segment in rest.split("."):
        if segment.endswith("[x]"):
            stem = segment[:-3]
            next_cur = []
            for node in cur:
                if not isinstance(node, dict):
                    continue
                next_cur.extend(v for k, v in node.items() if k.startswith(stem))
            cur = next_cur
            continue
        next_cur = []
        for node in cur:
            if isinstance(node, list):
                for item in node:
                    if isinstance(item, dict) and segment in item:
                        v = item[segment]
                        next_cur.extend(v if isinstance(v, list) else [v])
            elif isinstance(node, dict) and segment in node:
                v = node[segment]
                next_cur.extend(v if isinstance(v, list) else [v])
        cur = next_cur
        if not cur:
            return False, []
    return bool(cur), [c if not isinstance(c, (dict, list)) else type(c).__name__ for c in cur][:3]


def count_at_path(resource: dict, path: str) -> int:
    """Count of values at the path. Used by cardinality axis."""
    present, _evidence = evaluate_path(resource, path)
    if not present:
        return 0
    # evaluate_path returns up to 3 sample values; we need real count.
    # For a real count, walk the path again and count without trimming.
    parts = path.split(".", 1)
    if len(parts) < 2:
        return 0
    rest = parts[1]
    if ":" in rest and not rest.startswith("extension("):
        rest = rest.split(":", 1)[0]

    if rest.startswith("extension(") and rest.endswith(")"):
        slug = rest[len("extension(") : -1]
        return sum(1 for ext in (resource.get("extension") or []) if slug in (ext.get("url") or ""))

    cur: list = [resource]
    for segment in rest.split("."):
        if segment.endswith("[x]"):
            stem = segment[:-3]
            next_cur = []
            for node in cur:
                if not isinstance(node, dict):
                    continue
                next_cur.extend(v for k, v in node.items() if k.startswith(stem))
            cur = next_cur
            continue
        next_cur = []
        for node in cur:
            if isinstance(node, list):
                for item in node:
                    if isinstance(item, dict) and segment in item:
                        v = item[segment]
                        next_cur.extend(v if isinstance(v, list) else [v])
            elif isinstance(node, dict) and segment in node:
                v = node[segment]
                next_cur.extend(v if isinstance(v, list) else [v])
        cur = next_cur
        if not cur:
            return 0
    return len(cur)


def collect_at_path(resource: dict, path: str) -> list:
    """Return every value (untrimmed) at the path. Used by value_set + format axes."""
    parts = path.split(".", 1)
    if len(parts) < 2:
        return []
    rest = parts[1]
    if ":" in rest and not rest.startswith("extension("):
        rest = rest.split(":", 1)[0]

    if rest.startswith("extension(") and rest.endswith(")"):
        slug = rest[len("extension(") : -1]
        return [ext for ext in (resource.get("extension") or []) if slug in (ext.get("url") or "")]

    cur: list = [resource]
    for segment in rest.split("."):
        if segment.endswith("[x]"):
            stem = segment[:-3]
            next_cur = []
            for node in cur:
                if not isinstance(node, dict):
                    continue
                next_cur.extend(v for k, v in node.items() if k.startswith(stem))
            cur = next_cur
            continue
        next_cur = []
        for node in cur:
            if isinstance(node, list):
                for item in node:
                    if isinstance(item, dict) and segment in item:
                        v = item[segment]
                        next_cur.extend(v if isinstance(v, list) else [v])
            elif isinstance(node, dict) and segment in node:
                v = node[segment]
                next_cur.extend(v if isinstance(v, list) else [v])
        cur = next_cur
        if not cur:
            return []
    return cur


def presence_finding(*, profile_id: str, must_support: dict, present: bool, evidence: list, ehr: str, today: str) -> dict:
    expected = _expected_text(must_support)
    if present:
        return {
            "profile_id": profile_id,
            "path": must_support["path"],
            "deviation_category": "matches",
            "expected_per_us_core": expected,
            "observed_in_ehr": f"present (sample: {evidence[:2]})",
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"Found at {must_support['path']}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }
    return {
        "profile_id": profile_id,
        "path": must_support["path"],
        "deviation_category": "missing",
        "expected_per_us_core": expected,
        "observed_in_ehr": "absent from this resource sample",
        "deviation": (
            f"Element {must_support['path']} not present in the captured response. "
            "Could mean: not implemented; not populated for this patient; or "
            "the path encoding here doesn't match how the EHR emits it. "
            "Vendor-implementation gap is recorded only when this is true across all swept patients."
        ),
        "verification": {
            "source_url": "(see paired golden fixture)",
            "source_quote": f"Path {must_support['path']} not found in resource body",
            "verified_via": f"{ehr}_public_sandbox",
            "verified_date": today,
        },
    }


def _expected_text(ms: dict) -> str:
    parts = []
    if ms.get("must_support"):
        parts.append("MUST-SUPPORT")
    if ms.get("uscdi_requirement"):
        parts.append("USCDI requirement")
    parts.append(f"cardinality {ms.get('cardinality', '?')}")
    if ms.get("type"):
        parts.append(f"type {' | '.join(ms['type'])}")
    if ms.get("binding", {}).get("strength"):
        parts.append(f"binding {ms['binding']['strength']} → {ms['binding'].get('valueSet_id', '?')}")
    return ", ".join(parts)
