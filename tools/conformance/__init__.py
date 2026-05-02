"""Conformance analyzer — turns one (resource, must_support_entry) pair into
zero-or-more deviation findings, one per axis that violates.

Per PLAN_EPIC.md decision #2, conformance is checked along four orthogonal axes:

    presence    — the path resolves to at least one value in the response
    cardinality — the count of values at the path matches min..max from the SD
    value_set   — coded values come from the bound value set (when strength=required)
    format      — primitive values match the FHIR primitive type pattern

Each axis lives in its own module. `analyze()` is the orchestrator that calls each
axis and returns the merged finding list. Today's `evaluate_path` from
`tools.measure_phase_b` is preserved as the presence implementation.

A finding is a dict in the shape used by `ehrs/{ehr}/overlay.json#element_deviations`,
so callers can splat the list into the overlay without further translation.
"""
from __future__ import annotations

import datetime

from .cardinality import cardinality_finding
from .format import format_finding
from .presence import evaluate_path, presence_finding
from .value_set import ValueSetIndex, value_set_finding


def _base_finding(profile_id: str, ms: dict, evidence: list, ehr: str, today: str) -> dict:
    return {
        "profile_id": profile_id,
        "path": ms["path"],
        "expected_per_us_core": _expected(ms),
        "verification": {
            "source_url": "(see paired golden fixture)",
            "source_quote": "",  # axis-specific text fills this in
            "verified_via": f"{ehr}_public_sandbox",
            "verified_date": today,
        },
    }


def _expected(ms: dict) -> str:
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


def analyze(
    *,
    resource: dict,
    must_support: dict,
    profile_id: str,
    ehr: str,
    value_set_index: ValueSetIndex | None,
    today: str | None = None,
) -> list[dict]:
    """Run every axis on one (resource, must_support_entry) pair.

    `must_support` is a single entry from the baseline's profiles[].must_support[].
    Returns a list of finding dicts ready to drop into overlay.element_deviations.
    """
    today = today or datetime.date.today().isoformat()
    findings: list[dict] = []

    # Axis 1: presence. We always run this first — most other axes are no-ops if absent.
    present, evidence = evaluate_path(resource, must_support["path"])
    p_finding = presence_finding(
        profile_id=profile_id, must_support=must_support, present=present,
        evidence=evidence, ehr=ehr, today=today,
    )
    findings.append(p_finding)

    if not present:
        # Skip the other axes — nothing to measure
        return findings

    # Axis 2: cardinality. Min/max enforcement.
    c_finding = cardinality_finding(
        profile_id=profile_id, must_support=must_support, resource=resource,
        ehr=ehr, today=today,
    )
    if c_finding:
        findings.append(c_finding)

    # Axis 3: value-set membership. Skip when no binding or analyzer hasn't loaded VS index.
    if value_set_index is not None:
        vs_finding = value_set_finding(
            profile_id=profile_id, must_support=must_support, resource=resource,
            ehr=ehr, today=today, vs_index=value_set_index,
        )
        if vs_finding:
            findings.append(vs_finding)

    # Axis 4: primitive type / format checks.
    f_finding = format_finding(
        profile_id=profile_id, must_support=must_support, resource=resource,
        ehr=ehr, today=today,
    )
    if f_finding:
        findings.append(f_finding)

    return findings
