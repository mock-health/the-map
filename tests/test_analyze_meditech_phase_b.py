"""Smoke + structural tests for tools/analyze_meditech_phase_b.py.

The analyzer reads the captured Greenfield Phase B fixtures and emits
structured `element_deviation` rows. These tests cover the rule set
(missing-coding, nonstandard-system, duplicate-coding, missing-meta)
on synthetic inputs so adding a new rule keeps the existing rules locked
down, and lock the row_id stability so the merge into MEDITECH overlay
is idempotent.

Run:
    cd the-map && python3 -m pytest tests/test_analyze_meditech_phase_b.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

THE_MAP = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(THE_MAP))

from tools.analyze_meditech_phase_b import (  # noqa: E402
    _check_missing_coding,
    _check_missing_meta,
    _check_nonstandard_system,
    _check_duplicate_coding,
    _row_id,
    analyze,
)


# ---------------------------------------------------------------------------
# Per-check unit tests
# ---------------------------------------------------------------------------

def test_missing_coding_fires_on_empty_coding_array():
    resource = {
        "resourceType": "Condition",
        "code": {"coding": [], "text": "Hypertension"},
    }
    result = _check_missing_coding(resource, "Condition.code")
    assert result is not None
    cat, quote = result
    assert cat == "missing-coding"
    assert "Hypertension" in quote


def test_missing_coding_fires_on_null_coding():
    resource = {
        "resourceType": "Condition",
        "code": {"text": "Hypertension"},
    }
    result = _check_missing_coding(resource, "Condition.code")
    assert result is not None
    assert result[0] == "missing-coding"


def test_missing_coding_passes_when_coding_present():
    resource = {
        "resourceType": "Condition",
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "59621000"}],
        },
    }
    assert _check_missing_coding(resource, "Condition.code") is None


def test_nonstandard_system_fires_on_ndfrt_allergy():
    """MEDITECH-observed: AllergyIntolerance.code uses NDF-RT, not RxNorm."""
    resource = {
        "resourceType": "AllergyIntolerance",
        "code": {
            "coding": [{
                "system": "http://hl7.org/fhir/ndfrt",
                "code": "N0000011281",
                "display": "Penicillins",
            }],
        },
    }
    result = _check_nonstandard_system(resource, "AllergyIntolerance.code")
    assert result is not None
    cat, quote = result
    assert cat == "nonstandard-system"
    assert "ndfrt" in quote


def test_nonstandard_system_passes_on_rxnorm():
    resource = {
        "resourceType": "AllergyIntolerance",
        "code": {
            "coding": [{
                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                "code": "7980",
            }],
        },
    }
    assert _check_nonstandard_system(resource, "AllergyIntolerance.code") is None


def test_duplicate_coding_fires_when_same_system_code_appears_twice():
    """MEDITECH-observed: vital-signs Observation.code has duplicate LOINC entries."""
    resource = {
        "resourceType": "Observation",
        "code": {
            "coding": [
                {"system": "http://loinc.org", "code": "2708-6"},
                {"system": "http://loinc.org", "code": "2708-6"},
                {"system": "http://loinc.org", "code": "59408-5"},
            ],
        },
    }
    result = _check_duplicate_coding(resource, "Observation.code")
    assert result is not None
    cat, quote = result
    assert cat == "duplicate-coding"
    assert "2708-6" in quote


def test_duplicate_coding_passes_when_all_codings_unique():
    resource = {
        "resourceType": "Observation",
        "code": {
            "coding": [
                {"system": "http://loinc.org", "code": "2708-6"},
                {"system": "http://loinc.org", "code": "59408-5"},
            ],
        },
    }
    assert _check_duplicate_coding(resource, "Observation.code") is None


def test_missing_meta_fires_when_meta_absent():
    resource = {"resourceType": "Patient"}
    result = _check_missing_meta(resource)
    assert result is not None
    assert result[0] == "missing-meta"


def test_missing_meta_fires_when_meta_null():
    resource = {"resourceType": "Patient", "meta": None}
    result = _check_missing_meta(resource)
    assert result is not None


def test_missing_meta_passes_when_meta_populated():
    resource = {
        "resourceType": "Patient",
        "meta": {"lastUpdated": "2026-04-28T12:00:00Z", "versionId": "1"},
    }
    assert _check_missing_meta(resource) is None


# ---------------------------------------------------------------------------
# row_id stability — the merge into overlay must be idempotent
# ---------------------------------------------------------------------------

def test_row_id_is_stable_across_runs():
    a = _row_id("us-core-condition", "Condition.code", "missing-coding")
    b = _row_id("us-core-condition", "Condition.code", "missing-coding")
    assert a == b
    assert len(a) == 12


def test_row_id_changes_when_any_input_changes():
    base = _row_id("us-core-condition", "Condition.code", "missing-coding")
    assert base != _row_id("us-core-condition", "Condition.code", "nonstandard-system")
    assert base != _row_id("us-core-condition", "Condition.text", "missing-coding")
    assert base != _row_id("us-core-patient", "Condition.code", "missing-coding")


# ---------------------------------------------------------------------------
# End-to-end — analyzer over the real Phase B corpus
# ---------------------------------------------------------------------------

def test_analyze_returns_at_least_ten_rows_from_real_fixtures():
    """KATA-#133 acceptance: real measurements from the captured corpus.
    The fixture set under tests/golden/meditech/phase-b-2026-04-28/ has
    AllergyIntolerance, Condition, Observation, Patient, Encounter,
    DiagnosticReport, etc. The analyzer should find multiple deviations
    across them."""
    rows = analyze()
    assert len(rows) >= 10, f"expected ≥10 element_deviation rows; got {len(rows)}"
    # Confirm at least one row per high-signal deviation category.
    cats = {r["deviation_category"] for r in rows}
    assert "missing-coding" in cats, (
        f"Condition.code missing-coding should fire; got categories={cats}"
    )
    assert "nonstandard-system" in cats, (
        f"AllergyIntolerance.code NDF-RT nonstandard-system should fire; "
        f"got categories={cats}"
    )


def test_overlay_carries_real_phase_b_measurements():
    """The MEDITECH overlay must have element_deviations populated from
    the Phase B measurement work — empty array would mean the kata's
    measurement work hasn't landed."""
    overlay = json.loads((THE_MAP / "ehrs" / "meditech" / "overlay.json").read_text())
    ed = overlay.get("element_deviations") or []
    assert len(ed) >= 10, (
        f"MEDITECH overlay should carry ≥10 element_deviation rows from "
        f"Phase B measurement; got {len(ed)}"
    )
    # Confirm rows carry the measurement provenance.
    sources = {r.get("verification", {}).get("verified_via") for r in ed}
    assert "meditech_greenfield_phase_b" in sources
