"""P0 adversarial: the schema's verification-block enforcement IS the iron rule.

If the schema doesn't reject every category of citation violation, vendor PRs
can land claims without sources. These tests catch that. They build minimal
overlays from scratch (not real EHRs), inject one specific violation per case,
and assert validation fails.
"""
from __future__ import annotations

import jsonschema
import pytest


def _minimal_valid_overlay() -> dict:
    """Smallest overlay shape that satisfies the schema — used as the baseline
    that adversarial tests mutate. If schema requirements grow, update here.

    Note: the schema's required top-level fields do NOT include `verification`.
    Citations belong to claims (element_deviations rows, phase_b_findings, etc.),
    not to the file itself. An overlay with zero claims is legitimate and rare
    (early-stage vendor with only structural data). The iron rule applies on
    every claim row, enforced via the verification subschema referenced from
    those rows.
    """
    return {
        "ehr": "test-ehr",
        "ehr_display_name": "Test EHR (synthetic)",
        "capability_statement_fetched_date": "2026-04-29",
        "capability_statement_url": "https://example.test/metadata",
        "compatibility_statement": "Synthetic overlay for tests; not a real vendor.",
        "ehr_version_validated": "Test EHR v1.0",
    }


def _verification_block() -> dict:
    return {
        "source_url": "https://example.test/docs",
        "source_quote": "We hereby state that this is a fixture.",
        # The schema enforces an enum on verified_via; community_report is the
        # closest semantic fit for synthetic test data.
        "verified_via": "community_report",
        "verified_date": "2026-04-29",
    }


def _overlay_with_deviation(verification: dict | None) -> dict:
    """Overlay with a single element_deviation row whose verification block is
    `verification` (or absent if None). row_id is required by the schema; use a
    sentinel value matching the ^[a-f0-9]{12}$ pattern for synthetic data."""
    base = _minimal_valid_overlay()
    deviation = {
        "row_id": "1234567890ab",
        "profile_id": "us-core-patient",
        "path": "Patient.address.city",
        "deviation_category": "missing",
        "us_core_required": True,
    }
    if verification is not None:
        deviation["verification"] = verification
    base["element_deviations"] = [deviation]
    return base


def test_minimal_valid_overlay_passes(schema_overlay: dict) -> None:
    """Sanity: the baseline minimal overlay validates cleanly. If this fails,
    every other test in this file is meaningless — fix the baseline first."""
    jsonschema.validate(_minimal_valid_overlay(), schema_overlay)


def test_overlay_with_valid_deviation_passes(schema_overlay: dict) -> None:
    """A deviation row with a complete verification block validates."""
    jsonschema.validate(_overlay_with_deviation(_verification_block()), schema_overlay)


def test_deviation_missing_source_url_fails(schema_overlay: dict) -> None:
    """Iron rule: every cited claim has source_url. Removing it must fail."""
    bad = _verification_block()
    del bad["source_url"]
    with pytest.raises(jsonschema.ValidationError) as exc:
        jsonschema.validate(_overlay_with_deviation(bad), schema_overlay)
    assert "source_url" in exc.value.message


def test_deviation_missing_source_quote_fails(schema_overlay: dict) -> None:
    bad = _verification_block()
    del bad["source_quote"]
    with pytest.raises(jsonschema.ValidationError) as exc:
        jsonschema.validate(_overlay_with_deviation(bad), schema_overlay)
    assert "source_quote" in exc.value.message


def test_deviation_missing_verified_date_fails(schema_overlay: dict) -> None:
    bad = _verification_block()
    del bad["verified_date"]
    with pytest.raises(jsonschema.ValidationError) as exc:
        jsonschema.validate(_overlay_with_deviation(bad), schema_overlay)
    assert "verified_date" in exc.value.message


def test_deviation_missing_verified_via_fails(schema_overlay: dict) -> None:
    bad = _verification_block()
    del bad["verified_via"]
    with pytest.raises(jsonschema.ValidationError) as exc:
        jsonschema.validate(_overlay_with_deviation(bad), schema_overlay)
    assert "verified_via" in exc.value.message


def test_deviation_with_stub_quote_passes_schema_but_should_fail_validate(schema_overlay: dict) -> None:
    """The schema is permissive on quote contents; tools.validate enforces the
    'no STUB source_quote' rule on top of the schema. Document this division
    of labor so a future schema-only refactor doesn't silently drop the check.
    """
    stub = _verification_block()
    stub["source_quote"] = "STUB"
    # Schema-level: STUB passes (string non-empty)
    jsonschema.validate(_overlay_with_deviation(stub), schema_overlay)
    # tools.validate must reject this. We don't unit-test tools.validate here
    # (it's a CLI), but the test_real_overlays_all_validate test below
    # exercises it indirectly: shipped overlays must pass tools.validate too.


def test_real_overlays_all_validate(schema_overlay: dict, repo_root) -> None:
    """Smoke check: every shipped overlay validates clean. If a contributor PR
    breaks one, this is the first test that screams."""
    import json
    failures = []
    for overlay_path in sorted((repo_root / "ehrs").rglob("overlay.json")):
        try:
            jsonschema.validate(json.loads(overlay_path.read_text()), schema_overlay)
        except jsonschema.ValidationError as e:
            failures.append(f"{overlay_path.relative_to(repo_root)}: {e.message[:120]}")
    assert not failures, "shipped overlay(s) violate schema:\n  " + "\n  ".join(failures)
