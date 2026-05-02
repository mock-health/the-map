"""Regression test for the bundle-wide presence semantics of evaluate_one.

Pre-fix bug: evaluate_one only checked the FIRST resource of each type in the
bundle, so a path present on resources 2..N was wrongly reported as 'missing'.
That produced false 'absent across all swept patients' claims in element_deviations
when at least one Condition/MedicationRequest/etc in the bundle did have the path.

This test builds a synthetic Bundle where the first Condition lacks the
condition-assertedDate extension but a later Condition has it, and asserts the
path comes back 'matches', not 'missing'.
"""
from __future__ import annotations

import json

from tools.conformance import ValueSetIndex
from tools.measure_phase_b import (
    IG_PACKAGE_DIR,
    US_CORE_BASELINE,
    evaluate_one,
)


def test_presence_is_bundle_wide_not_first_resource_only():
    bundle = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "first-without",
                    "category": [{"coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": "problem-list-item",
                    }]}],
                    "subject": {"reference": "Patient/p1"},
                    "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]},
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "verificationStatus": {"coding": [{"code": "confirmed"}]},
                },
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "id": "second-with",
                    "category": [{"coding": [{
                        "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": "problem-list-item",
                    }]}],
                    "subject": {"reference": "Patient/p1"},
                    "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]},
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                    "verificationStatus": {"coding": [{"code": "confirmed"}]},
                    "extension": [{
                        "url": "http://hl7.org/fhir/StructureDefinition/condition-assertedDate",
                        "valueDateTime": "2026-01-12",
                    }],
                },
            },
        ],
    }
    baseline = json.loads(US_CORE_BASELINE.read_text())
    vs_index = ValueSetIndex(IG_PACKAGE_DIR)

    findings, diag = evaluate_one(
        body=bundle,
        profile_id="us-core-condition-problems-health-concerns",
        baseline=baseline,
        ehr="cerner",
        vs_index=vs_index,
        today="2026-05-02",
        patient_id="p1",
    )

    assert diag["evaluated_resource_count"] == 2

    asserted = [f for f in findings if f["path"] == "Condition.extension(condition-assertedDate)"]
    assert len(asserted) == 1, f"expected one finding for condition-assertedDate, got {len(asserted)}"
    assert asserted[0]["deviation_category"] == "matches", (
        f"expected 'matches' (path present on second Condition); got {asserted[0]['deviation_category']!r}. "
        "This indicates evaluate_one regressed to first-resource-only semantics."
    )
