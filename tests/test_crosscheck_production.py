"""Unit tests for tools.crosscheck_production — the analyzer/differ that turns a
production FHIR pull into overlay-shaped findings. The bucketing in
diff_against_sandbox and the n=1 honesty correction are what the overlay
correction's credibility rests on, so they get the most coverage.

These are pure-logic tests: no network, no real PHI. Synthetic resources and a
synthetic sandbox overlay only.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.crosscheck_production import (
    correct_n1_gaps,
    diff_against_sandbox,
    load_resources,
    write_phi_outputs,
    write_report,
)
from tools.row_id import compute_row_id


# --- helpers ---------------------------------------------------------------


def _dev(profile: str, path: str, category: str, **extra) -> dict:
    """A minimal element-deviation row with a correct row_id."""
    row = {
        "profile_id": profile,
        "path": path,
        "deviation_category": category,
        "row_id": compute_row_id(profile, path, category),
    }
    row.update(extra)
    return row


def _empty_buckets() -> dict:
    return {
        "confirmed_deviations": [],
        "confirmed_matches": [],
        "divergent": [],
        "novel_deviations": [],
        "absent_types": [],
        "untested": [],
        "benign_untracked_matches": 0,
    }


# --- load_resources: the 5 input shapes ------------------------------------


def test_load_resources_bare_resource(tmp_path: Path) -> None:
    (tmp_path / "p.json").write_text(json.dumps({"resourceType": "Patient", "id": "x"}))
    res = load_resources(tmp_path)
    assert [r["resourceType"] for r in res] == ["Patient"]


def test_load_resources_unwraps_bundle(tmp_path: Path) -> None:
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {"resource": {"resourceType": "Condition", "id": "c1"}},
            {"resource": {"resourceType": "Condition", "id": "c2"}},
            {"resource": {"foo": "no resourceType — skipped"}},
        ],
    }
    (tmp_path / "b.json").write_text(json.dumps(bundle))
    res = load_resources(tmp_path)
    assert [r["resourceType"] for r in res] == ["Condition", "Condition"]


def test_load_resources_top_level_array(tmp_path: Path) -> None:
    """The jmandel/ehi-to-fhir per-type fhir-target shape: a bare JSON array."""
    arr = [{"resourceType": "Observation", "id": "o1"}, {"resourceType": "Observation", "id": "o2"}]
    (tmp_path / "Observation.json").write_text(json.dumps(arr))
    res = load_resources(tmp_path)
    assert len(res) == 2 and all(r["resourceType"] == "Observation" for r in res)


def test_load_resources_unwraps_saved_response_envelope(tmp_path: Path) -> None:
    env = {"_captured_at": "2026-01-01", "body": {"resourceType": "Encounter", "id": "e1"}}
    (tmp_path / "env.json").write_text(json.dumps(env))
    res = load_resources(tmp_path)
    assert [r["resourceType"] for r in res] == ["Encounter"]


def test_load_resources_skips_bad_json(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    (tmp_path / "good.json").write_text(json.dumps({"resourceType": "Patient", "id": "x"}))
    (tmp_path / "bad.json").write_text("{ this is not json ")
    res = load_resources(tmp_path)
    assert [r["resourceType"] for r in res] == ["Patient"]
    assert "skipping bad.json" in capsys.readouterr().err


def test_load_resources_empty_dir_exits(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        load_resources(tmp_path)


# --- correct_n1_gaps: the iron rule on coverage ----------------------------


def test_correct_n1_gaps_relabels_vendor_gap() -> None:
    rows = [
        _dev("us-core-patient", "Patient.foo", "missing",
             multi_patient_evidence={"category": "vendor-implementation-gap"},
             deviation="absent everywhere"),
        _dev("us-core-patient", "Patient.bar", "value-set-narrowed",
             multi_patient_evidence={"category": "value-set-narrowed-everywhere"},
             deviation="narrowed"),
    ]
    correct_n1_gaps(rows)
    assert rows[0]["multi_patient_evidence"]["category"] == "patient-data-gap-n1"
    assert "[n=1 correction]" in rows[0]["deviation"]
    # A non-vendor-gap row is untouched.
    assert rows[1]["multi_patient_evidence"]["category"] == "value-set-narrowed-everywhere"
    assert "[n=1 correction]" not in rows[1]["deviation"]


# --- diff_against_sandbox: every bucket ------------------------------------


def test_diff_against_sandbox_buckets_each_finding() -> None:
    sandbox = {
        "element_deviations": [
            _dev("us-core-patient", "Patient.gender", "value-set-narrowed"),   # will be confirmed_deviation
            _dev("us-core-patient", "Patient.name", "matches"),                # will be confirmed_match
            _dev("us-core-encounter", "Encounter.status", "missing"),          # divergent target (prod differs)
            _dev("us-core-goal", "Goal.lifecycleStatus", "value-set-narrowed"),  # never exercised → untested
        ]
    }
    prod_rows = [
        _dev("us-core-patient", "Patient.gender", "value-set-narrowed"),       # row_id hit, not matches
        _dev("us-core-patient", "Patient.name", "matches"),                    # row_id hit, matches
        _dev("us-core-encounter", "Encounter.status", "value-set-narrowed"),   # same path, different cat → divergent
        _dev("us-core-condition", "Condition.code", "value-set-mismatch"),     # new path, deviation → novel
        _dev("us-core-condition", "Condition.onset", "matches"),               # new path, matches → benign count
        _dev("us-core-implantable-device", "Device", "patient-data-gap-n1"),   # no '.' → absent type
    ]
    b = diff_against_sandbox(prod_rows, sandbox)

    assert [r["path"] for r in b["confirmed_deviations"]] == ["Patient.gender"]
    assert [r["path"] for r in b["confirmed_matches"]] == ["Patient.name"]
    assert [r["path"] for r in b["divergent"]] == ["Encounter.status"]
    assert b["divergent"][0]["_sandbox_categories"] == ["missing"]
    assert [r["path"] for r in b["novel_deviations"]] == ["Condition.code"]
    assert [r["path"] for r in b["absent_types"]] == ["Device"]
    assert b["benign_untracked_matches"] == 1
    # untested = sandbox rows whose exact row_id was not reproduced. The diverged
    # Encounter.status(missing) row qualifies (prod produced a different category,
    # hence a different row_id), alongside the never-exercised Goal row.
    assert [r["path"] for r in b["untested"]] == ["Encounter.status", "Goal.lifecycleStatus"]


# --- write_phi_outputs: the wire/ehi gate + tier placeholder ---------------


def test_write_phi_outputs_wire_emits_candidates_with_community_placeholder(tmp_path: Path) -> None:
    buckets = _empty_buckets()
    buckets["novel_deviations"] = [_dev("us-core-patient", "Patient.foo", "value-set-narrowed")]
    write_phi_outputs(tmp_path, buckets["novel_deviations"], buckets, "wire",
                      "https://x/FHIR", "community_report", "https://repo/commit")
    cand = json.loads((tmp_path / "candidate_rows.json").read_text())
    assert len(cand) == 1
    assert cand[0]["verification"]["verified_via"] == "community_report"
    assert cand[0]["verification"]["source_quote"].startswith("(fill structural excerpt")
    assert cand[0]["_crosscheck_intent"] == "new-row"


def test_write_phi_outputs_wire_smart_uses_todo_placeholder(tmp_path: Path) -> None:
    buckets = _empty_buckets()
    buckets["novel_deviations"] = [_dev("us-core-patient", "Patient.foo", "value-set-narrowed")]
    write_phi_outputs(tmp_path, buckets["novel_deviations"], buckets, "wire",
                      "https://x/FHIR", "production_patient_smart", None)
    cand = json.loads((tmp_path / "candidate_rows.json").read_text())
    assert cand[0]["verification"]["source_quote"].startswith("TODO-DEIDENTIFY")


def test_write_phi_outputs_ehi_emits_no_candidates(tmp_path: Path) -> None:
    buckets = _empty_buckets()
    buckets["novel_deviations"] = [_dev("us-core-patient", "Patient.foo", "value-set-narrowed")]
    write_phi_outputs(tmp_path, buckets["novel_deviations"], buckets, "ehi",
                      "https://x/FHIR", "production_patient_smart", None)
    assert (tmp_path / "raw_findings.json").exists()
    assert not (tmp_path / "candidate_rows.json").exists()


# --- write_report: tier wording is honest ----------------------------------


@pytest.mark.parametrize(
    "source,verified_via,expected",
    [
        ("wire", "community_report", "de-identified + published by a third party"),
        ("wire", "production_patient_smart", "genuine production wire bytes"),
        ("ehi", "production_patient_smart", "corroboration only"),
    ],
)
def test_write_report_tier_wording(tmp_path: Path, source: str, verified_via: str, expected: str) -> None:
    out = tmp_path / "report.md"
    write_report(out, source=source, endpoint="https://x/FHIR", n_resources=1,
                 resource_types={"Patient": 1}, buckets=_empty_buckets(),
                 patient_label="t", verified_via=verified_via, cite=None)
    assert expected in out.read_text()
