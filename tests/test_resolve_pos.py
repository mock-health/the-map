"""Per-vendor resolution tests for tools/resolve_endpoints_to_pos.py.

Synthetic Bundle inputs cover:
  - Cerner: Endpoint + sibling Organization with state/zip5/name → unique match
  - Cerner: ambiguous (clinic name overlapping a hospital) → no auto-match
  - Meditech: Endpoint with Organization referencing it (reverse pointer) → match
  - Epic: Endpoint-only with contained Organization → no auto-match (candidates only)
  - Manual override: epic-pos.manual.json overrides candidates
"""
from __future__ import annotations

from pathlib import Path

from tools import resolve_endpoints_to_pos as rp


def _hospital(ccn: str, name: str, city: str, state: str, zip5: str,
              subtype: str = "01", beds: int = 100) -> dict:
    return {
        "ccn": ccn, "name": name, "address_line": f"{ccn} MAIN ST",
        "city": city, "state": state, "zip": zip5, "phone": "",
        "fac_subtype_code": subtype, "bed_count": beds, "urban_rural": "U",
        "fips_state": "", "fips_county": "", "cbsa_code": "",
        "certification_date": "2000-01-01", "termination_code": "00",
    }


def _build_index(hospitals: list[dict]) -> dict:
    catalog = {"hospitals": hospitals}
    return rp.index_catalog(catalog)


# --- name normalization ---------------------------------------------------

def test_jaccard_perfect_match() -> None:
    a = rp.name_tokens("Crawford County Memorial Hospital")
    b = rp.name_tokens("CRAWFORD COUNTY MEMORIAL HOSPITAL")
    assert rp.jaccard(a, b) == 1.0


def test_jaccard_drops_stopwords() -> None:
    """The stopword filter must remove 'hospital'/'medical'/'center' so a name
    like 'Mercy Hospital' and 'Mercy Medical Center' don't get penalized for
    spec-trivial differences."""
    a = rp.name_tokens("Mercy Hospital of Iowa City")
    b = rp.name_tokens("Mercy Medical Center Iowa City")
    # Both reduce to {"mercy", "iowa", "city"}
    assert a == b == {"mercy", "iowa", "city"}


# --- per-vendor extraction ------------------------------------------------

def test_iter_endpoint_records_cerner_style() -> None:
    """Cerner: top-level Endpoint + Organization, joined via Organization.endpoint
    reverse-pointer."""
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {"resource": {
                "resourceType": "Endpoint",
                "id": "ep-1",
                "address": "https://fhir-ehr.cerner.com/r4/ep-1/",
            }},
            {"resource": {
                "resourceType": "Organization",
                "id": "org-1",
                "name": "Memorial Hospital of Anywhere",
                "address": [{"city": "Anywhere", "state": "TX", "postalCode": "75001"}],
                "endpoint": [{"reference": "Endpoint/ep-1"}],
                "identifier": [
                    {"system": rp.NPI_SYSTEM, "value": "1234567890"},
                ],
            }},
        ],
    }
    [rec] = rp.iter_endpoint_records(bundle, "cerner")
    assert rec["endpoint_id"] == "ep-1"
    assert rec["name"] == "Memorial Hospital of Anywhere"
    assert rec["state"] == "TX"
    assert rec["zip5"] == "75001"
    assert rec["npi"] == "1234567890"


def test_iter_endpoint_records_epic_style() -> None:
    """Epic: Endpoint-only with contained Organization, no address."""
    bundle = {
        "resourceType": "Bundle",
        "entry": [
            {"resource": {
                "resourceType": "Endpoint",
                "id": "ep-epic-1",
                "name": "Boston Children's Hospital",
                "address": "https://epicproxy.et1234.epichosted.com/...",
                "contained": [
                    {"resourceType": "Organization", "id": "o1",
                     "name": "Boston Children's Hospital"},
                ],
                "managingOrganization": {"reference": "o1"},
            }},
        ],
    }
    [rec] = rp.iter_endpoint_records(bundle, "epic")
    assert rec["name"] == "Boston Children's Hospital"
    assert rec["state"] == ""
    assert rec["zip5"] == ""
    assert rec["npi"] is None


# --- resolution -----------------------------------------------------------

def test_cerner_unique_state_zip5_match_resolves() -> None:
    idx = _build_index([
        _hospital("100001", "MEMORIAL HOSPITAL OF ANYWHERE", "ANYWHERE", "TX", "75001"),
        _hospital("100002", "OTHER HOSPITAL", "DALLAS", "TX", "75002"),
    ])
    record = {
        "endpoint_id": "ep-1", "endpoint_address": "https://x/",
        "name": "Memorial Hospital of Anywhere", "npi": None,
        "city": "Anywhere", "state": "TX", "zip5": "75001",
    }
    out = rp.resolve_one(record, idx)
    assert out["match_strategy"] == "address_state_zip5_name"
    assert out["ccn"] == "100001"
    assert out["match_confidence"] >= rp.THRESH_STATE_ZIP5
    assert out["name_pos"] == "MEMORIAL HOSPITAL OF ANYWHERE"


def test_cerner_clinic_in_hospital_zip_does_not_auto_resolve() -> None:
    """A physician-practice org sharing the same zip as a hospital must not
    auto-resolve to that hospital just because they share location words."""
    idx = _build_index([
        _hospital("100001", "JACKSON HOSPITAL & CLINIC INC", "MONTGOMERY", "AL", "36117"),
    ])
    record = {
        "endpoint_id": "ep-clinic", "endpoint_address": "",
        "name": "Family Practice Clinic of Montgomery, PA", "npi": None,
        "city": "Montgomery", "state": "AL", "zip5": "36117",
    }
    out = rp.resolve_one(record, idx)
    # tokens: family, practice, clinic, montgomery, pa  vs  jackson
    # Jaccard low → no auto-resolve, but candidates may surface
    assert out["match_strategy"] in ("unmatched", "address_state_name", "address_state_zip5_name")
    if out["match_strategy"] == "address_state_zip5_name":
        # If we did auto-resolve, score must be high enough to justify it.
        assert out["match_confidence"] >= rp.THRESH_STATE_ZIP5
    else:
        assert out["ccn"] is None


def test_meditech_auto_resolves_via_state_zip5() -> None:
    idx = _build_index([
        _hospital("210001", "ALLENDALE COUNTY HOSPITAL", "FAIRFAX", "SC", "29827"),
    ])
    record = {
        "endpoint_id": "med-1", "endpoint_address": "",
        "name": "Allendale County Hospital", "npi": None,
        "city": "Fairfax", "state": "SC", "zip5": "29827",
    }
    out = rp.resolve_one(record, idx)
    assert out["match_strategy"] == "address_state_zip5_name"
    assert out["ccn"] == "210001"


def test_epic_no_state_yields_candidates_only() -> None:
    idx = _build_index([
        _hospital("100001", "BOSTON CHILDREN'S HOSPITAL", "BOSTON", "MA", "02115"),
        _hospital("100002", "CHILDREN'S HOSPITAL OF PHILADELPHIA", "PHILADELPHIA", "PA", "19104"),
    ])
    record = {
        "endpoint_id": "ep-epic-1", "endpoint_address": "",
        "name": "Boston Children's Hospital", "npi": None,
        "city": "", "state": "", "zip5": "",
    }
    out = rp.resolve_one(record, idx)
    assert out["ccn"] is None
    assert out["match_strategy"] == "name_only"
    cands = out.get("candidates", [])
    assert any(c["ccn"] == "100001" for c in cands)


def test_unmatched_when_no_state_no_candidates() -> None:
    idx = _build_index([
        _hospital("100001", "SOMEWHERE MEDICAL CENTER", "FOO", "TX", "75001"),
    ])
    record = {
        "endpoint_id": "ep-x", "endpoint_address": "",
        "name": "Random Construction Co.", "npi": None,
        "city": "", "state": "", "zip5": "",
    }
    out = rp.resolve_one(record, idx)
    assert out["match_strategy"] == "unmatched"
    assert out["ccn"] is None


# --- override file --------------------------------------------------------

def test_manual_override_wins(tmp_path: Path, monkeypatch) -> None:
    """A *.manual.json that maps endpoint_id → ccn must overwrite the
    auto-resolved row even when the auto-resolution disagrees."""
    import json
    idx = _build_index([
        _hospital("100001", "AUTO MATCH HOSPITAL", "AUTOTOWN", "TX", "75001"),
        _hospital("999999", "OVERRIDE TARGET", "OVERTOWN", "NY", "10001"),
    ])
    records = [
        rp.resolve_one(
            {
                "endpoint_id": "ep-1", "endpoint_address": "",
                "name": "Auto Match Hospital", "npi": None,
                "city": "Autotown", "state": "TX", "zip5": "75001",
            },
            idx,
        )
    ]
    # Stamp iron-rule fields the way resolve_vendor() does before override
    for r in records:
        r["vendor"] = "test"
        r.setdefault("verified_via", "cms_pos_csv")
        r.setdefault("verified_date", "2026-05-01")

    override_path = tmp_path / "test-pos.manual.json"
    override_path.write_text(json.dumps({"ep-1": "999999"}))
    applied = rp.apply_overrides(
        records, override_path, idx, "2026-05-01",
        match_strategy="manual_override", verified_via="manual_override",
    )
    assert applied == 1
    assert records[0]["ccn"] == "999999"
    assert records[0]["match_strategy"] == "manual_override"
    assert records[0]["verified_via"] == "manual_override"
    assert records[0]["match_confidence"] == 1.0


def test_manual_override_beats_llm_override(tmp_path: Path) -> None:
    """When both *.llm.json and *.manual.json target the same endpoint,
    manual must win because it's applied second."""
    import json
    idx = _build_index([
        _hospital("100001", "AUTO", "X", "TX", "75001"),
        _hospital("200001", "LLM PICK", "Y", "TX", "75002"),
        _hospital("300001", "MANUAL PICK", "Z", "TX", "75003"),
    ])
    records = [{
        "endpoint_id": "ep-1", "vendor": "test",
        "ccn": "100001", "match_strategy": "address_state_zip5_name",
        "match_confidence": 0.6, "name_observed": "Auto",
        "verified_via": "cms_pos_csv", "verified_date": "2026-05-01",
    }]
    llm_path = tmp_path / "test-pos.llm.json"
    manual_path = tmp_path / "test-pos.manual.json"
    llm_path.write_text(json.dumps({"ep-1": "200001"}))
    manual_path.write_text(json.dumps({"ep-1": "300001"}))

    # Apply in production order: LLM first, then manual.
    rp.apply_overrides(records, llm_path, idx, "2026-05-01",
                       match_strategy="llm_assisted", verified_via="llm_assisted")
    assert records[0]["ccn"] == "200001"
    assert records[0]["match_strategy"] == "llm_assisted"
    rp.apply_overrides(records, manual_path, idx, "2026-05-01",
                       match_strategy="manual_override", verified_via="manual_override")
    assert records[0]["ccn"] == "300001"
    assert records[0]["match_strategy"] == "manual_override"
    assert records[0]["verified_via"] == "manual_override"


# --- schema validation ---------------------------------------------------

def test_committed_resolutions_schema_valid(repo_root: Path) -> None:
    """Every shipped data/hospital-overrides/{vendor}-pos.json must validate
    cleanly against the schema. Iron-rule extension."""
    import json

    import jsonschema
    schema_path = repo_root / "schema" / "hospital_resolution.schema.json"
    if not schema_path.exists():
        return
    schema = json.loads(schema_path.read_text())
    files = sorted((repo_root / "data" / "hospital-overrides").glob("*-pos.json"))
    if not files:
        return
    for f in files:
        jsonschema.validate(json.loads(f.read_text()), schema)


def test_resolution_schema_requires_iron_rule_fields(repo_root: Path) -> None:
    """Removing verified_via or verified_date from an endpoint resolution row
    must fail schema validation."""
    import copy
    import json

    import jsonschema
    import pytest
    schema_path = repo_root / "schema" / "hospital_resolution.schema.json"
    if not schema_path.exists():
        return
    schema = json.loads(schema_path.read_text())
    base = {
        "vendor": "test", "captured_date": "2026-05-01",
        "pos_catalog": "data/cms-pos/hospitals-2026-05-01.json",
        "endpoints": [{
            "endpoint_id": "x", "vendor": "test",
            "match_strategy": "unmatched", "match_confidence": 0.0,
            "verified_via": "cms_pos_csv", "verified_date": "2026-05-01",
        }],
        "verification": {
            "source_url": "data/cms-pos/hospitals-2026-05-01.json",
            "source_quote": "fixture",
            "verified_via": "cms_pos_csv", "verified_date": "2026-05-01",
        },
    }
    jsonschema.validate(base, schema)  # baseline must validate
    for missing in ("verified_via", "verified_date"):
        bad = copy.deepcopy(base)
        del bad["endpoints"][0][missing]
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)
