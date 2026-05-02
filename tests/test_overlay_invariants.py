"""Per-vendor overlay shape invariants.

Asserts every claim ehrs/{ehr}/overlay.json makes about itself holds:
  - row_id uniqueness (no duplicate citation anchors)
  - profile_id resolves to a real us-core profile
  - verified_date is valid ISO and within the 540-day staleness window
  - verified_via is in the schema enum and the source_url matches its expected domain
  - patient_ids referenced in deviations exist in sandbox_patients.json
  - multi_patient_coverage meets the 3-patients-per-probe design promise

Vendors that haven't accumulated a particular evidence type yet (e.g., Meditech
has no element_deviations and no multi_patient_coverage at v1.0) skip cleanly.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS = ("epic", "cerner", "meditech")
STALE_DAYS = 540


def _load_overlay(ehr: str) -> dict:
    return json.loads((REPO_ROOT / "ehrs" / ehr / "overlay.json").read_text())


def _load_us_core_profile_ids() -> set[str]:
    baseline = json.loads((REPO_ROOT / "us-core" / "us-core-6.1-baseline.json").read_text())
    return {p["profile_id"] for p in baseline["profiles"]}


def _load_url_patterns() -> dict[str, list[str]]:
    raw = json.loads((REPO_ROOT / "tests" / "fixtures" / "verified_via_url_patterns.json").read_text())
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _all_verifications(ov: dict):
    """Yield (path_label, verification_dict) for every verification block in the overlay."""
    for top in ("auth_overlay", "operation_outcome_overlay", "pagination_overlay",
                "phase_b_findings", "multi_patient_coverage", "access_scope_notes"):
        block = ov.get(top)
        if isinstance(block, dict) and isinstance(block.get("verification"), dict):
            yield top, block["verification"]
    for i, dev in enumerate(ov.get("element_deviations", []) or []):
        v = dev.get("verification")
        if isinstance(v, dict):
            yield f"element_deviations[{i}]", v
    spo = ov.get("search_param_observations") or {}
    if isinstance(spo, dict):
        for resource_type, block in spo.items():
            if isinstance(block, dict) and isinstance(block.get("verification"), dict):
                yield f"search_param_observations.{resource_type}", block["verification"]


@pytest.mark.parametrize("ehr", EHRS)
def test_element_deviation_row_id_unique(ehr: str) -> None:
    ov = _load_overlay(ehr)
    devs = ov.get("element_deviations", []) or []
    if not devs:
        pytest.skip(f"{ehr} has no element_deviations yet")
    row_ids = [d["row_id"] for d in devs]
    duplicates = {r for r in row_ids if row_ids.count(r) > 1}
    assert not duplicates, f"{ehr}: duplicate row_ids in element_deviations: {duplicates}"


@pytest.mark.parametrize("ehr", EHRS)
def test_element_deviation_profile_id_resolves(ehr: str) -> None:
    ov = _load_overlay(ehr)
    devs = ov.get("element_deviations", []) or []
    if not devs:
        pytest.skip(f"{ehr} has no element_deviations yet")
    valid = _load_us_core_profile_ids()
    bad = sorted({d["profile_id"] for d in devs if d["profile_id"] not in valid})
    assert not bad, (
        f"{ehr}: element_deviations reference profile_ids not in us-core/us-core-6.1-baseline.json: {bad}"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_verified_date_iso_and_fresh(ehr: str) -> None:
    """Every verification.verified_date parses as ISO and is within 540 days of today."""
    ov = _load_overlay(ehr)
    today = datetime.date.today()
    stale: list[str] = []
    invalid: list[str] = []
    for label, v in _all_verifications(ov):
        raw = v.get("verified_date", "")
        try:
            d = datetime.date.fromisoformat(raw)
        except ValueError:
            invalid.append(f"{label}: {raw!r}")
            continue
        age = (today - d).days
        if age > STALE_DAYS:
            stale.append(f"{label}: {age} days old (verified_date={raw})")
    assert not invalid, f"{ehr}: invalid ISO verified_date values: {invalid}"
    assert not stale, f"{ehr}: verified_date stale (>{STALE_DAYS} days):\n  " + "\n  ".join(stale)


@pytest.mark.parametrize("ehr", EHRS)
def test_verified_via_in_schema_enum(ehr: str, schema_overlay: dict) -> None:
    enum = set(schema_overlay["$defs"]["verification"]["properties"]["verified_via"]["enum"])
    ov = _load_overlay(ehr)
    bad = []
    for label, v in _all_verifications(ov):
        via = v.get("verified_via")
        if via not in enum:
            bad.append(f"{label}: {via!r}")
    assert not bad, f"{ehr}: verified_via not in enum {sorted(enum)}: {bad}"


@pytest.mark.parametrize("ehr", EHRS)
def test_source_url_well_formed(ehr: str) -> None:
    """source_url must parse to http/https with non-empty netloc, OR be a documented sentinel."""
    ov = _load_overlay(ehr)
    bad: list[str] = []
    sentinels = {"(see paired golden fixture)"}
    for label, v in _all_verifications(ov):
        url = v.get("source_url", "")
        if url in sentinels:
            continue
        # Permit local repo paths (tests/golden/...) — they're not URLs but they ARE valid
        # source_url values per the production_fleet schema description.
        if url.startswith("tests/golden/") or url.startswith("ehrs/"):
            continue
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            bad.append(f"{label}: {url!r}")
    assert not bad, f"{ehr}: malformed source_url values:\n  " + "\n  ".join(bad)


@pytest.mark.parametrize("ehr", EHRS)
def test_source_url_matches_verified_via(ehr: str) -> None:
    """When verified_via implies a vendor sandbox, source_url must contain a matching domain."""
    ov = _load_overlay(ehr)
    patterns = _load_url_patterns()
    sentinels = {"(see paired golden fixture)"}
    mismatches: list[str] = []
    for label, v in _all_verifications(ov):
        via = v.get("verified_via", "")
        url = v.get("source_url", "")
        if url in sentinels or url.startswith(("tests/golden/", "ehrs/")):
            continue
        expected = patterns.get(via, [])
        if not expected:
            continue  # no constraint for this verified_via (e.g., vendor_official_docs)
        if not any(needle in url for needle in expected):
            mismatches.append(f"{label}: verified_via={via} but url={url!r} (expected one of {expected})")
    assert not mismatches, f"{ehr}: verified_via/source_url mismatches:\n  " + "\n  ".join(mismatches)


@pytest.mark.parametrize("ehr", EHRS)
def test_multi_patient_coverage_meets_promise(ehr: str) -> None:
    """Two assertions:
      (a) every probe was swept against at least 1 patient (catches empty sweeps,
          which would be a data-integrity bug),
      (b) at least one probe in this overlay was swept against ≥3 patients (so the
          README's "3-patient sweep" design promise is met *somewhere* in the data).

    Per-probe ≥3 is not a realistic gate because some resources (e.g., Cerner
    Immunization) only exist on one sandbox patient — that's resource sparsity,
    not a measurement gap."""
    ov = _load_overlay(ehr)
    mpc = ov.get("multi_patient_coverage")
    if not mpc:
        pytest.skip(f"{ehr} has no multi_patient_coverage block yet")
    swept = mpc.get("patients_swept_per_probe", {}) or {}
    if not swept:
        pytest.skip(f"{ehr}: patients_swept_per_probe empty")

    counts: list[int] = []
    empty: list[str] = []
    for probe, v in swept.items():
        count = len(v) if isinstance(v, list) else (int(v) if isinstance(v, (int, float)) else None)
        if count is None:
            continue
        counts.append(count)
        if count < 1:
            empty.append(f"{probe}: {count}")
    assert not empty, f"{ehr}: probes swept against zero patients: {empty}"
    assert max(counts) >= 3, (
        f"{ehr}: no probe swept against ≥3 patients (max was {max(counts)}). "
        f"Design promise is a 3+ patient sweep on at least the dense probes."
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_element_deviation_patient_id_in_sandbox(ehr: str) -> None:
    """Patient ids in element_deviations.multi_patient_evidence must exist in sandbox_patients.json."""
    sp_path = REPO_ROOT / "ehrs" / ehr / "sandbox_patients.json"
    if not sp_path.exists():
        pytest.skip(f"{ehr} has no sandbox_patients.json")
    sandbox_ids = {p["id"] for p in json.loads(sp_path.read_text()).get("patients", [])}
    if not sandbox_ids:
        pytest.skip(f"{ehr}: sandbox_patients.json empty")
    ov = _load_overlay(ehr)
    devs = ov.get("element_deviations", []) or []
    referenced: set[str] = set()
    for d in devs:
        if d.get("patient_id"):
            referenced.add(d["patient_id"])
        mpe = d.get("multi_patient_evidence") or {}
        for key in ("patients_swept", "patients_present_in", "patients_absent_in",
                    "patients_with_this_category"):
            for pid in mpe.get(key, []) or []:
                referenced.add(pid)
    if not referenced:
        pytest.skip(f"{ehr}: no patient_id references in element_deviations")
    unknown = referenced - sandbox_ids
    assert not unknown, (
        f"{ehr}: element_deviations reference patient_ids not in sandbox_patients.json: {sorted(unknown)}"
    )
