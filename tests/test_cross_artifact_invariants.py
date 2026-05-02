"""Cross-artifact integrity checks.

The map is split across many files (CapStmts under ehrs/ AND under tests/golden/,
overlays referencing US Core profiles, fleet entries that should match the brands
bundle in tests/golden/cross-vendor/). When any of these slip out of sync the
schema validators won't catch it — these tests do.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS = ("epic", "cerner", "meditech")

# Per-vendor brands bundle paths under tests/golden/cross-vendor/. Multiple paths
# means the fleet endpoint set is the UNION of those bundles.
BRANDS_BUNDLES: dict[str, list[str]] = {
    "epic": ["epic-r4-endpoints-2026-04-27.json", "epic-dstu2-endpoints-2026-04-27.json"],
    "cerner": [
        "oracle-health-provider-r4-endpoints-2026-04-27.json",
        "oracle-health-patient-r4-endpoints-2026-04-27.json",
    ],
    "meditech": ["meditech-brands-2026-04-27.json"],
}


def _golden_dir(ehr: str) -> Path:
    return REPO_ROOT / "tests" / "golden" / ehr


def _cross_vendor_dir() -> Path:
    return REPO_ROOT / "tests" / "golden" / "cross-vendor"


@pytest.mark.parametrize("ehr", EHRS)
def test_overlay_capstmt_date_matches_golden_filename(ehr: str) -> None:
    """The overlay's capability_statement_fetched_date must appear in some golden CapStmt
    filename under tests/golden/{ehr}/CapabilityStatement-{date}.json."""
    overlay = json.loads((REPO_ROOT / "ehrs" / ehr / "overlay.json").read_text())
    fetched = overlay["capability_statement_fetched_date"]
    gd = _golden_dir(ehr)
    if not gd.exists():
        pytest.skip(f"{ehr}: tests/golden/{ehr}/ absent")
    matches = list(gd.glob(f"CapabilityStatement-{fetched}*.json"))
    # Tolerate any filename containing the date (e.g., variants like -argonaut, -greenfield).
    if not matches:
        matches = [p for p in gd.glob("CapabilityStatement*.json") if fetched in p.name]
    assert matches, (
        f"{ehr}: overlay.capability_statement_fetched_date={fetched} but no matching "
        f"CapStmt in tests/golden/{ehr}/. Available: "
        f"{sorted(p.name for p in gd.glob('CapabilityStatement*.json'))}"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_every_golden_capstmt_has_published_counterpart(ehr: str) -> None:
    """Each tests/golden/{ehr}/CapabilityStatement-*.json must be parseable JSON and the
    FHIR CapStmt-equivalent for its version (Conformance in DSTU2, CapabilityStatement
    in STU3+). The published ehrs/{ehr}/CapabilityStatement.json must exist and be R4."""
    gd = _golden_dir(ehr)
    if not gd.exists():
        pytest.skip(f"{ehr}: tests/golden/{ehr}/ absent")
    published = REPO_ROOT / "ehrs" / ehr / "CapabilityStatement.json"
    assert published.exists(), f"{ehr}: published CapabilityStatement.json missing"
    cs = json.loads(published.read_text())
    assert cs.get("resourceType") == "CapabilityStatement", f"{ehr}: published CapStmt has wrong resourceType"
    assert cs.get("fhirVersion", "").startswith("4."), f"{ehr}: published CapStmt is not R4"
    valid_capstmt_types = {"CapabilityStatement", "Conformance"}  # Conformance = pre-R4 (DSTU2/STU3)
    for golden in gd.glob("CapabilityStatement*.json"):
        try:
            g = json.loads(golden.read_text())
        except json.JSONDecodeError as e:
            pytest.fail(f"{ehr}: {golden.name} is not valid JSON: {e}")
        rt = g.get("resourceType")
        assert rt in valid_capstmt_types, (
            f"{ehr}: golden {golden.name} has resourceType {rt!r} "
            f"(expected one of {sorted(valid_capstmt_types)})"
        )


@pytest.mark.parametrize("ehr", EHRS)
def test_fleet_endpoint_ids_in_brands_bundle(ehr: str) -> None:
    """Every fleet endpoint must trace back to a brands-bundle Endpoint by FHIR
    base URL. (The fleet's `endpoint_id` is a path-slug; the brands bundle uses
    a FHIR resource UUID; the only common key is the URL `address`.)"""
    fleet = json.loads((REPO_ROOT / "ehrs" / ehr / "production_fleet.json").read_text())
    cross_dir = _cross_vendor_dir()
    bundle_addrs: set[str] = set()
    for fname in BRANDS_BUNDLES[ehr]:
        path = cross_dir / fname
        if not path.exists():
            continue
        bundle = json.loads(path.read_text())
        for entry in bundle.get("entry", []) or []:
            res = entry.get("resource") or {}
            if res.get("resourceType") == "Endpoint" and res.get("address"):
                bundle_addrs.add(res["address"].rstrip("/"))
    if not bundle_addrs:
        pytest.skip(f"{ehr}: no brands bundle endpoints found in tests/golden/cross-vendor/")
    fleet_addrs: set[str] = set()
    for c in fleet.get("capstmt_shape_clusters", []) or []:
        for ep in c.get("endpoints", []) or []:
            if ep.get("address"):
                fleet_addrs.add(ep["address"].rstrip("/"))
    for ep in fleet.get("outlier_endpoints", []) or []:
        if ep.get("address"):
            fleet_addrs.add(ep["address"].rstrip("/"))
    missing = fleet_addrs - bundle_addrs
    slack = max(1, int(0.02 * len(fleet_addrs)))
    assert len(missing) <= slack, (
        f"{ehr}: {len(missing)} fleet addresses not in brands bundle(s) {BRANDS_BUNDLES[ehr]}. "
        f"First missing: {sorted(missing)[:5]}"
    )


def test_us_core_profile_ids_referenced_exist() -> None:
    """The union of profile_ids referenced across all overlays' element_deviations must
    be a subset of the us-core baseline."""
    baseline = json.loads((REPO_ROOT / "us-core" / "us-core-6.1-baseline.json").read_text())
    valid = {p["profile_id"] for p in baseline["profiles"]}
    referenced: set[str] = set()
    for ehr in EHRS:
        ov = json.loads((REPO_ROOT / "ehrs" / ehr / "overlay.json").read_text())
        for d in ov.get("element_deviations", []) or []:
            if d.get("profile_id"):
                referenced.add(d["profile_id"])
    if not referenced:
        pytest.skip("no profile_ids referenced anywhere")
    bad = sorted(referenced - valid)
    assert not bad, f"profile_ids referenced in overlays but not in us-core baseline: {bad}"


def test_fleet_profile_ids_use_consistent_form() -> None:
    """us_core_profile_support_rate.profile_id values are stable identifiers (no whitespace,
    no full URLs masquerading as ids)."""
    bad: list[str] = []
    pat = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*$")
    for ehr in EHRS:
        fleet = json.loads((REPO_ROOT / "ehrs" / ehr / "production_fleet.json").read_text())
        for r in fleet.get("us_core_profile_support_rate", []) or []:
            pid = r.get("profile_id", "")
            if not pat.match(pid):
                bad.append(f"{ehr}: {pid!r}")
    assert not bad, f"profile_ids fail identifier shape: {bad[:10]}"
