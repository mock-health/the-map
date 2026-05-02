"""Per-vendor hospital-resolution shape invariants.

Asserts every endpoint in the production fleet has a resolution row, the
match-strategy distribution is sane (no vendor 100% unmatched), and any
manual_override rows carry a justification. Skips cleanly if the
data/hospital-overrides/ artifacts haven't been committed yet.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS = ("epic", "cerner", "meditech")

# Empirical floors from the 2026-05-01 resolution. Test catches regressions from
# this baseline; not a quality target. Update when resolver improves materially.
MATCHED_UNIQUE_FLOORS = {"epic": 0.30, "cerner": 0.20, "meditech": 0.50}


def _resolution_path(ehr: str) -> Path:
    return REPO_ROOT / "data" / "hospital-overrides" / f"{ehr}-pos.json"


def _fleet_path(ehr: str) -> Path:
    return REPO_ROOT / "ehrs" / ehr / "production_fleet.json"


def _load_resolution(ehr: str):
    p = _resolution_path(ehr)
    if not p.exists():
        return None
    return json.loads(p.read_text())


def _fleet_addresses(ehr: str, *, provider_scope_only: bool = False) -> set[str]:
    """Cross-artifact join key: FHIR base URL (`address`).

    The fleet's per-cluster `endpoint_id` is a path-encoded slug derived from
    the FHIR base URL; brands bundles + hospital-overrides use the FHIR
    Endpoint resource's `id` (a UUID). The only field present in all three
    artifacts is the FHIR base URL itself.

    `provider_scope_only=True` filters out clusters with `access_scope=patient`.
    Used when comparing against the hospital-resolution file, which only resolves
    provider-tier hosts (patient-tier hosts back to the same physical hospitals)."""
    fleet = json.loads(_fleet_path(ehr).read_text())
    addrs: set[str] = set()
    for c in fleet.get("capstmt_shape_clusters", []) or []:
        if provider_scope_only and c.get("access_scope") == "patient":
            continue
        for ep in c.get("endpoints", []) or []:
            if ep.get("address"):
                addrs.add(ep["address"].rstrip("/"))
    if not provider_scope_only:
        for ep in fleet.get("outlier_endpoints", []) or []:
            if ep.get("address"):
                addrs.add(ep["address"].rstrip("/"))
    else:
        # Outliers carry cluster_id; filter via cluster lookup.
        cluster_scope = {c["cluster_id"]: c.get("access_scope") for c in fleet.get("capstmt_shape_clusters", []) or []}
        for ep in fleet.get("outlier_endpoints", []) or []:
            if cluster_scope.get(ep.get("cluster_id")) == "patient":
                continue
            if ep.get("address"):
                addrs.add(ep["address"].rstrip("/"))
    return addrs


@pytest.mark.parametrize("ehr", EHRS)
def test_every_fleet_endpoint_has_resolution_row(ehr: str) -> None:
    """Every provider-tier fleet endpoint must have a hospital-resolution row.

    Patient-tier endpoints (Cerner's fhir-myrecord.cerner.com hosts) are
    intentionally not separately resolved — they back to the same physical
    hospitals as the provider tier."""
    res = _load_resolution(ehr)
    if res is None:
        pytest.skip(f"{ehr}: data/hospital-overrides/{ehr}-pos.json absent")
    fleet_addrs = _fleet_addresses(ehr, provider_scope_only=True)
    if not fleet_addrs:
        pytest.skip(f"{ehr}: no provider-tier fleet addresses to compare")
    res_addrs = {
        ep["endpoint_address"].rstrip("/")
        for ep in res.get("endpoints", []) or []
        if ep.get("endpoint_address")
    }
    missing = fleet_addrs - res_addrs
    # Allow up to 2% slack for endpoints that fell out of the brands bundle between
    # harvest and resolution runs.
    slack = max(1, int(0.02 * len(fleet_addrs)))
    assert len(missing) <= slack, (
        f"{ehr}: {len(missing)} provider-tier fleet addresses have no resolution row "
        f"(slack {slack}). First missing: {sorted(missing)[:5]}"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_match_strategy_distribution_sane(ehr: str) -> None:
    res = _load_resolution(ehr)
    if res is None:
        pytest.skip(f"{ehr}: hospital-overrides absent")
    summary = res.get("match_summary") or {}
    total = summary.get("endpoints_total", 0)
    matched_unique = summary.get("matched_unique", 0)
    unmatched = summary.get("unmatched", 0)
    assert total > 0, f"{ehr}: endpoints_total is zero"
    assert unmatched < total, f"{ehr}: 100% of endpoints unmatched"
    rate = matched_unique / total
    floor = MATCHED_UNIQUE_FLOORS[ehr]
    assert rate >= floor, (
        f"{ehr}: matched_unique rate {rate:.2%} below regression floor {floor:.0%} "
        f"({matched_unique}/{total})"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_manual_override_has_justification(ehr: str) -> None:
    """manual_override rows must document why they overrode the auto-resolver.
    The schema doesn't enforce a notes/reason field; this test does."""
    res = _load_resolution(ehr)
    if res is None:
        pytest.skip(f"{ehr}: hospital-overrides absent")
    overrides = [ep for ep in (res.get("endpoints") or []) if ep.get("match_strategy") == "manual_override"]
    if not overrides:
        pytest.skip(f"{ehr}: no manual_override rows")
    bad = []
    for ep in overrides:
        # Accept any of: explicit override_reason, top-level notes, candidates with score=1.0 + matched name.
        reason = ep.get("override_reason") or ep.get("notes")
        if not reason:
            bad.append(ep["endpoint_id"])
    assert not bad, f"{ehr}: manual_override rows missing override_reason/notes: {bad[:5]}"


@pytest.mark.parametrize("ehr", EHRS)
def test_match_summary_arithmetic(ehr: str) -> None:
    """match_summary numbers reconcile: unique + multiple + unmatched == total."""
    res = _load_resolution(ehr)
    if res is None:
        pytest.skip(f"{ehr}: hospital-overrides absent")
    s = res.get("match_summary") or {}
    total = s.get("endpoints_total", 0)
    parts = s.get("matched_unique", 0) + s.get("matched_multiple", 0) + s.get("unmatched", 0)
    assert parts == total, (
        f"{ehr}: match_summary unique+multiple+unmatched={parts} != endpoints_total={total}"
    )
