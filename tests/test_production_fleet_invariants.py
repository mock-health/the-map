"""Per-vendor production_fleet.json shape invariants.

Asserts harvest reconciliation, cluster sums, modal flag, profile-rate range,
outlier resolution, brands consistency, and (gated) freshness. These run
against the canonical ehrs/{ehr}/production_fleet.json files committed in the
repo.
"""
from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS = ("epic", "cerner", "meditech")
LAUNCH_GATE_DAYS = 14


def _load_fleet(ehr: str) -> dict:
    return json.loads((REPO_ROOT / "ehrs" / ehr / "production_fleet.json").read_text())


@pytest.mark.parametrize("ehr", EHRS)
def test_harvest_summary_reconciliation(ehr: str) -> None:
    fleet = _load_fleet(ehr)
    h = fleet["harvest_summary"]
    assert h["endpoints_attempted"] == h["capstmt_fetched_ok"] + h["capstmt_fetch_failed"], (
        f"{ehr}: harvest_summary attempted ({h['endpoints_attempted']}) "
        f"!= ok ({h['capstmt_fetched_ok']}) + failed ({h['capstmt_fetch_failed']})"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_cluster_endpoint_count_le_fetched_ok(ehr: str) -> None:
    fleet = _load_fleet(ehr)
    clusters = fleet.get("capstmt_shape_clusters", []) or []
    if not clusters:
        pytest.skip(f"{ehr}: no capstmt_shape_clusters")
    total = sum(c["endpoint_count"] for c in clusters)
    fetched_ok = fleet["harvest_summary"]["capstmt_fetched_ok"]
    assert total <= fetched_ok, (
        f"{ehr}: cluster endpoint_count sum ({total}) > capstmt_fetched_ok ({fetched_ok})"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_modal_cluster_per_access_scope(ehr: str) -> None:
    """Exactly one modal cluster per access_scope; modal cluster is the largest within its scope.

    Multi-bundle vendors (Cerner) have one modal per (provider, patient) tier. Single-bundle
    vendors (Epic, Meditech) have one modal overall (treated as a single None scope).
    """
    fleet = _load_fleet(ehr)
    clusters = fleet.get("capstmt_shape_clusters", []) or []
    if not clusters:
        pytest.skip(f"{ehr}: no clusters")

    by_scope: dict[str, list[dict]] = {}
    for c in clusters:
        scope = c.get("access_scope") or "_default"
        by_scope.setdefault(scope, []).append(c)

    for scope, items in by_scope.items():
        modal = [c for c in items if c.get("modal") is True]
        assert len(modal) == 1, (
            f"{ehr}/{scope}: expected exactly 1 modal cluster, got {len(modal)} "
            f"({[c['cluster_id'] for c in modal]})"
        )
        largest = max(items, key=lambda c: c["endpoint_count"])
        assert modal[0]["cluster_id"] == largest["cluster_id"], (
            f"{ehr}/{scope}: modal cluster {modal[0]['cluster_id']} "
            f"({modal[0]['endpoint_count']}) is not the largest "
            f"({largest['cluster_id']} = {largest['endpoint_count']})"
        )


@pytest.mark.parametrize("ehr", EHRS)
def test_us_core_support_rate_in_range(ehr: str) -> None:
    fleet = _load_fleet(ehr)
    rows = fleet.get("us_core_profile_support_rate", []) or []
    bad = [r for r in rows if not (0.0 <= r["fraction"] <= 1.0)]
    assert not bad, f"{ehr}: us_core_profile_support_rate fractions out of [0,1]: {bad[:5]}"


@pytest.mark.parametrize("ehr", EHRS)
def test_outlier_endpoints_resolve_to_non_modal_cluster(ehr: str) -> None:
    fleet = _load_fleet(ehr)
    outliers = fleet.get("outlier_endpoints", []) or []
    if not outliers:
        pytest.skip(f"{ehr}: no outliers")
    cluster_by_id = {c["cluster_id"]: c for c in fleet.get("capstmt_shape_clusters", []) or []}
    bad: list[str] = []
    for o in outliers:
        cid = o.get("cluster_id")
        cluster = cluster_by_id.get(cid)
        if cluster is None:
            bad.append(f"outlier {o['endpoint_id']}: cluster_id {cid!r} not in capstmt_shape_clusters")
        elif cluster.get("modal") is True:
            bad.append(f"outlier {o['endpoint_id']}: belongs to MODAL cluster {cid}")
    assert not bad, f"{ehr}: outlier resolution failures:\n  " + "\n  ".join(bad)


@pytest.mark.parametrize("ehr", EHRS)
def test_brands_bundle_total_consistent(ehr: str) -> None:
    """brands_bundle_total_endpoints should equal endpoints_attempted unless harvest_summary
    documents a divergence in notes. Multi-bundle vendors may legitimately attempt > total
    if they union multiple bundles."""
    fleet = _load_fleet(ehr)
    bundle_total = fleet["brands_bundle_total_endpoints"]
    attempted = fleet["harvest_summary"]["endpoints_attempted"]
    notes = fleet["harvest_summary"].get("notes", "") or ""
    if bundle_total == attempted:
        return
    # Multi-bundle vendors (Cerner unions provider + patient) get a pass if the
    # brands_bundle_source_url contains multiple paths (newline-delimited per schema).
    multi_bundle = "\n" in (fleet.get("brands_bundle_source_url") or "")
    if multi_bundle:
        return
    assert notes, (
        f"{ehr}: brands_bundle_total_endpoints ({bundle_total}) != endpoints_attempted "
        f"({attempted}) and harvest_summary.notes is empty (no documented divergence)"
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_captured_date_within_launch_gate(ehr: str) -> None:
    """Hard fail when THE_MAP_LAUNCH_GATE=1 and captured_date is older than 14 days.
    Otherwise emits no failure (relies on the existing fleet-stale WARN in tools/validate.py)."""
    if os.environ.get("THE_MAP_LAUNCH_GATE") != "1":
        pytest.skip("THE_MAP_LAUNCH_GATE not set; freshness gate inactive")
    fleet = _load_fleet(ehr)
    captured = datetime.date.fromisoformat(fleet["captured_date"])
    age = (datetime.date.today() - captured).days
    assert age <= LAUNCH_GATE_DAYS, (
        f"{ehr}: captured_date {fleet['captured_date']} is {age} days old "
        f"(launch gate requires ≤ {LAUNCH_GATE_DAYS}). Re-run "
        f"`python -m tools.harvest_production_capstmts {ehr} && "
        f"python -m tools.analyze_fleet_drift {ehr}`."
    )


@pytest.mark.parametrize("ehr", EHRS)
def test_fleet_verified_via_matches_vendor(ehr: str) -> None:
    """The verified_via on the fleet must encode the vendor (e.g. epic_production_fleet)."""
    fleet = _load_fleet(ehr)
    via = fleet["verification"]["verified_via"]
    assert via.startswith(ehr), (
        f"{ehr}: production_fleet.verification.verified_via={via!r} should start with {ehr!r}"
    )
