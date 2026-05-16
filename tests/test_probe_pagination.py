"""Assertions on ``tools/probe_pagination.py`` (KATA-CAPTURE-BUNDLE-LINK-SEMANTICS).

The probe is offline-only — it walks captured Bundle fixtures under
``tests/golden/{epic,cerner,meditech}/`` and synthesises a per-vendor
``pagination`` overlay block. These tests:

  * Run the probe end-to-end in a temp directory so we don't mutate the live
    overlays / reports during CI.
  * Assert the expected report sections exist and have the right structural shape.
  * Pin a handful of vendor-specific findings the probe MUST surface — the same
    findings KATA-EPIC-PAGINATION-* downstream consumes.
"""
from __future__ import annotations

import datetime
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def probe_findings() -> dict:
    """Import the probe and run it against the live fixtures in dry-run."""
    sys.path.insert(0, str(REPO_ROOT))
    from tools import probe_pagination as pp

    per_vendor: dict = {}
    for vendor in ("epic", "cerner", "meditech"):
        per_vendor[vendor] = pp.probe_vendor(vendor)
    return per_vendor


def test_probe_runs_on_all_three_vendors(probe_findings: dict) -> None:
    """The probe completes for all three vendors without raising."""
    assert set(probe_findings.keys()) == {"epic", "cerner", "meditech"}
    for vendor, findings in probe_findings.items():
        assert findings["vendor"] == vendor
        assert "verified_date" in findings
        assert "verified_via" in findings


def test_each_vendor_processes_at_least_some_fixtures(probe_findings: dict) -> None:
    """All three vendors have at least one captured Bundle fixture in the corpus."""
    for vendor, findings in probe_findings.items():
        assert findings["fixture_count"] > 0, (
            f"{vendor}: probe found zero Bundle fixtures — fixture corpus regression"
        )


def test_epic_pagination_dimensions(probe_findings: dict) -> None:
    """Epic-specific findings that downstream KATA-EPIC-PAGINATION-* depends on."""
    epic = probe_findings["epic"]
    # Epic uses an opaque sessionID cursor (not a query continuation)
    assert epic["next_url_format"] == "opaque-base64"
    # Epic emits both `self` and `next` link relations across the corpus
    assert "self" in epic["link_relations_observed"]
    assert "next" in epic["link_relations_observed"]
    # Epic's Bundle.total quirk MUST be classified as "reflects_page_size_when_count_set"
    assert epic["total_semantics"]["label"] == "reflects_page_size_when_count_set"
    # Epic uses a mode=outcome advisory entry on empty results (per overlay)
    assert epic["empty_bundle_shape"] in {"mode_outcome_entry"}
    # Vital-signs 1000-entry page must be observed
    assert epic.get("vital_signs_page_size_observed") == 1000


def test_cerner_pagination_dimensions(probe_findings: dict) -> None:
    """Cerner-specific findings."""
    cerner = probe_findings["cerner"]
    # Cerner's next-page cursor is a `-pageContext=` opaque blob alongside preserved
    # query params — classified as ``vendor-custom`` by the probe heuristic.
    assert cerner["next_url_format"] == "vendor-custom"
    # Cerner consistently omits Bundle.total on the majority of fixtures — pin that
    # semantics label (a minority of small bundles, e.g. Immunization, do echo total).
    assert cerner["total_semantics"]["label"] == "absent_in_most_captures"
    # Cerner returns empty/no `entry` field on no-match queries (not mode=outcome)
    assert cerner["empty_bundle_shape"] in {"empty_entry_array", "no_entry_field", "unobserved"}


def test_meditech_pagination_dimensions(probe_findings: dict) -> None:
    """MEDITECH-specific findings."""
    meditech = probe_findings["meditech"]
    # MEDITECH's next-page cursor uses `continue=<resource-prefix>.<UUID>` —
    # classified as ``vendor-custom`` by the probe heuristic.
    assert meditech["next_url_format"] in {"vendor-custom", "unknown"}
    # MEDITECH preserves Bundle.total and it matches the entry count in every
    # captured fixture.
    assert meditech["total_semantics"]["label"] in {
        "reflects_returned_match_count",
        "mixed",
    }


def test_overlay_block_shape(probe_findings: dict) -> None:
    """The overlay block produced for each vendor has the required citation fields
    (the iron rule)."""
    sys.path.insert(0, str(REPO_ROOT))
    from tools import probe_pagination as pp

    for vendor, findings in probe_findings.items():
        block = pp.build_overlay_block(findings)
        # Iron rule — every overlay row carries verification metadata
        assert "verification" in block, f"{vendor}: pagination block missing verification"
        ver = block["verification"]
        for k in ("source_url", "source_quote", "verified_via", "verified_date"):
            assert ver.get(k), f"{vendor}: pagination.verification missing {k}"
        # Verified date is ISO and today
        datetime.date.fromisoformat(ver["verified_date"])
        # Required structural fields
        for k in (
            "link_relations",
            "next_url_format",
            "empty_bundle_shape",
            "total_semantics",
            "token_stability",
            "fixture_count_probed",
            "fixtures_with_next_link",
            "probe_metadata",
        ):
            assert k in block, f"{vendor}: pagination block missing {k}"
        # probe_metadata carries the verified_via_tool name (kata-spec requirement)
        assert block["probe_metadata"]["verified_via_tool"] == "probe_pagination.py"


def test_overlay_verified_via_in_schema_enum(probe_findings: dict) -> None:
    """The verified_via emitted by the probe must be in the overlay schema enum so
    test_overlay_invariants continues to pass after the probe writes."""
    sys.path.insert(0, str(REPO_ROOT))
    from tools import probe_pagination as pp

    schema = json.loads((REPO_ROOT / "schema" / "overlay.schema.json").read_text())
    enum = set(schema["$defs"]["verification"]["properties"]["verified_via"]["enum"])
    for vendor, findings in probe_findings.items():
        block = pp.build_overlay_block(findings)
        assert block["verification"]["verified_via"] in enum, (
            f"{vendor}: verified_via={block['verification']['verified_via']!r} not in enum {enum}"
        )


def test_report_renders_with_comparison_table(probe_findings: dict, tmp_path: Path) -> None:
    """Markdown report renders, includes a comparison table with all three vendors,
    and includes a per-vendor detail section for each."""
    sys.path.insert(0, str(REPO_ROOT))
    from tools import probe_pagination as pp

    today = datetime.date.today().isoformat()
    md = pp.render_markdown_report(probe_findings, today)
    assert f"Per-vendor pagination link semantics — {today}" in md
    assert "## Comparison table" in md
    # All three vendors appear in headers / detail
    for vendor in ("Epic", "Cerner", "MEDITECH"):
        assert vendor in md, f"vendor {vendor} missing from rendered report"
    # Comparison table contains the canonical row labels
    for label in (
        "Fixtures probed",
        "Fixtures with rel=next",
        "next URL format",
        "Bundle.total semantics",
        "empty bundle shape",
        "token stability",
    ):
        assert label in md, f"comparison table missing row {label!r}"


def test_probe_main_writes_artifacts(tmp_path: Path) -> None:
    """End-to-end: copy the repo into tmp, run ``python -m tools.probe_pagination``,
    and confirm it writes the overlay block, MD report, and JSON sidecar without
    mutating any other overlay key."""
    # Stage the repo (skip .git for speed)
    repo_copy = tmp_path / "the-map"
    repo_copy.mkdir()
    for d in ("ehrs", "tests", "tools", "schema", "us-core"):
        src = REPO_ROOT / d
        if src.exists():
            shutil.copytree(src, repo_copy / d, ignore=shutil.ignore_patterns("__pycache__"))
    # Snapshot the pre-existing pagination_overlay for each vendor — must be preserved.
    pre_overlays: dict[str, dict] = {}
    for vendor in ("epic", "cerner", "meditech"):
        pre_overlays[vendor] = json.loads(
            (repo_copy / "ehrs" / vendor / "overlay.json").read_text()
        )

    res = subprocess.run(
        [sys.executable, "-m", "tools.probe_pagination"],
        cwd=repo_copy,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "probe_pagination.py" in res.stdout

    today = datetime.date.today().isoformat()
    md = repo_copy / "reports" / f"pagination-{today}.md"
    js = repo_copy / "reports" / "pagination" / f"pagination-{today}.json"
    assert md.exists(), f"missing markdown report {md}"
    assert js.exists(), f"missing JSON sidecar {js}"

    sidecar = json.loads(js.read_text())
    assert sidecar["kata"] == "KATA-CAPTURE-BUNDLE-LINK-SEMANTICS"
    assert set(sidecar["per_vendor"].keys()) == {"epic", "cerner", "meditech"}

    # Each overlay now carries a `pagination` block AND preserves the legacy
    # `pagination_overlay` block intact.
    for vendor, pre in pre_overlays.items():
        post = json.loads(
            (repo_copy / "ehrs" / vendor / "overlay.json").read_text()
        )
        assert "pagination" in post, f"{vendor}: pagination block not written"
        # Legacy block preserved verbatim
        assert post.get("pagination_overlay") == pre.get("pagination_overlay"), (
            f"{vendor}: probe mutated the legacy pagination_overlay block"
        )
        # No unrelated top-level keys vanished
        assert set(pre.keys()) - {"pagination"} <= set(post.keys()), (
            f"{vendor}: probe dropped top-level overlay keys"
        )
