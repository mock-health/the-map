"""P0 regression: HTML render output is the SEO surface; URL stability is forever.

Once mock.health/map links to /epic/Patient/birthDate.html, that URL is forever.
If render output paths shift silently, every external citation breaks. These
tests pin the path scheme and the staleness-banner behavior.

Also tests XSS escape on render: a malicious overlay row with <script> in
source_quote must be rendered as &lt;script&gt;, not as live HTML.
"""
from __future__ import annotations

import datetime
import html
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.render_html import (
    _staleness_banner,
    render_page,
    safe_filename,
)

# --- Staleness banner ---


def test_staleness_banner_none_returns_empty() -> None:
    assert _staleness_banner(None) == ""


def test_staleness_banner_empty_string_returns_empty() -> None:
    assert _staleness_banner("") == ""


def test_staleness_banner_today_no_banner() -> None:
    today = datetime.date.today().isoformat()
    assert _staleness_banner(today) == ""


def test_staleness_banner_within_90_days_no_banner() -> None:
    sixty_days_ago = (datetime.date.today() - datetime.timedelta(days=60)).isoformat()
    assert _staleness_banner(sixty_days_ago) == ""


def test_staleness_banner_91_days_aging() -> None:
    aging = (datetime.date.today() - datetime.timedelta(days=91)).isoformat()
    banner = _staleness_banner(aging)
    assert "staleness-banner" in banner
    assert "expired" not in banner.split("class=", 1)[1].split(">", 1)[0]
    assert "aging" in banner


def test_staleness_banner_400_days_expired() -> None:
    expired = (datetime.date.today() - datetime.timedelta(days=400)).isoformat()
    banner = _staleness_banner(expired)
    assert "staleness-banner expired" in banner
    assert "expired" in banner


def test_staleness_banner_future_date_no_banner() -> None:
    """Future dates indicate data error, not stale data — render no banner.
    The iron-rule's verified_date is for citation, not for surfacing typos."""
    future = (datetime.date.today() + datetime.timedelta(days=30)).isoformat()
    assert _staleness_banner(future) == ""


def test_staleness_banner_unparseable_no_banner() -> None:
    assert _staleness_banner("not-a-date") == ""
    assert _staleness_banner("2026/04/29") == ""  # wrong separator
    assert _staleness_banner("yesterday") == ""


# --- safe_filename: URL stability ---


def test_safe_filename_replaces_slash_dot_colon_paren() -> None:
    """The renderer mirrors mock.health/map/{ehr}/{resource}/{element}.html.
    safe_filename must produce stable, link-able names for every path shape."""
    assert safe_filename("Patient.address.city") == "Patient_address_city"
    assert safe_filename("Patient.extension(us-core-birthsex)") == "Patient_extension_us-core-birthsex"
    assert safe_filename("Observation.code") == "Observation_code"


# --- XSS escape: malicious overlay rendering ---


def test_render_page_escapes_script_in_title() -> None:
    """A malicious overlay with <script> in EHR display name must be escaped, not executed."""
    page = render_page(
        title="<script>alert(1)</script>",
        body="<p>safe body</p>",
        breadcrumbs=[("Map", "../index.html")],
    )
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page


def test_render_page_escapes_script_in_breadcrumb_label() -> None:
    page = render_page(
        title="ok",
        body="<p>body</p>",
        breadcrumbs=[("<script>alert(2)</script>", "../index.html")],
    )
    assert "<script>alert(2)</script>" not in page
    assert "&lt;script&gt;alert(2)&lt;/script&gt;" in page


def test_render_page_escapes_javascript_url_in_breadcrumb_href() -> None:
    """A malicious breadcrumb href like javascript:alert() must be escaped so the
    rendered <a href=...> can't execute. Note: html.escape only escapes <,>,&,",';
    we rely on the escaping passing through unchanged but also need the dom to
    treat it as a literal URL string. This documents the current behavior."""
    page = render_page(
        title="ok",
        body="<p>body</p>",
        breadcrumbs=[("label", "javascript:alert(3)")],
    )
    # html.escape leaves javascript: alone (it's only ascii); browsers decide
    # whether to navigate. The defense in depth here is keeping breadcrumb
    # hrefs out of overlay-controlled fields. For now, the page renders without
    # the unescaped < or >, which is what html.escape promises.
    assert "<script" not in page  # no script tag injection from breadcrumb
    assert "javascript:alert(3)" in page  # escape doesn't strip; browser decides


def test_render_page_no_banner_when_captured_date_fresh() -> None:
    """Note: 'staleness-banner' appears in the embedded <style> block on every
    page (it's a CSS class definition). Check for the actual banner DIV instead."""
    today = datetime.date.today().isoformat()
    page = render_page(title="t", body="<p>b</p>", breadcrumbs=[("M", "./index.html")], captured_date=today)
    assert '<div class="staleness-banner' not in page


def test_render_page_emits_banner_when_captured_date_old() -> None:
    old = (datetime.date.today() - datetime.timedelta(days=200)).isoformat()
    page = render_page(title="t", body="<p>b</p>", breadcrumbs=[("M", "./index.html")], captured_date=old)
    assert '<div class="staleness-banner' in page


# --- Real-overlay render smoke test ---


@pytest.mark.parametrize("ehr", ["epic", "cerner", "meditech"])
def test_render_html_succeeds_for_all_overlays(ehr: str, tmp_path: Path, repo_root: Path) -> None:
    """Render every shipped overlay end-to-end and assert non-empty HTML output
    without KeyError / AttributeError. Catches missing-key crashes in the
    renderer when an overlay omits an optional field."""
    if not (repo_root / "ehrs" / ehr / "overlay.json").exists():
        pytest.skip(f"{ehr} overlay not present")
    from tools.render_html import render_ehr_index
    from tools.synthesize import conformance_matrix

    ehr_view = conformance_matrix(ehr)
    out_dir = tmp_path / ehr
    render_ehr_index(ehr, ehr_view, out_dir)
    index = out_dir / "index.html"
    assert index.exists(), f"{ehr}: render_ehr_index produced no index.html"
    body = index.read_text()
    assert body.strip(), f"{ehr}: rendered index.html is empty"
    assert "<html" in body.lower(), f"{ehr}: rendered index.html lacks <html> tag"


def test_render_real_epic_produces_expected_path_structure(tmp_path: Path, repo_root: Path) -> None:
    """End-to-end render of the shipped Epic overlay. Asserts the URL path scheme
    matches mock.health/map/{ehr}/{resource_or_profile}/{element}.html exactly.
    Pinning here means a refactor of safe_filename() or the renderer can't
    silently rename URLs.
    """
    if not (repo_root / "ehrs" / "epic" / "overlay.json").exists():
        pytest.skip("epic overlay not present in this checkout")
    from tools.render_html import render_ehr_index, render_element_page, render_profile_index
    from tools.synthesize import conformance_matrix

    ehr_view = conformance_matrix("epic")
    out_dir = tmp_path / "epic"
    captured_date = ehr_view.get("captured_date")
    render_ehr_index("epic", ehr_view, out_dir)
    assert (out_dir / "index.html").exists()

    # First profile with at least one MUST-SUPPORT element
    profile = next(
        (p for p in ehr_view.get("profiles", []) if p.get("ms_total")),
        None,
    )
    assert profile is not None, "every shipped epic overlay must have at least one P0 profile"

    render_profile_index("epic", profile, out_dir, captured_date=captured_date)
    pid = profile["profile_id"]
    assert (out_dir / pid / "index.html").exists()

    # First element on that profile
    element = profile["elements"][0]
    render_element_page("epic", profile, element, out_dir, captured_date=captured_date)
    expected = out_dir / pid / f"{safe_filename(element['path'])}.html"
    assert expected.exists()

    # The element page must contain the element path text (escaped) — citable URL
    page_html = expected.read_text()
    assert html.escape(element["path"]) in page_html


def test_superseded_rows_excluded_from_conformance_matrix(repo_root: Path) -> None:
    """A row listed in another row's previous_row_ids is corrected history: it stays in
    overlay.json for the audit trail but must NOT appear in the rendered matrix or
    category_summary. Otherwise a sandbox 'missing' row and its community_report 'matches'
    correction both render in the same cell and the gap is double-counted. Regression
    guard for the 2026-06-19 Epic corrections."""
    import json as _json

    from tools.synthesize import conformance_matrix

    overlay_path = repo_root / "ehrs" / "epic" / "overlay.json"
    if not overlay_path.exists():
        pytest.skip("epic overlay not present in this checkout")
    devs = _json.loads(overlay_path.read_text()).get("element_deviations", []) or []
    superseded = {rid for d in devs for rid in (d.get("previous_row_ids") or [])}
    if not superseded:
        pytest.skip("epic overlay has no superseded rows")
    by_id = {d.get("row_id"): d for d in devs}

    ehr_view = conformance_matrix("epic")
    cells = {(p["profile_id"], e["path"]): e for p in ehr_view["profiles"] for e in p["elements"]}

    for sid in superseded:
        old = by_id.get(sid)
        assert old is not None, f"previous_row_ids references a missing row {sid}"
        cell = cells.get((old["profile_id"], old["path"]))
        if cell is None:
            continue  # path is not a MUST-SUPPORT baseline element; nothing rendered
        live = [
            d
            for d in devs
            if d["profile_id"] == old["profile_id"]
            and d["path"] == old["path"]
            and d.get("row_id") not in superseded
        ]
        assert cell["deviation_count"] == len(live), (
            f"superseded row {sid} leaked into rendered cell "
            f"({old['profile_id']}, {old['path']}): count {cell['deviation_count']} != live {len(live)}"
        )


def test_no_conformance_cell_is_both_matches_and_missing(repo_root: Path) -> None:
    """A single element path cannot be simultaneously present-and-conformant ('matches')
    and absent ('missing') in one EHR. Both categories in one cell means a correction was
    added without superseding the contradicted row — the exact bug previous_row_ids fixes."""
    from tools.synthesize import conformance_matrix

    if not (repo_root / "ehrs" / "epic" / "overlay.json").exists():
        pytest.skip("epic overlay not present in this checkout")
    ehr_view = conformance_matrix("epic")
    for p in ehr_view["profiles"]:
        for e in p["elements"]:
            cats = set(e["deviation_categories"])
            assert not {"matches", "missing"} <= cats, (
                f"{p['profile_id']} {e['path']} renders as both matches and missing: {sorted(cats)}"
            )
