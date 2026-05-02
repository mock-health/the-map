"""Render the Map's SEO-browseable HTML pages.

Produces a static site under `reports/{quarter}/html/{ehr}/`:
  - index.html              EHR landing page (links per resource)
  - {resource}/index.html   per-resource landing (links per element)
  - {resource}/{element}.html  one page per US Core MUST-SUPPORT element

Each leaf page shows: spec requirement (cardinality, type, binding), what the EHR
actually does (deviation category, observed sample, multi-patient evidence), and
links to the citing golden fixture.

Output paths are designed to map to `mock.health/map/{ehr}/{resource}/{element}`
under any static host with directory-style URLs.

Usage:
    python -m tools.render_html epic
    python -m tools.render_html epic --out=reports/2026-q2/html
"""
from __future__ import annotations

import argparse
import datetime
import html
import json
import sys
from pathlib import Path

from tools.synthesize import conformance_matrix

REPO_ROOT = Path(__file__).resolve().parent.parent

CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1rem; color: #222; line-height: 1.5; }
h1, h2, h3 { line-height: 1.2; }
header { border-bottom: 1px solid #ddd; padding-bottom: 0.5rem; margin-bottom: 1.5rem; }
header a { color: #0366d6; text-decoration: none; }
table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
th, td { padding: 0.5rem 0.75rem; border-bottom: 1px solid #eee; text-align: left; vertical-align: top; }
th { background: #f6f8fa; }
code { background: #f6f8fa; padding: 0 0.25em; border-radius: 3px; font-size: 0.9em; }
.cat-matches { color: #098039; font-weight: 600; }
.cat-missing { color: #b04020; font-weight: 600; }
.cat-vs { color: #b06010; font-weight: 600; }
.cat-card { color: #b04060; font-weight: 600; }
.cat-fmt { color: #804060; font-weight: 600; }
.cat-unmeasured { color: #999; font-style: italic; }
.evidence { background: #f9f9f9; padding: 0.5rem 0.75rem; border-left: 3px solid #ddd; margin: 0.5rem 0; font-family: monospace; font-size: 0.85em; }
.fleet-panel { background: #eef5fb; border-left: 3px solid #0366d6; padding: 0.75rem 1rem; margin: 1rem 0; }
.fleet-panel h3 { margin-top: 0; }
.fleet-bar { display: inline-block; height: 0.6em; background: linear-gradient(to right, #098039, #b06010, #b04020); border-radius: 3px; margin: 0 0.5em; }
.fleet-pct { font-weight: 600; }
.fleet-low { color: #b04020; }
.fleet-mid { color: #b06010; }
.fleet-high { color: #098039; }
.staleness-banner { background: #fff8e1; border-left: 4px solid #f0a020; padding: 0.6rem 0.9rem; margin: 0 0 1rem; font-size: 0.92em; }
.staleness-banner.expired { background: #fde7e2; border-left-color: #b04020; }
.staleness-banner strong { color: #804010; }
.staleness-banner.expired strong { color: #802010; }
nav { font-size: 0.9em; color: #666; }
nav a { color: #0366d6; }
"""

# Days after which a captured_date triggers a staleness banner. After 365 days
# the banner darkens to "expired" red. None or future dates → no banner.
STALENESS_WARN_DAYS = 90
STALENESS_EXPIRED_DAYS = 365


def _staleness_banner(captured_date: str | None) -> str:
    """Return a yellow/red banner if captured_date is more than 90 days old.

    captured_date is ISO 8601 (YYYY-MM-DD). Future dates and unparseable
    strings render no banner — the iron rule's verified_date is for citation
    audit, not for surfacing data errors.
    """
    if not captured_date:
        return ""
    try:
        captured = datetime.date.fromisoformat(captured_date[:10])
    except (ValueError, TypeError):
        return ""
    today = datetime.date.today()
    age_days = (today - captured).days
    if age_days <= STALENESS_WARN_DAYS:
        return ""
    css_class = "staleness-banner expired" if age_days >= STALENESS_EXPIRED_DAYS else "staleness-banner"
    label = "expired" if age_days >= STALENESS_EXPIRED_DAYS else "aging"
    return (
        f'<div class="{css_class}"><strong>Data {label}.</strong> '
        f"Captured {html.escape(captured_date)} — that's {age_days} days ago. "
        "A vendor's CapabilityStatement may have moved on; verify findings against "
        "the source URL on each row before citing. "
        '<a href="https://github.com/mock-health/the-map/blob/main/CONTRIBUTING.md#refreshing-an-ehrs-data">refresh instructions →</a></div>'
    )

CATEGORY_CSS = {
    "matches": "cat-matches",
    "missing": "cat-missing",
    "value-set-mismatch": "cat-vs",
    "value-set-narrowed": "cat-vs",
    "value-set-unverified-locally": "cat-vs",
    "cardinality-min-violated": "cat-card",
    "cardinality-max-violated": "cat-card",
    "format-violation": "cat-fmt",
    "extra-extension": "cat-vs",
}


def safe_filename(s: str) -> str:
    return s.replace("/", "_").replace(".", "_").replace(":", "_").replace("(", "_").replace(")", "")


def render_page(*, title: str, body: str, breadcrumbs: list[tuple[str, str]], captured_date: str | None = None) -> str:
    crumbs = " / ".join(f'<a href="{html.escape(href)}">{html.escape(label)}</a>' for label, href in breadcrumbs)
    banner = _staleness_banner(captured_date)
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<style>{CSS}</style>
</head><body>
<header><nav>{crumbs}</nav><h1>{html.escape(title)}</h1></header>
{banner}
{body}
<footer><hr><p style="font-size:0.85em;color:#777">Auto-generated by tools/render_html.py from ehrs/{{ehr}}/CapabilityStatement.json + overlay.json. <a href="https://github.com/mock-health/the-map">source</a></p></footer>
</body></html>"""


def _fleet_pct_css(frac: float | None) -> str:
    if frac is None:
        return ""
    if frac >= 0.85:
        return "fleet-high"
    if frac >= 0.50:
        return "fleet-mid"
    return "fleet-low"


def _fleet_cell(p: dict) -> str:
    """Render the production-fleet support-rate cell for a profile."""
    frac = p.get("fleet_support_rate")
    if frac is None:
        return '<td><em style="color:#999">no fleet data</em></td>'
    n = p.get("fleet_customers_advertising") or 0
    d = p.get("fleet_customers_with_capstmt") or 0
    cls = _fleet_pct_css(frac)
    return f'<td><span class="fleet-pct {cls}">{frac * 100:.1f}%</span> <small style="color:#666">({n}/{d})</small></td>'


def render_ehr_index(ehr: str, ehr_view: dict, out_dir: Path) -> None:
    profiles = sorted(ehr_view.get("profiles", []), key=lambda p: (p.get("priority") or "P9", p.get("profile_id")))
    rows = []
    for p in profiles:
        if not p.get("ms_total"):
            continue
        measured = p.get("elements_measured") or 0
        ms = p.get("ms_total")
        link = f"{p['profile_id']}/index.html"
        rows.append(
            f'<tr><td>{html.escape(p.get("priority") or "?")}</td>'
            f'<td><a href="{link}">{html.escape(p["profile_id"])}</a></td>'
            f'<td>{html.escape(p.get("resource_type") or "?")}</td>'
            f'<td>{ms}</td><td>{measured}</td>'
            f'<td>{"yes" if p.get("ehr_declares_support") else "<em>not in CapStmt</em>"}</td>'
            f'{_fleet_cell(p)}</tr>'
        )
    summary = ehr_view.get("category_summary", {})
    sumlines = "".join(f"<li>{html.escape(k)}: {v}</li>" for k, v in sorted(summary.items()))
    fleet_panel = _ehr_fleet_panel(ehr_view.get("production_fleet"))
    body = f"""
<p>Captured {html.escape(ehr_view.get('captured_date') or '?')} against US Core baseline {html.escape(ehr_view.get('baseline_version') or '?')}.</p>
{fleet_panel}
<h2>Conformance summary</h2><ul>{sumlines}</ul>
<h2>Profiles</h2>
<table><thead><tr><th>Priority</th><th>Profile</th><th>Resource</th><th>MUST-SUPPORT</th><th>Measured</th><th>Declared in CapStmt</th><th>Fleet support</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
"""
    page = render_page(
        title=f"Map — {ehr}",
        body=body,
        breadcrumbs=[("Map", "../index.html"), (ehr, "./index.html")],
        captured_date=ehr_view.get("captured_date"),
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(page)


def _ehr_fleet_panel(fleet: dict | None) -> str:
    """Production-fleet headline panel for the EHR landing page."""
    if not fleet:
        return ""
    h = fleet.get("harvest_summary", {})
    clusters = fleet.get("capstmt_shape_clusters", [])
    sw_dist = fleet.get("software_distribution", {})
    top_sw = list(sw_dist.items())[:3]
    sw_html = ", ".join(f"<code>{html.escape(k)}</code> ({v})" for k, v in top_sw) if top_sw else "(none)"
    cluster_summary = ""
    if clusters:
        modal = clusters[0]
        cluster_summary = (
            f"{len(clusters)} CapStmt shape cluster(s); modal <strong>{html.escape(modal['cluster_id'])}</strong> "
            f"covers {modal['endpoint_count']} endpoints with {modal['supported_profiles_total']} profiles. "
            f'<a href="fleet/index.html">drift detail →</a>'
        )
    return f"""
<div class="fleet-panel">
  <h3>Production fleet ({html.escape(fleet.get('captured_date') or '?')})</h3>
  <p>Harvested <strong>{h.get('endpoints_attempted', 0)}</strong> customer endpoints from
     <code>{html.escape(fleet.get('brands_bundle_source_url') or '?')}</code>.
     {h.get('capstmt_fetched_ok', 0)} returned a usable CapabilityStatement
     ({100 * h.get('capstmt_fetched_ok', 0) // max(1, h.get('endpoints_attempted', 1))}%).</p>
  <p>Software: {sw_html}</p>
  <p>{cluster_summary}</p>
</div>
"""


def render_profile_index(ehr: str, profile: dict, out_dir: Path, *, captured_date: str | None = None) -> None:
    pid = profile["profile_id"]
    rows = []
    for el in profile.get("elements", []):
        cats = el.get("deviation_categories") or ["unmeasured"]
        cat_html = " ".join(
            f'<span class="{CATEGORY_CSS.get(c, "cat-unmeasured")}">{html.escape(c)}</span>'
            for c in cats
        )
        link = f"{safe_filename(el['path'])}.html"
        rows.append(
            f'<tr><td><a href="{link}"><code>{html.escape(el["path"])}</code></a></td>'
            f'<td>{html.escape(el.get("cardinality") or "?")}</td>'
            f'<td>{html.escape(", ".join(el.get("type") or []))}</td>'
            f'<td>{cat_html}</td></tr>'
        )
    body = f"""
<p>{profile.get('ms_total', 0)} MUST-SUPPORT elements (US Core 6.1.0); {profile.get('elements_measured', 0)} measured by Phase B sweep.</p>
<table><thead><tr><th>Path</th><th>Cardinality</th><th>Type</th><th>Status</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
"""
    page = render_page(
        title=f"{pid} — {ehr}",
        body=body,
        breadcrumbs=[("Map", "../../index.html"), (ehr, "../index.html"), (pid, "./index.html")],
        captured_date=captured_date,
    )
    p_dir = out_dir / pid
    p_dir.mkdir(parents=True, exist_ok=True)
    (p_dir / "index.html").write_text(page)


def _element_fleet_block(profile: dict) -> str:
    """Production-reality block on the per-element page.

    The fleet support rate is per-PROFILE, not per-element — vendors don't
    publish per-element advertised support, only `supportedProfile` per
    resource. So the block is identical for all elements within one profile,
    but it earns its keep by surfacing the production-reality denominator
    on the page where the integrator is currently looking.
    """
    frac = profile.get("fleet_support_rate")
    if frac is None:
        return ""  # No fleet snapshot for this EHR; render nothing.
    n = profile.get("fleet_customers_advertising") or 0
    d = profile.get("fleet_customers_with_capstmt") or 0
    cls = _fleet_pct_css(frac)
    pct = f"{frac * 100:.1f}%"
    if frac < 0.50:
        bullet = (
            f"Only <strong>{n}</strong> of {d} reachable customers advertise this profile. "
            f"If you build against the sandbox alone, expect most production deployments NOT to expose this profile in their CapabilityStatement."
        )
    elif frac < 0.85:
        bullet = (
            f"<strong>{n}</strong> of {d} reachable customers advertise this profile — partial fleet rollout. "
            f"Worth defensive-coding for the {d - n} customers ({((d - n) / max(1, d) * 100):.0f}%) where the profile is absent."
        )
    else:
        bullet = (
            f"<strong>{n}</strong> of {d} reachable customers advertise this profile (≥85%, modal-cluster behavior). "
            f"Sandbox behavior is a reasonable proxy for production for this profile."
        )
    return f"""
<h2>Production reality</h2>
<div class="fleet-panel">
<p><strong>Fleet support rate</strong>: <span class="fleet-pct {cls}">{pct}</span> across reachable customer endpoints in this vendor's HTI-1 §170.404 brands bundle.</p>
<p>{bullet}</p>
<p style="font-size:0.85em;color:#666"><a href="../fleet/index.html">See full fleet drift report →</a> · This number reflects <em>claimed</em> CapabilityStatement support, not Phase B-measured per-element compliance.</p>
</div>
"""


def render_element_page(ehr: str, profile: dict, element: dict, out_dir: Path, *, captured_date: str | None = None) -> None:
    pid = profile["profile_id"]
    path = element["path"]
    cats = element.get("deviation_categories") or ["unmeasured"]
    cat_html = " ".join(
        f'<span class="{CATEGORY_CSS.get(c, "cat-unmeasured")}">{html.escape(c)}</span>'
        for c in cats
    )
    binding = element.get("binding") or {}
    binding_html = ""
    if binding:
        bs = binding.get("strength")
        bv = binding.get("valueSet_id")
        binding_html = f"<p><strong>Binding:</strong> {html.escape(bs or '?')} → <code>{html.escape(bv or '?')}</code></p>"

    samples = element.get("sample_observed") or []
    sample_html = ""
    if samples:
        sample_html = "<h2>Observed samples</h2>"
        for s in samples:
            mp = s.get("multi_patient_evidence") or {}
            sample_html += (
                f'<div class="evidence"><strong>{html.escape(s.get("category", ""))}</strong> — '
                f'{html.escape(s.get("observed_in_ehr", ""))[:400]}'
                f'{"<br>multi-patient: " + html.escape(json.dumps(mp)) if mp else ""}</div>'
            )
    else:
        sample_html = "<p><em>No deviation row recorded for this element.</em></p>"

    fleet_block = _element_fleet_block(profile)
    body = f"""
<p><strong>Resource:</strong> <code>{html.escape(profile.get('resource_type') or '?')}</code></p>
<p><strong>Cardinality:</strong> <code>{html.escape(element.get('cardinality') or '?')}</code></p>
<p><strong>Type:</strong> {html.escape(', '.join(element.get('type') or []))}</p>
{binding_html}
<p><strong>MUST-SUPPORT:</strong> {element.get('must_support')}, <strong>USCDI requirement:</strong> {element.get('uscdi_requirement')}</p>
<h2>Status</h2>
<p>{cat_html}</p>
{sample_html}
{fleet_block}
"""
    page = render_page(
        title=f"{path} — {pid} — {ehr}",
        body=body,
        breadcrumbs=[("Map", "../../../index.html"), (ehr, "../../index.html"), (pid, "../index.html"), (path, "")],
        captured_date=captured_date,
    )
    p_dir = out_dir / pid
    p_dir.mkdir(parents=True, exist_ok=True)
    (p_dir / f"{safe_filename(path)}.html").write_text(page)


def render_fleet_index(ehr: str, fleet: dict, out_dir: Path) -> None:
    """Per-EHR fleet landing page — software histogram, cluster table, profile
    support-rate table, outliers list."""
    sw_rows = "".join(
        f"<tr><td><code>{html.escape(k)}</code></td><td>{v}</td></tr>"
        for k, v in fleet.get("software_distribution", {}).items()
    )
    fv_rows = "".join(
        f"<tr><td><code>{html.escape(k)}</code></td><td>{v}</td></tr>"
        for k, v in fleet.get("fhir_version_distribution", {}).items()
    )
    cluster_rows = "".join(
        f'<tr><td><a href="clusters/{html.escape(c["cluster_id"])}.html">{html.escape(c["cluster_id"])}</a>'
        f"{' <strong>(modal)</strong>' if c.get('modal') else ''}</td>"
        f"<td>{c['endpoint_count']}</td><td>{c['supported_profiles_total']}</td>"
        f"<td>{len(c.get('resources_advertised', []))}</td>"
        f"<td><code>{html.escape(c.get('example_endpoint_address') or '')}</code></td></tr>"
        for c in fleet.get("capstmt_shape_clusters", [])
    )
    profile_rows = []
    for p in fleet.get("us_core_profile_support_rate", []):
        if not p.get("in_us_core_baseline"):
            continue
        cls = _fleet_pct_css(p["fraction"])
        profile_rows.append(
            f'<tr><td><code>{html.escape(p["profile_id"])}</code></td>'
            f'<td><span class="fleet-pct {cls}">{p["fraction"] * 100:.1f}%</span></td>'
            f'<td>{p["customers_advertising"]}/{p["customers_with_capstmt"]}</td>'
            f'<td>{"yes" if p.get("absent_in_modal_cluster") else "no"}</td></tr>'
        )
    profile_rows_html = "".join(profile_rows)

    smart = fleet.get("smart_config_drift", {})
    smart_html = ""
    if smart:
        scope_dist = smart.get("scope_count_distribution", {})
        scope_rows = "".join(
            f"<tr><td>{html.escape(k)}</td><td>{v}</td></tr>"
            for k, v in scope_dist.items()
        )
        modal_caps = smart.get("capabilities_modal_set", [])
        smart_html = f"""
<h2>SMART configuration drift</h2>
<p><strong>Modal capabilities set</strong> ({len(modal_caps)} items):
{', '.join(f'<code>{html.escape(c)}</code>' for c in modal_caps[:20])}</p>
<h3>scopes_supported count distribution</h3>
<table><thead><tr><th>bucket</th><th>customers</th></tr></thead><tbody>{scope_rows}</tbody></table>
"""

    outliers = fleet.get("outlier_endpoints", [])
    outlier_rows = "".join(
        f"<tr><td><code>{html.escape(o['endpoint_id'])}</code></td>"
        f"<td><code>{html.escape(o.get('address') or '')}</code></td>"
        f"<td>{html.escape(o['cluster_id'])}</td>"
        f"<td>{html.escape(o.get('deviation_summary') or '')}</td></tr>"
        for o in outliers[:100]
    )
    outlier_html = ""
    if outliers:
        outlier_html = f"""
<h2>Outliers ({len(outliers)} non-modal endpoints)</h2>
<table><thead><tr><th>Endpoint</th><th>Address</th><th>Cluster</th><th>Deviation</th></tr></thead>
<tbody>{outlier_rows}</tbody></table>
{f'<p><em>Showing first 100 of {len(outliers)}; full list in production_fleet.json</em></p>' if len(outliers) > 100 else ''}
"""

    h = fleet.get("harvest_summary", {})
    body = f"""
<p>Snapshot {html.escape(fleet.get('captured_date') or '?')}, sourced from
   <code>{html.escape(fleet.get('brands_bundle_source_url') or '?')}</code>.</p>

<h2>Harvest summary</h2>
<ul>
  <li><strong>{h.get('endpoints_attempted', 0)}</strong> endpoints attempted</li>
  <li><strong>{h.get('capstmt_fetched_ok', 0)}</strong> CapabilityStatements ({100 * h.get('capstmt_fetched_ok', 0) // max(1, h.get('endpoints_attempted', 1))}%)</li>
  <li><strong>{h.get('smart_config_fetched_ok', 0)}</strong> SMART configurations ({100 * h.get('smart_config_fetched_ok', 0) // max(1, h.get('endpoints_attempted', 1))}%)</li>
  <li>Wall clock: {h.get('wall_clock_seconds', '?')}s at concurrency={h.get('concurrency', '?')}</li>
  <li>Failure categories: {html.escape(json.dumps(h.get('failure_categories', {})))}</li>
</ul>

<h2>Software distribution</h2>
<table><thead><tr><th>software.name + version</th><th>count</th></tr></thead><tbody>{sw_rows}</tbody></table>

<h2>FHIR version distribution</h2>
<table><thead><tr><th>fhirVersion</th><th>count</th></tr></thead><tbody>{fv_rows}</tbody></table>

<h2>CapabilityStatement shape clusters</h2>
<p>Endpoints grouped by sorted-canonical hash of their REST surface — same shape = same FHIR API to a client.</p>
<table><thead><tr><th>Cluster</th><th>Endpoints</th><th>Profiles</th><th>Resources</th><th>Example address</th></tr></thead>
<tbody>{cluster_rows}</tbody></table>

<h2>US Core profile support rate (baseline only)</h2>
<p>Per US Core 6.1 profile: how many reachable customers list it in <code>supportedProfile</code>.</p>
<table><thead><tr><th>Profile</th><th>Support</th><th>n / total</th><th>Absent in modal</th></tr></thead>
<tbody>{profile_rows_html}</tbody></table>

{smart_html}
{outlier_html}
"""
    page = render_page(
        title=f"Production fleet — {ehr}",
        body=body,
        breadcrumbs=[("Map", "../../index.html"), (ehr, "../index.html"), ("fleet", "./index.html")],
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "index.html").write_text(page)


def render_fleet_cluster_page(ehr: str, cluster: dict, out_dir: Path) -> None:
    """One page per CapStmt shape cluster."""
    profiles_html = "".join(
        f"<li><code>{html.escape(p)}</code></li>"
        for p in cluster.get("supported_profiles", [])
    )
    resources_html = ", ".join(f"<code>{html.escape(r)}</code>" for r in cluster.get("resources_advertised", []))
    body = f"""
<p>{cluster['endpoint_count']} endpoints share this CapStmt shape (hash <code>{html.escape(cluster['shape_hash'])}</code>).
   {'<strong>This is the modal cluster</strong> — the fleet-reference deployment.' if cluster.get('modal') else ''}</p>
<h2>Example endpoint</h2>
<p><code>{html.escape(cluster.get('example_endpoint_address') or '?')}</code> ({html.escape(cluster.get('example_endpoint_id') or '?')})</p>
<h2>Resources advertised ({len(cluster.get('resources_advertised', []))})</h2>
<p>{resources_html}</p>
<h2>Supported profiles ({cluster['supported_profiles_total']})</h2>
<ul>{profiles_html}</ul>
"""
    page = render_page(
        title=f"{cluster['cluster_id']} — {ehr}",
        body=body,
        breadcrumbs=[("Map", "../../../index.html"), (ehr, "../../index.html"), ("fleet", "../index.html"), (cluster["cluster_id"], "")],
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{cluster['cluster_id']}.html").write_text(page)


def render_root_index(ehrs: list[str], out_root: Path, *, has_cross_vendor_fleet: bool = False) -> None:
    rows = "".join(f'<li><a href="{e}/index.html">{html.escape(e)}</a></li>' for e in ehrs)
    cross_link = ""
    if has_cross_vendor_fleet:
        cross_link = '<h2>Cross-vendor</h2><ul><li><a href="cross-vendor-fleet.html">Per-profile fleet support across all vendors</a></li></ul>'
    body = f"<h2>EHR Maps</h2><ul>{rows}</ul>{cross_link}"
    page = render_page(title="The Map", body=body, breadcrumbs=[("Map", "./index.html")])
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "index.html").write_text(page)


# Canonical US Core profile id → list of FHIR-core / vendor-shorthand variants
# vendors advertise instead. The substituted id covers the same clinical content
# but breaks any tooling that joins on the US Core id literally.
#
# Empirical drivers (2026-04-27 fleet harvest):
#   - MEDITECH advertises FHIR-core lowercase ids (bp, bmi, bodyheight) at ~96%
#     and `us-core-*` at 0% across the same vital-signs profiles
#   - Cerner lists `us-core-*` on the modal cluster + the FHIR-core alias only
#     in a tiny outlier tail (0.14%)
#   - Epic uses only `us-core-*` ids
#
# The cross-vendor view should pick the best datum per vendor (canonical first,
# alias fallback) and annotate when an alias was used. We do NOT silently merge:
# the substitution is itself a finding.
PROFILE_ID_ALIASES: dict[str, list[str]] = {
    "us-core-blood-pressure": ["bp"],
    "us-core-bmi": ["bmi"],
    "us-core-body-height": ["bodyheight"],
    "us-core-body-weight": ["bodyweight"],
    "us-core-body-temperature": ["bodytemp"],
    "us-core-head-circumference": ["headcircum"],
    "us-core-heart-rate": ["heartrate"],
    "us-core-respiratory-rate": ["resprate"],
    # `oxygensat` is the FHIR-core SpO2 vital; us-core-pulse-oximetry is more
    # specific (US Core narrows the LOINC value-set). They're not literally the
    # same profile, but in the cross-vendor view they're advertising the same
    # clinical concept; treat as alias and annotate.
    "us-core-pulse-oximetry": ["oxygensat"],
    "us-core-vital-signs": ["vitalspanel"],
}

# Reverse lookup: alias id → canonical id. Built once at import.
_ALIAS_TO_CANONICAL: dict[str, str] = {
    alias: canonical
    for canonical, aliases in PROFILE_ID_ALIASES.items()
    for alias in aliases
}


def render_cross_vendor_fleet(out_root: Path) -> bool:
    """Render reports/{quarter}/html/cross-vendor-fleet.html: per-profile multi-vendor
    support-rate table joining ehrs/{ehr}/production_fleet.json across all vendors that
    have a snapshot. Vendors missing a snapshot show "—" for every cell.

    Returns True if the page was written (≥1 vendor had a fleet snapshot), else False.
    """
    fleets: dict[str, dict] = {}
    for d in sorted((REPO_ROOT / "ehrs").iterdir()):
        fleet_path = d / "production_fleet.json"
        if fleet_path.exists():
            try:
                fleets[d.name] = json.loads(fleet_path.read_text())
            except json.JSONDecodeError:
                continue
    if not fleets:
        return False

    vendor_order = sorted(fleets)

    # Per-vendor profile_id → entry lookup
    by_vendor: dict[str, dict[str, dict]] = {
        v: {p["profile_id"]: p for p in fleets[v].get("us_core_profile_support_rate", [])}
        for v in vendor_order
    }

    # Build the row list: every profile_id we saw, but with FHIR-core aliases
    # folded into their canonical us-core-* id. The aliases drop out of the row
    # list (rendered as part of the canonical row instead).
    profile_set: set[str] = set()
    for v in vendor_order:
        for pid in by_vendor[v]:
            profile_set.add(_ALIAS_TO_CANONICAL.get(pid, pid))
    profiles = sorted(profile_set)

    def _pick_for_vendor(v: str, canonical_pid: str) -> tuple[dict | None, str | None]:
        """Return (best entry, alias_used_or_None) for this vendor + canonical id.

        Picks canonical first; falls back to any alias if canonical isn't
        advertised. Treats an explicit 0.0% canonical entry as 'not really
        advertised' when an alias is present at non-zero — that's the MEDITECH
        case where us-core-blood-pressure is listed at 0% and `bp` at 96%.
        """
        canonical_entry = by_vendor[v].get(canonical_pid)
        canonical_frac = (canonical_entry or {}).get("fraction") if canonical_entry else None
        for alias in PROFILE_ID_ALIASES.get(canonical_pid, []):
            alias_entry = by_vendor[v].get(alias)
            if not alias_entry:
                continue
            alias_frac = alias_entry.get("fraction")
            # Use the alias when it's a strictly better datum (canonical is missing,
            # canonical reports zero, or alias reports much higher).
            if (
                canonical_entry is None
                or (canonical_frac in (None, 0, 0.0) and alias_frac and alias_frac > 0)
            ):
                return alias_entry, alias
        return canonical_entry, None

    def _cell(v: str, canonical_pid: str) -> str:
        entry, alias = _pick_for_vendor(v, canonical_pid)
        if not entry:
            return '<td><em style="color:#999">—</em></td>'
        frac = entry.get("fraction")
        if frac is None:
            return '<td><em style="color:#999">no data</em></td>'
        n = entry.get("customers_advertising") or 0
        d = entry.get("customers_with_capstmt") or 0
        cls = _fleet_pct_css(frac)
        suffix = ""
        if alias:
            suffix = f' <small style="color:#b06010" title="Vendor advertises this profile under FHIR-core id {alias!r}, not the US Core canonical id">(via <code>{html.escape(alias)}</code>)</small>'
        return f'<td><span class="fleet-pct {cls}">{frac * 100:.1f}%</span> <small style="color:#666">({n}/{d})</small>{suffix}</td>'

    rows = []
    for pid in profiles:
        cells = "".join(_cell(v, pid) for v in vendor_order)
        # Baseline marker: prefer canonical entry's flag; fall back to any alias's
        baseline = False
        for v in vendor_order:
            entry, _ = _pick_for_vendor(v, pid)
            if entry and entry.get("in_us_core_baseline"):
                baseline = True
                break
        baseline_cell = '✔' if baseline else '<em style="color:#999">non-baseline</em>'
        rows.append(f'<tr><td><code>{html.escape(pid)}</code></td><td>{baseline_cell}</td>{cells}</tr>')

    captured_dates = ", ".join(
        f"{html.escape(v)} {html.escape(fleets[v].get('captured_date') or '?')}"
        for v in vendor_order
    )
    vendor_headers = "".join(
        f'<th>{html.escape(v)} <small style="color:#666">({fleets[v].get("brands_bundle_total_endpoints", "?")} endpts)</small></th>'
        for v in vendor_order
    )
    body = f"""
<p>Per-profile US Core <code>supportedProfile</code> declaration rate across vendor production fleets.
Each cell reports <em>customers advertising the profile / customers whose CapabilityStatement we successfully retrieved</em>.
"—" means we have no fleet snapshot for that vendor yet.</p>
<p style="color:#666">Snapshots: {captured_dates}.</p>

<table>
<thead><tr><th>Profile</th><th>US Core baseline?</th>{vendor_headers}</tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>

<h2>How to read this</h2>
<ul>
  <li><strong>Color</strong>: green ≥ 85%, orange 50–84%, red &lt; 50%, plain gray = no data.</li>
  <li><strong>Cell value</strong>: the fraction of that vendor's reachable customers that list the profile in their CapabilityStatement's <code>rest.resource[].supportedProfile</code> — i.e. <em>claimed</em> support, not measured Phase B compliance.</li>
  <li><strong>Non-baseline profiles</strong> appear when at least one vendor advertises a profile not in the latest US Core baseline (vendor-specific extensions, deprecated profiles, etc.).</li>
  <li><strong>"—"</strong> for an entire column: no production_fleet.json snapshot is on file for that vendor yet.</li>
  <li><strong>(via <code>bp</code>)</strong> annotations mean the vendor advertises this profile under a FHIR-core id (e.g. <code>bp</code> for blood pressure) instead of the US Core canonical id (<code>us-core-blood-pressure</code>). Same clinical content, different namespace. The cell shows whichever variant the vendor actually advertises with a non-zero rate. MEDITECH does this for all vital-signs profiles; Cerner only on its 0.14% outlier tail; Epic uses canonical only.</li>
</ul>
"""
    page = render_page(
        title="Cross-vendor production fleet support",
        body=body,
        breadcrumbs=[("Map", "./index.html"), ("Cross-vendor fleet", "cross-vendor-fleet.html")],
    )
    out_root.mkdir(parents=True, exist_ok=True)
    (out_root / "cross-vendor-fleet.html").write_text(page)
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", nargs="?")
    ap.add_argument("--all", action="store_true")
    today = datetime.date.today()
    quarter = f"{today.year}-q{(today.month - 1) // 3 + 1}"
    ap.add_argument("--out", default=f"reports/{quarter}/html")
    args = ap.parse_args()

    out_root = REPO_ROOT / args.out if not Path(args.out).is_absolute() else Path(args.out)

    targets = []
    if args.all:
        for d in sorted((REPO_ROOT / "ehrs").iterdir()):
            if (d / "overlay.json").exists():
                targets.append(d.name)
    elif args.ehr:
        targets = [args.ehr]
    else:
        ap.error("pass an EHR identifier or --all")

    rendered_count = 0
    fleet_pages = 0
    for ehr in targets:
        ehr_view = conformance_matrix(ehr)
        captured_date = ehr_view.get("captured_date")
        out_dir = out_root / ehr
        render_ehr_index(ehr, ehr_view, out_dir)
        for profile in ehr_view.get("profiles", []):
            if not profile.get("ms_total"):
                continue
            render_profile_index(ehr, profile, out_dir, captured_date=captured_date)
            for element in profile.get("elements", []):
                render_element_page(ehr, profile, element, out_dir, captured_date=captured_date)
                rendered_count += 1
        fleet = ehr_view.get("production_fleet")
        if fleet:
            fleet_dir = out_dir / "fleet"
            render_fleet_index(ehr, fleet, fleet_dir)
            fleet_pages += 1
            for cluster in fleet.get("capstmt_shape_clusters", []):
                render_fleet_cluster_page(ehr, cluster, fleet_dir / "clusters")
                fleet_pages += 1
    has_xv = render_cross_vendor_fleet(out_root)
    render_root_index(targets, out_root, has_cross_vendor_fleet=has_xv)

    print(f"rendered {rendered_count} element pages + {fleet_pages} fleet pages across {len(targets)} EHR(s) → {out_root.relative_to(REPO_ROOT)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
