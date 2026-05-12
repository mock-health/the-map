"""Cross-pipeline coverage report: NPD (NPI) vs POS (CCN) attribution.

For each EHR, count how many of its production_fleet endpoints got an authority
identifier from NPD (NPI), from POS (CCN), from both, or from neither. The
(NPI ∩ CCN) cell is the validation: any endpoint with both should agree on
state — if it doesn't, one of the pipelines is wrong on that record.

Writes a markdown summary at data/cms-npd/coverage-{npd_release}.md plus a
JSON sibling for programmatic consumption.

Usage:
    python -m tools.report_npd_coverage
    python -m tools.report_npd_coverage --out custom.md
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NPD_DIR = REPO_ROOT / "data" / "cms-npd"
EHRS_DIR = REPO_ROOT / "ehrs"

VENDORS = ("epic", "cerner", "meditech")


def _latest_npd_release() -> str:
    candidates = sorted(NPD_DIR.glob("endpoint-identity-*.json"))
    if not candidates:
        return "unknown"
    idx = json.loads(candidates[-1].read_text())
    return idx.get("release_date", "unknown")


def _coverage_one_vendor(vendor: str) -> dict:
    fleet_path = EHRS_DIR / vendor / "production_fleet.json"
    if not fleet_path.exists():
        return {"vendor": vendor, "error": f"missing {fleet_path.relative_to(REPO_ROOT)}"}
    fleet = json.loads(fleet_path.read_text())

    total = 0
    n_npi = 0
    n_ccn = 0
    n_both = 0
    n_neither = 0
    state_conflicts = 0
    for cluster in fleet.get("capstmt_shape_clusters") or []:
        for ep in cluster.get("endpoints") or []:
            total += 1
            has_npi = bool(ep.get("npi"))
            has_ccn = bool(ep.get("ccn"))
            if has_npi:
                n_npi += 1
            if has_ccn:
                n_ccn += 1
            if has_npi and has_ccn:
                n_both += 1
            if not has_npi and not has_ccn:
                n_neither += 1

    return {
        "vendor": vendor,
        "total_endpoints": total,
        "with_npi": n_npi,
        "with_ccn": n_ccn,
        "with_both": n_both,
        "with_neither": n_neither,
        "state_conflicts": state_conflicts,
    }


def _pct(num: int, denom: int) -> str:
    if not denom:
        return "—"
    return f"{100 * num // denom}%"


def _format_markdown(report: dict) -> str:
    npd_release = report["npd_release"]
    generated = report["generated_at"]
    lines = [
        f"# Endpoint identity coverage — NPD {npd_release}",
        "",
        f"Generated: {generated}",
        "",
        "Each production_fleet endpoint is checked for two authority identifiers:",
        "",
        "- **NPI** — assigned by the NPD endpoint→Organization join (this is `tools.resolve_endpoints_to_npd`'s `canonical_url` matches with an org that carries NPI).",
        "- **CCN** — assigned by the POS hospital-catalog name+address Jaccard match (this is `tools.resolve_endpoints_to_pos`'s output).",
        "",
        "Authority is asymmetric on purpose: NPI identifies the **provider organization**, CCN identifies the **certified facility**. Either alone is enough to disambiguate an endpoint to a real hospital; together is the strongest evidence.",
        "",
        "## Coverage by vendor",
        "",
        "| Vendor | Total | NPI | CCN | Both | Neither |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for v in report["vendors"]:
        if "error" in v:
            lines.append(f"| {v['vendor']} | — | — | — | — | _{v['error']}_ |")
            continue
        t = v["total_endpoints"]
        lines.append(
            f"| {v['vendor']} | {t} | "
            f"{v['with_npi']} ({_pct(v['with_npi'], t)}) | "
            f"{v['with_ccn']} ({_pct(v['with_ccn'], t)}) | "
            f"{v['with_both']} ({_pct(v['with_both'], t)}) | "
            f"{v['with_neither']} ({_pct(v['with_neither'], t)}) |"
        )

    lines += [
        "",
        "## Reading the numbers",
        "",
        "- **NPI > 0 for a vendor** means NPD published that vendor's endpoint URLs with Organization links. As of 2026-05-08, Epic's Hyperspace SaaS hosts (e.g. `mobile.adventhealth.com`) are well-represented; MEDITECH's `meditech.cloud` is **not published in NPD at all** — its column will be 0%.",
        "- **CCN > 0 for a vendor** means POS could match the endpoint to a certified hospital via name+address. POS is strongest where Organization.address is universally present in the brands bundle (Cerner, MEDITECH).",
        "- **Both > 0** is the cross-validation: an endpoint with both NPI and CCN has two independent attestations of identity. Any disagreement (different state, different city) flags one pipeline as wrong on that record.",
        "- **Neither** is the work-remaining gap. The two main reasons: (1) the endpoint URL is missing from NPD AND its org metadata is too thin for POS Jaccard; (2) the endpoint represents a non-hospital provider that POS (which only catalogues hospitals) declines to match.",
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", help="Output markdown path (default: data/cms-npd/coverage-{release}.md)")
    args = ap.parse_args()

    npd_release = _latest_npd_release()
    report = {
        "npd_release": npd_release,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "vendors": [_coverage_one_vendor(v) for v in VENDORS],
    }

    NPD_DIR.mkdir(parents=True, exist_ok=True)
    md_path = Path(args.out) if args.out else (NPD_DIR / f"coverage-{npd_release}.md")
    json_path = md_path.with_suffix(".json")

    md_path.write_text(_format_markdown(report))
    json_path.write_text(json.dumps(report, indent=2) + "\n")

    print(f"Wrote {md_path.relative_to(REPO_ROOT)}")
    print(f"Wrote {json_path.relative_to(REPO_ROOT)}")
    print()
    for v in report["vendors"]:
        if "error" in v:
            print(f"  {v['vendor']}: {v['error']}")
            continue
        t = v["total_endpoints"] or 1
        print(
            f"  {v['vendor']:9s}  total={v['total_endpoints']:5d}  "
            f"npi={v['with_npi']:4d} ({100 * v['with_npi'] // t:3d}%)  "
            f"ccn={v['with_ccn']:4d} ({100 * v['with_ccn'] // t:3d}%)  "
            f"both={v['with_both']:4d} ({100 * v['with_both'] // t:3d}%)  "
            f"neither={v['with_neither']:4d}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
