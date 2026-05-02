"""Synthesize the Map view at render-time directly from CapabilityStatement + overlay.

NO INTERMEDIATE SCHEMA. The Map's two source-of-truth files per EHR are:
  - ehrs/{ehr}/CapabilityStatement.json  (verbatim from sandbox; never edited)
  - ehrs/{ehr}/overlay.json              (only what CapStmt cannot tell us)

This module produces the Map data that downstream consumers want — the per-EHR
× per-profile × per-element view used by the SEO renderer, the /compare tool,
and any external integration. Consumers call `synthesize_ehr(ehr)` or
`synthesize_all()` and get a Python dict ready to render to HTML/JSON/CSV.

Usage:
    python -m tools.synthesize epic              # print synthesized view
    python -m tools.synthesize epic --resource Patient
    python -m tools.synthesize --all --json > /tmp/map.json
"""
import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
US_CORE_BASELINE_PATH = REPO_ROOT / "us-core" / "us-core-6.1-baseline.json"

# US Core profile URL → profile_id (stable identifier used in overlays)
def _profile_id_from_url(url: str) -> str:
    base = url.split("|")[0]
    return base.rsplit("/", 1)[-1] if "/" in base else base


def load_ehr_sources(ehr: str) -> tuple[dict, dict]:
    """Return (CapabilityStatement, overlay) for an EHR. Both required."""
    ehr_dir = EHRS_DIR / ehr
    cs_path = ehr_dir / "CapabilityStatement.json"
    ov_path = ehr_dir / "overlay.json"
    if not cs_path.exists():
        raise FileNotFoundError(f"missing {cs_path} — run `python -m tools.fetch_capability {ehr}` first")
    if not ov_path.exists():
        raise FileNotFoundError(f"missing {ov_path}")
    return json.loads(cs_path.read_text()), json.loads(ov_path.read_text())


def load_production_fleet(ehr: str) -> dict | None:
    """Return the production_fleet.json snapshot for an EHR, or None if not yet
    harvested. Optional input — synthesis falls back gracefully when missing."""
    p = EHRS_DIR / ehr / "production_fleet.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def load_us_core_baseline() -> dict:
    return json.loads(US_CORE_BASELINE_PATH.read_text())


def synthesize_ehr(ehr: str) -> dict:
    """Build the Map view for one EHR by joining CapabilityStatement + overlay
    (+ optional production_fleet snapshot when available)."""
    capstmt, overlay = load_ehr_sources(ehr)
    baseline = load_us_core_baseline()
    fleet = load_production_fleet(ehr)
    fleet_support_by_pid: dict[str, dict] = {}
    if fleet:
        for entry in fleet.get("us_core_profile_support_rate", []):
            fleet_support_by_pid[entry["profile_id"]] = entry

    sw = capstmt.get("software", {})
    rest = capstmt.get("rest", [{}])[0]
    resources = rest.get("resource", [])

    # Index US Core baseline profiles
    baseline_profiles = {p["profile_id"]: p for p in baseline.get("profiles", [])}

    # Build per-profile view from supportedProfile declarations
    profiles_view: dict[str, dict] = {}
    for r in resources:
        resource_type = r.get("type")
        for sp_url in r.get("supportedProfile", []):
            pid = _profile_id_from_url(sp_url)
            baseline_entry = baseline_profiles.get(pid)
            fleet_entry = fleet_support_by_pid.get(pid)
            profiles_view.setdefault(pid, {
                "profile_id": pid,
                "profile_url": sp_url.split("|")[0],
                "profile_version": sp_url.split("|")[1] if "|" in sp_url else None,
                "resource_type": resource_type,
                "ehr_declares_support": True,
                "interactions_supported": [i.get("code") for i in r.get("interaction", [])],
                "search_params": [
                    {
                        "name": p.get("name"),
                        "type": p.get("type"),
                        "documentation": p.get("documentation", "").strip(),
                    }
                    for p in r.get("searchParam", [])
                ],
                "us_core_must_support": baseline_entry.get("must_support", []) if baseline_entry else [],
                "us_core_supported_searches": baseline_entry.get("supported_searches", []) if baseline_entry else [],
                "us_core_priority": baseline_entry.get("priority") if baseline_entry else None,
                "element_deviations": [],
                "fleet_support_rate": fleet_entry.get("fraction") if fleet_entry else None,
                "fleet_customers_advertising": fleet_entry.get("customers_advertising") if fleet_entry else None,
                "fleet_customers_with_capstmt": fleet_entry.get("customers_with_capstmt") if fleet_entry else None,
                "fleet_absent_in_modal_cluster": fleet_entry.get("absent_in_modal_cluster") if fleet_entry else None,
                "verification": {
                    "source_url": overlay["capability_statement_url"],
                    "source_quote": f"CapabilityStatement.rest[0].resource[type={resource_type}].supportedProfile contains {sp_url}",
                    "verified_via": _capability_verified_via(ehr),
                    "verified_query": f"GET {overlay['capability_statement_url']} Accept: application/fhir+json",
                    "verified_date": overlay["capability_statement_fetched_date"],
                },
            })

    # Apply overlay element_deviations onto matching profile entries
    for dev in overlay.get("element_deviations", []):
        pid = dev["profile_id"]
        if pid in profiles_view:
            profiles_view[pid]["element_deviations"].append(dev)
        else:
            # Overlay declares deviation for a profile EHR doesn't claim — record it as orphan-deviation
            profiles_view.setdefault(pid, {
                "profile_id": pid,
                "ehr_declares_support": False,
                "orphan_deviations": [],
            })["element_deviations"] = profiles_view[pid].get("element_deviations", []) + [dev]

    # Per-resource search params declared in CapStmt (separate from profile-bound view)
    resource_searches: dict[str, dict] = {}
    for r in resources:
        rt = r.get("type")
        if not rt:
            continue
        ov_search = overlay.get("search_param_observations", {}).get(rt, {})
        resource_searches[rt] = {
            "interactions": [i.get("code") for i in r.get("interaction", [])],
            "search_params": [
                {
                    "name": p.get("name"),
                    "type": p.get("type"),
                    "documentation": p.get("documentation", "").strip(),
                }
                for p in r.get("searchParam", [])
            ],
            "supported_includes": r.get("searchInclude", []),
            "supported_revincludes": r.get("searchRevInclude", []),
            "supported_profiles": [_profile_id_from_url(sp) for sp in r.get("supportedProfile", [])],
            "params_silently_ignored": ov_search.get("params_silently_ignored", []),
            "params_with_value_restrictions": ov_search.get("params_with_value_restrictions", []),
            "verification": {
                "source_url": overlay["capability_statement_url"],
                "source_quote": f"CapabilityStatement.rest[0].resource[type={rt}]",
                "verified_via": _capability_verified_via(ehr),
                "verified_query": f"GET {overlay['capability_statement_url']} Accept: application/fhir+json",
                "verified_date": overlay["capability_statement_fetched_date"],
            },
        }

    # Auth view = CapStmt security + overlay
    security = rest.get("security", {})
    smart_uris = {}
    for ext in security.get("extension", []):
        if ext.get("url", "").endswith("oauth-uris"):
            for sub in ext.get("extension", []):
                if "url" in sub and "valueUri" in sub:
                    smart_uris[sub["url"]] = sub["valueUri"]
    auth_view = {
        "services_declared": [
            (c.get("code") or s.get("text"))
            for s in security.get("service", [])
            for c in (s.get("coding") or [{}])
        ],
        "cors_supported": security.get("cors"),
        "smart_oauth_uris": smart_uris,
        "overlay": overlay.get("auth_overlay"),
        "verification": {
            "source_url": overlay["capability_statement_url"],
            "source_quote": "CapabilityStatement.rest[0].security",
            "verified_via": _capability_verified_via(ehr),
            "verified_date": overlay["capability_statement_fetched_date"],
        },
    }

    return {
        "ehr": overlay["ehr"],
        "ehr_display_name": overlay["ehr_display_name"],
        "ehr_version_validated": overlay.get("ehr_version_validated") or _version_from_capstmt(capstmt),
        "capability_statement_fetched_date": overlay["capability_statement_fetched_date"],
        "capability_statement_url": overlay["capability_statement_url"],
        "compatibility_statement": overlay["compatibility_statement"],
        "fhir_version": capstmt.get("fhirVersion"),
        "capability_statement_software": sw,
        "capability_statement_status": capstmt.get("status"),
        "capability_statement_date": capstmt.get("date"),
        "auth": auth_view,
        "operation_outcome": overlay.get("operation_outcome_overlay"),
        "pagination": overlay.get("pagination_overlay"),
        "profiles": list(profiles_view.values()),
        "resources": resource_searches,
        "production_fleet": fleet,
    }


def _version_from_capstmt(c: dict) -> str:
    sw = c.get("software", {})
    if sw.get("name") and sw.get("version"):
        return f"{sw['name']} {sw['version']}"
    return c.get("name") or "unknown"


def _capability_verified_via(ehr: str) -> str:
    return f"{ehr}_public_sandbox"


def synthesize_all() -> dict:
    out = {}
    for ehr_dir in sorted(EHRS_DIR.iterdir()):
        if not ehr_dir.is_dir():
            continue
        try:
            out[ehr_dir.name] = synthesize_ehr(ehr_dir.name)
        except FileNotFoundError as e:
            out[ehr_dir.name] = {"error": str(e)}
    return out


def conformance_matrix(ehr: str) -> dict:
    """Per-profile, per-element conformance matrix joining baseline must_support
    with the overlay's element_deviations. Each cell carries category + sample evidence
    + multi-patient evidence. Output is the canonical form for the Map's HTML and
    JSON renderers; one row per (profile_id × must_support_path)."""
    capstmt, overlay = load_ehr_sources(ehr)
    baseline = load_us_core_baseline()
    fleet = load_production_fleet(ehr)
    fleet_support_by_pid: dict[str, dict] = {}
    if fleet:
        for entry in fleet.get("us_core_profile_support_rate", []):
            fleet_support_by_pid[entry["profile_id"]] = entry
    baseline_profiles = {p["profile_id"]: p for p in baseline.get("profiles", [])}

    declared_profile_ids: set[str] = set()
    rest = capstmt.get("rest", [{}])[0]
    for r in rest.get("resource", []):
        for sp in r.get("supportedProfile", []):
            declared_profile_ids.add(_profile_id_from_url(sp))

    deviations_by_pp: dict[tuple[str, str], list[dict]] = {}
    for d in overlay.get("element_deviations", []) or []:
        deviations_by_pp.setdefault((d["profile_id"], d["path"]), []).append(d)

    profiles_out: list[dict] = []
    summary_counts: dict[str, int] = {}
    for pid, baseline_profile in baseline_profiles.items():
        if not baseline_profile.get("must_support"):
            continue
        elements: list[dict] = []
        for ms in baseline_profile["must_support"]:
            cell_devs = deviations_by_pp.get((pid, ms["path"]), [])
            categories = sorted({d["deviation_category"] for d in cell_devs})
            cell = {
                "path": ms["path"],
                "cardinality": ms.get("cardinality"),
                "type": ms.get("type"),
                "binding": ms.get("binding"),
                "must_support": ms.get("must_support"),
                "uscdi_requirement": ms.get("uscdi_requirement"),
                "deviation_categories": categories,
                "deviation_count": len(cell_devs),
                "sample_observed": [
                    {"category": d["deviation_category"], "observed_in_ehr": d.get("observed_in_ehr", ""),
                     "multi_patient_evidence": d.get("multi_patient_evidence", {})}
                    for d in cell_devs
                ],
            }
            elements.append(cell)
            for c in categories or ["unmeasured"]:
                summary_counts[c] = summary_counts.get(c, 0) + 1
        fleet_entry = fleet_support_by_pid.get(pid)
        profiles_out.append({
            "profile_id": pid,
            "resource_type": baseline_profile.get("resource_type"),
            "priority": baseline_profile.get("priority"),
            "ehr_declares_support": pid in declared_profile_ids,
            "elements": elements,
            "ms_total": len(elements),
            "elements_measured": sum(1 for e in elements if e["deviation_count"] > 0),
            "fleet_support_rate": fleet_entry.get("fraction") if fleet_entry else None,
            "fleet_customers_advertising": fleet_entry.get("customers_advertising") if fleet_entry else None,
            "fleet_customers_with_capstmt": fleet_entry.get("customers_with_capstmt") if fleet_entry else None,
            "fleet_absent_in_modal_cluster": fleet_entry.get("absent_in_modal_cluster") if fleet_entry else None,
        })

    return {
        "ehr": ehr,
        "captured_date": overlay.get("capability_statement_fetched_date"),
        "baseline_version": baseline.get("version"),
        "category_summary": summary_counts,
        "profiles": profiles_out,
        "production_fleet": fleet,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", nargs="?")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--resource", help="Filter to one resource type (Patient, Observation, ...)")
    ap.add_argument("--json", action="store_true", help="Output JSON instead of human-readable")
    ap.add_argument("--matrix", action="store_true", help="Output the conformance matrix instead of the full synthesis")
    ap.add_argument("--write-matrix", help="Write the conformance matrix to a file under reports/{quarter}/.")
    args = ap.parse_args()

    if args.all:
        data = synthesize_all()
    elif args.ehr:
        if args.matrix or args.write_matrix:
            data = conformance_matrix(args.ehr)
        else:
            data = synthesize_ehr(args.ehr)
            if args.resource:
                data = {**data, "resources": {args.resource: data["resources"].get(args.resource)},
                        "profiles": [p for p in data["profiles"] if p.get("resource_type") == args.resource]}
    else:
        ap.error("pass an EHR identifier or --all")

    if args.write_matrix:
        out_path = Path(args.write_matrix)
        if not out_path.is_absolute():
            out_path = REPO_ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(data, indent=2) + "\n")
        print(f"wrote {out_path.relative_to(REPO_ROOT)}")
        if not args.json:
            return 0

    if args.json or args.matrix:
        print(json.dumps(data, indent=2))
    else:
        _pretty_print(data)
    return 0


def _pretty_print(data: dict):
    if "ehr" in data:
        _print_one(data)
    else:
        for ehr, view in data.items():
            print(f"\n========== {ehr} ==========")
            if "error" in view:
                print(f"  ERROR: {view['error']}")
            else:
                _print_one(view)


def _print_one(view: dict):
    print(f"{view.get('ehr_display_name', view.get('ehr'))} — {view.get('ehr_version_validated')}")
    print(f"  FHIR {view.get('fhir_version')}, fetched {view.get('capability_statement_fetched_date')}")
    print(f"  CapStmt: {view.get('capability_statement_url')}")
    print(f"  Auth services: {view['auth']['services_declared']}")
    if view['auth']['smart_oauth_uris']:
        for k, v in view['auth']['smart_oauth_uris'].items():
            print(f"    {k}: {v}")
    print(f"  CORS: {view['auth']['cors_supported']}")
    print(f"\n  {len(view['profiles'])} profiles claimed supported:")
    for p in view['profiles']:
        ms = len(p.get("us_core_must_support", []))
        ed = len(p.get("element_deviations", []))
        print(f"    {p['profile_id']}  ({p.get('resource_type')}, {p.get('us_core_priority') or '?'}) — {ms} MUST-SUPPORT in baseline, {ed} overlay deviations, interactions={p.get('interactions_supported')}")
    print(f"\n  {len(view['resources'])} resource types in CapabilityStatement")


if __name__ == "__main__":
    sys.exit(main())
