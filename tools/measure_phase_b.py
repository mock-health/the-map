"""Phase B sweep — multi-patient + four-axis conformance against an EHR sandbox.

Per PLAN_EPIC.md this is the v1 successor to the v0.1 single-patient presence-only
sweep. It walks every P0 (and configurable P1) probe across the patient catalog
produced by `tools.enumerate_sandbox_patients`, runs each captured response through
the four-axis conformance analyzer (presence, cardinality, value-set, format), and
writes both per-(resource,patient) golden fixtures and an updated overlay.

Outputs:
  - tests/golden/{ehr}/phase-b-{date}/{slug}-{patient}.json   raw verbatim responses
  - tests/golden/{ehr}/phase-b-{date}/sweep-summary.json      overall numbers
  - ehrs/{ehr}/overlay.json                                   element_deviations,
      multi_patient_coverage, operation_outcome_overlay.examples, pagination_overlay
      (the 5 v0.1 phase_b_findings headlines are preserved unchanged)

Usage:
    python -m tools.measure_phase_b epic
    python -m tools.measure_phase_b epic --probes=Patient,Observation-lab   # subset
    python -m tools.measure_phase_b epic --dry-run         # don't write overlay
    python -m tools.measure_phase_b epic --max-pages=2     # cap pagination walk

The sweep token is acquired once at start; multi-patient runs are short enough that
single-token covers all probes (Epic tokens are 1h validity).
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import requests

from tools._env import load_env
from tools.conformance import ValueSetIndex, analyze
from tools.oauth_handshake import EHR_CONFIG, get_access_token
from tools.row_id import ensure_row_id

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"
US_CORE_BASELINE = REPO_ROOT / "us-core" / "us-core-6.1-baseline.json"
IG_PACKAGE_DIR = REPO_ROOT / "us-core" / "ig-package" / "package"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) Phase-B-sweep"

# Probe slug → (HTTP path template, list of US Core profile IDs to evaluate against)
P0_PROBES: dict[str, dict] = {
    "Patient": {
        "method": "GET",
        "path_template": "Patient/{patient_id}",
        "us_core_profiles": ["us-core-patient"],
    },
    "AllergyIntolerance": {
        "method": "GET",
        "path_template": "AllergyIntolerance?patient={patient_id}",
        "us_core_profiles": ["us-core-allergyintolerance"],
    },
    "Condition-problems": {
        "method": "GET",
        "path_template": "Condition?patient={patient_id}&category=problem-list-item",
        "us_core_profiles": ["us-core-condition-problems-health-concerns"],
    },
    "Condition-encounter-diag": {
        "method": "GET",
        "path_template": "Condition?patient={patient_id}&category=encounter-diagnosis",
        "us_core_profiles": ["us-core-condition-encounter-diagnosis"],
    },
    "Condition-health-concern": {
        "method": "GET",
        "path_template": "Condition?patient={patient_id}&category=health-concern",
        "us_core_profiles": ["us-core-condition-problems-health-concerns"],
    },
    "Observation-lab": {
        "method": "GET",
        "path_template": "Observation?patient={patient_id}&category=laboratory",
        "us_core_profiles": ["us-core-observation-lab"],
    },
    "Observation-vital-signs": {
        "method": "GET",
        "path_template": "Observation?patient={patient_id}&category=vital-signs",
        "us_core_profiles": ["us-core-vital-signs"],
    },
    "MedicationRequest": {
        "method": "GET",
        "path_template": "MedicationRequest?patient={patient_id}",
        "us_core_profiles": ["us-core-medicationrequest"],
    },
    "Encounter": {
        "method": "GET",
        "path_template": "Encounter?patient={patient_id}",
        "us_core_profiles": ["us-core-encounter"],
    },
    "Procedure": {
        "method": "GET",
        "path_template": "Procedure?patient={patient_id}",
        "us_core_profiles": ["us-core-procedure"],
    },
    "DiagnosticReport-lab": {
        "method": "GET",
        "path_template": "DiagnosticReport?patient={patient_id}&category=LAB",
        "us_core_profiles": ["us-core-diagnosticreport-lab"],
    },
    "Immunization": {
        "method": "GET",
        "path_template": "Immunization?patient={patient_id}",
        "us_core_profiles": ["us-core-immunization"],
    },
    "DocumentReference": {
        "method": "GET",
        "path_template": "DocumentReference?patient={patient_id}",
        "us_core_profiles": ["us-core-documentreference"],
    },
}

ERROR_PROBES = [
    {
        "label": "404 on nonexistent Patient ID",
        "method": "GET",
        "path": "Patient/this-id-definitely-does-not-exist-{rand}",
        "expected_status_class": "4xx",
    },
    {
        "label": "search with unsupported param",
        "method": "GET",
        "path": "Patient?totally-bogus-param=foo",
        "expected_status_class": "4xx-or-200-with-warning",
    },
    {
        "label": "request unsupported resource type",
        "method": "GET",
        "path": "FakeResourceTypeThatDoesNotExist?_count=1",
        "expected_status_class": "4xx",
    },
]


def fhir_get(base: str, path: str, token: str | None) -> requests.Response:
    headers = {"Accept": "application/fhir+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.get(f"{base.rstrip('/')}/{path.lstrip('/')}", headers=headers, timeout=60)


def real_entries(bundle: dict) -> list[dict]:
    """Return Bundle.entry items that aren't OperationOutcome `mode=outcome` placeholders."""
    out = []
    for e in bundle.get("entry", []) or []:
        if (e.get("search") or {}).get("mode") == "outcome":
            continue
        res = e.get("resource") or {}
        if res.get("resourceType") == "OperationOutcome":
            continue
        out.append(e)
    return out


def first_resource_of_type(bundle_or_resource: dict, resource_type: str) -> dict | None:
    if bundle_or_resource.get("resourceType") == resource_type:
        return bundle_or_resource
    if bundle_or_resource.get("resourceType") != "Bundle":
        return None
    for entry in real_entries(bundle_or_resource):
        r = entry.get("resource", {})
        if r.get("resourceType") == resource_type:
            return r
    return None


def all_resources_of_type(bundle_or_resource: dict, resource_type: str) -> list[dict]:
    if bundle_or_resource.get("resourceType") == resource_type:
        return [bundle_or_resource]
    if bundle_or_resource.get("resourceType") != "Bundle":
        return []
    return [
        e["resource"]
        for e in real_entries(bundle_or_resource)
        if e.get("resource", {}).get("resourceType") == resource_type
    ]


def evaluate_one(*, body: dict, profile_id: str, baseline: dict, ehr: str, vs_index: ValueSetIndex, today: str, patient_id: str) -> tuple[list[dict], dict]:
    """Run conformance analysis on every applicable resource in `body` against the named profile.

    Presence is checked across ALL resources of the type in the bundle: a path is "present"
    for this patient if at least one resource has it. Other axes (cardinality, value_set,
    format) are run against the first resource that has the path, since those are only
    meaningful when the path resolves.
    """
    from tools.conformance.presence import evaluate_path

    profile = next((p for p in baseline["profiles"] if p["profile_id"] == profile_id), None)
    if not profile or not profile.get("must_support"):
        return [], {"reason": "no must_support in baseline"}

    rtype = profile["resource_type"]
    targets = all_resources_of_type(body, rtype)
    diagnostics = {
        "resource_type": rtype,
        "patient_id": patient_id,
        "evaluated_resource_count": len(targets),
    }

    if not targets:
        return [{
            "profile_id": profile_id,
            "path": rtype,
            "deviation_category": "missing",
            "expected_per_us_core": f"resource of type {rtype} present in response",
            "observed_in_ehr": f"no {rtype} entry returned for patient {patient_id}",
            "deviation": (
                f"Bundle returned no {rtype} entries to evaluate for patient {patient_id}. "
                "May be vendor-implementation gap or patient-data gap; consult multi_patient_coverage."
            ),
            "patient_id": patient_id,
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"Bundle.entry[].resource.resourceType did not include {rtype}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }], diagnostics

    findings: list[dict] = []
    for ms in profile["must_support"]:
        # Find the first resource that has this path; presence-anywhere semantics
        first_with_path = next(
            (r for r in targets if evaluate_path(r, ms["path"])[0]),
            None,
        )
        anchor = first_with_path if first_with_path is not None else targets[0]
        rows = analyze(
            resource=anchor,
            must_support=ms,
            profile_id=profile_id,
            ehr=ehr,
            value_set_index=vs_index,
            today=today,
        )
        for row in rows:
            row["patient_id"] = patient_id
            findings.append(row)
    return findings, diagnostics


def aggregate_multi_patient(per_patient: list[dict]) -> tuple[list[dict], list[dict]]:
    """Reshape per-(patient,profile,path) findings into a deduped overlay-shaped list,
    plus a `multi_patient_coverage` summary that distinguishes vendor-gap vs patient-gap.

    For each (profile_id, path):
      - The presence axis (matches/missing) gets one summary row that captures the
        multi-patient distribution: vendor-implementation-gap, patient-data-gap, or
        matches-everywhere.
      - Every other deviation_category (cardinality-*, value-set-*, format-*) emits its
        own row with the patient list, independent of the presence summary.
    """
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for f in per_patient:
        by_key[(f["profile_id"], f["path"])].append(f)

    deduped: list[dict] = []
    coverage: list[dict] = []
    for (pid, path), entries in by_key.items():
        cats = Counter(e["deviation_category"] for e in entries)
        patient_ids_by_cat: dict[str, list[str]] = defaultdict(list)
        for e in entries:
            patient_ids_by_cat[e["deviation_category"]].append(e.get("patient_id", "?"))
        seen_patients = sorted({e.get("patient_id", "?") for e in entries})
        n = len(seen_patients)

        # 1) Presence row — always emit one (matches/missing dichotomy)
        m_match = len(set(patient_ids_by_cat.get("matches", [])))
        m_missing = len(set(patient_ids_by_cat.get("missing", [])))
        if m_match + m_missing > 0:
            if m_missing == n and m_match == 0:
                row = dict(next(e for e in entries if e["deviation_category"] == "missing"))
                row.pop("patient_id", None)
                row["observed_in_ehr"] = f"absent across all {n} swept patients ({', '.join(seen_patients)})"
                row["deviation"] = (
                    f"Element {path} not present in any of the {n} swept patient responses. "
                    "Recorded as vendor-implementation gap."
                )
                row["multi_patient_evidence"] = {
                    "category": "vendor-implementation-gap",
                    "patients_swept": seen_patients,
                    "patients_present_in": [],
                    "patients_absent_in": seen_patients,
                }
                deduped.append(row)
            elif m_missing == 0:
                row = dict(next(e for e in entries if e["deviation_category"] == "matches"))
                row.pop("patient_id", None)
                row["observed_in_ehr"] = f"present in all {n} swept patients"
                row["multi_patient_evidence"] = {
                    "category": "matches-everywhere",
                    "patients_swept": seen_patients,
                    "patients_present_in": seen_patients,
                    "patients_absent_in": [],
                }
                deduped.append(row)
            else:
                present_pts = sorted(set(patient_ids_by_cat.get("matches", [])))
                absent_pts = sorted(set(patient_ids_by_cat.get("missing", [])))
                row = dict(next(e for e in entries if e["deviation_category"] == "matches"))
                row.pop("patient_id", None)
                row["deviation_category"] = "matches"
                row["observed_in_ehr"] = f"present in {m_match}/{n} patients; absent in {m_missing}/{n}"
                row["deviation"] = (
                    f"Element {path} present in {m_match}/{n} swept patients ({', '.join(present_pts)}); "
                    f"absent in {m_missing}/{n} ({', '.join(absent_pts)}). "
                    "Recorded as patient-data gap, not vendor-implementation gap."
                )
                row["multi_patient_evidence"] = {
                    "category": "patient-data-gap",
                    "patients_swept": seen_patients,
                    "patients_present_in": present_pts,
                    "patients_absent_in": absent_pts,
                }
                deduped.append(row)

        # 2) Non-presence axes — one row per distinct category (value-set-*, cardinality-*, format-*)
        for cat, patient_list in patient_ids_by_cat.items():
            if cat in ("matches", "missing"):
                continue
            exemplar = next(e for e in entries if e["deviation_category"] == cat)
            row = dict(exemplar)
            row.pop("patient_id", None)
            uniq_patients = sorted(set(patient_list))
            row["multi_patient_evidence"] = {
                "category": cat,
                "patients_swept": seen_patients,
                "patients_with_this_category": uniq_patients,
            }
            base_dev = row.get("deviation", "") or ""
            patient_count = len(uniq_patients)
            row["deviation"] = (
                base_dev + f" Observed in {patient_count}/{n} patient(s): {', '.join(uniq_patients)}."
            ).strip()
            deduped.append(row)

        coverage.append({
            "profile_id": pid,
            "path": path,
            "patients_swept": seen_patients,
            "category_counts": dict(cats),
        })

    return deduped, coverage


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--probes", help="comma-separated probe slugs (default: all P0)")
    ap.add_argument("--max-pages", type=int, default=3, help="pagination walk depth")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env(strict=True)

    cfg = EHR_CONFIG[args.ehr]
    today = datetime.date.today().isoformat()
    base = os.environ[cfg.get("phase_b_base_var") or cfg["fhir_base_var"]]
    no_auth = bool(cfg.get("phase_b_no_auth"))
    ua = USER_AGENT

    selection_path = EHRS_DIR / args.ehr / "p0_patient_selection.json"
    if not selection_path.exists():
        sys.exit(f"missing {selection_path} — run `python -m tools.enumerate_sandbox_patients {args.ehr}` first")
    selection = json.loads(selection_path.read_text())

    probes_filter = set(args.probes.split(",")) if args.probes else None
    probes_to_run = {k: v for k, v in P0_PROBES.items() if not probes_filter or k in probes_filter}

    print(f"== Phase B sweep against {args.ehr} ==")
    print(f"  base:    {base}")
    print(f"  date:    {today}")
    print(f"  probes:  {len(probes_to_run)} of {len(P0_PROBES)} P0")
    print()

    if no_auth:
        token = None
        print("step 1/4 — auth: skipped (phase_b_no_auth=True; using open tier)")
    else:
        print("step 1/4 — get access token (via tools.auth_flows)")
        token, _ = get_access_token(args.ehr)
        print(f"  OK ({len(token)} char token)")

    golden_root = GOLDEN_DIR / args.ehr / f"phase-b-{today}"
    golden_root.mkdir(parents=True, exist_ok=True)

    baseline = json.loads(US_CORE_BASELINE.read_text())
    vs_index = ValueSetIndex(IG_PACKAGE_DIR)

    # Step 2: per-(probe, patient) sweep
    print()
    print(f"step 2/4 — sweep {len(probes_to_run)} probes × selected patients")
    per_patient_findings: list[dict] = []
    pagination_target: tuple[str, dict, int] | None = None  # (probe_slug, first_page_body, count)
    coverage_by_probe: dict[str, list[dict]] = {}

    for probe_slug, probe in probes_to_run.items():
        sel = (selection.get("probes") or {}).get(probe_slug, {})
        patient_ids = sel.get("patient_ids") or []
        if not patient_ids:
            print(f"  SKIP {probe_slug}: no selected patients (gap finding)")
            continue
        print(f"  -- {probe_slug} (against {len(patient_ids)} patients)")
        for pid in patient_ids:
            path = probe["path_template"].format(patient_id=pid)
            r = fhir_get(base, path, token)
            try:
                body = r.json()
            except ValueError:
                body = {"_non_json_body": r.text[:2000]}
            golden_path = golden_root / f"{probe_slug.lower()}-{pid}.json"
            golden_path.write_text(json.dumps({
                "_request": {"method": probe["method"], "url": f"{base}/{path}", "status": r.status_code},
                "_captured_at": datetime.datetime.now(datetime.UTC).isoformat(),
                "_probe": probe_slug,
                "_patient_id": pid,
                "body": body,
            }, indent=2))

            # Pagination target: prefer the largest first page seen across the sweep
            if isinstance(body, dict) and body.get("resourceType") == "Bundle":
                count = len(real_entries(body))
                if count >= 1 and (pagination_target is None or count > pagination_target[2]):
                    pagination_target = (probe_slug, body, count)

            if r.ok:
                for profile_id in probe["us_core_profiles"]:
                    findings, diag = evaluate_one(
                        body=body, profile_id=profile_id, baseline=baseline,
                        ehr=args.ehr, vs_index=vs_index, today=today, patient_id=pid,
                    )
                    per_patient_findings.extend(findings)
                    coverage_by_probe.setdefault(probe_slug, []).append({
                        "patient_id": pid,
                        "profile_id": profile_id,
                        "http_status": r.status_code,
                        "diag": diag,
                    })
            time.sleep(0.15)

    print()
    print(f"step 3/4 — fetch {len(ERROR_PROBES)} error-path probes")
    error_examples = []
    for ep in ERROR_PROBES:
        path = ep["path"].replace("{rand}", f"{random.randint(10**9, 10**10):x}")
        r = fhir_get(base, path, token)
        try:
            body = r.json()
        except ValueError:
            body = {"_non_json_body": r.text[:2000]}
        slug = ep["label"].lower().replace(" ", "-").replace("/", "-")
        (golden_root / f"error-{slug}.json").write_text(json.dumps({
            "_request": {"method": "GET", "url": f"{base}/{path}", "status": r.status_code},
            "_captured_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "body": body,
        }, indent=2))
        print(f"  {r.status_code}  {ep['label']}")
        error_examples.append({
            "trigger": f"GET {base}/{path}",
            "response_body": json.dumps(body)[:1500],
        })

    print()
    print("step 4/4 — pagination walk on largest Bundle")
    pagination_result = {
        "default_count": None, "max_count": None, "behavior_at_max_plus_one": "",
        "link_relations_observed": [], "next_token_format": "unknown",
        "next_token_stable_across_pages": None,
        "notes": "no multi-entry Bundle returned in P0 probes; pagination unmeasured this pass",
    }
    if pagination_target is not None:
        slug, first_page, entry_count = pagination_target
        rels = sorted({l.get("relation", "") for l in first_page.get("link", []) or []})
        pagination_result["link_relations_observed"] = [r for r in rels if r in {"self", "next", "previous", "first", "last"}]
        pagination_result["default_count"] = entry_count
        next_url = next(
            (l.get("url") for l in first_page.get("link", []) or [] if l.get("relation") == "next"),
            None,
        )
        if next_url:
            pagination_result["next_token_format"] = "parseable-query-string" if "?" in next_url else "opaque-base64"
            pages_walked = 1
            seen_next_tokens = [next_url]
            cur_url = next_url
            while cur_url and pages_walked < args.max_pages:
                page_hdrs = {"Accept": "application/fhir+json", "User-Agent": ua}
                if token:
                    page_hdrs["Authorization"] = f"Bearer {token}"
                rp = requests.get(cur_url, headers=page_hdrs, timeout=30)
                if not rp.ok:
                    break
                bp = rp.json()
                cur_url = next((l.get("url") for l in bp.get("link", []) or [] if l.get("relation") == "next"), None)
                if cur_url:
                    seen_next_tokens.append(cur_url)
                pages_walked += 1
            pagination_result["next_token_stable_across_pages"] = len(set(seen_next_tokens)) == len(seen_next_tokens)
            pagination_result["notes"] = (
                f"walked from largest bundle: {slug} (first page entries={entry_count}). walked {pages_walked} pages (max {args.max_pages})"
            )
        else:
            pagination_result["notes"] = (
                f"largest bundle: {slug} ({entry_count} first-page entries) had no rel=next link"
            )
        print(f"  walked from {slug} ({entry_count} entries first page): {pagination_result['notes']}")
    else:
        print("  skipped (no eligible Bundle)")

    # Aggregate findings across patients
    deduped_deviations, coverage_summary = aggregate_multi_patient(per_patient_findings)

    sweep_summary = {
        "ehr": args.ehr,
        "captured_date": today,
        "probes_run": list(probes_to_run.keys()),
        "patient_selection_source": str(selection_path.relative_to(REPO_ROOT)),
        "raw_findings_count": len(per_patient_findings),
        "deduped_deviations_count": len(deduped_deviations),
        "category_counts": Counter(d["deviation_category"] for d in deduped_deviations),
        "patients_per_probe": {
            slug: [c["patient_id"] for c in entries]
            for slug, entries in coverage_by_probe.items()
        },
    }
    summary_path = golden_root / "sweep-summary.json"
    summary_path.write_text(json.dumps({**sweep_summary, "category_counts": dict(sweep_summary["category_counts"])}, indent=2))

    print()
    print("== Sweep summary ==")
    print(f"  raw findings:       {sweep_summary['raw_findings_count']}")
    print(f"  deduped deviations: {sweep_summary['deduped_deviations_count']}")
    for cat, n in sorted(dict(sweep_summary['category_counts']).items()):
        print(f"    {cat:<32} {n}")
    print(f"  golden dir:        {golden_root.relative_to(REPO_ROOT)}/")

    if args.dry_run:
        print("\n(--dry-run: not writing overlay)")
        return 0

    overlay_path = EHRS_DIR / args.ehr / "overlay.json"
    overlay = json.loads(overlay_path.read_text())

    overlay["element_deviations"] = [ensure_row_id(dev) for dev in deduped_deviations]
    overlay["multi_patient_coverage"] = {
        "captured_date": today,
        "patients_swept_per_probe": sweep_summary["patients_per_probe"],
        "per_path_coverage": coverage_summary,
        "verification": {
            "source_url": str(golden_root.relative_to(REPO_ROOT)),
            "source_quote": "see per-(resource,patient) golden files for raw evidence",
            "verified_via": f"{args.ehr}_public_sandbox",
            "verified_date": today,
        },
    }
    overlay["operation_outcome_overlay"] = {
        "shape": "fhir-standard-with-vendor-extensions",
        "common_extensions": [],
        "common_codes": [],
        "examples": error_examples,
        "verification": {
            "source_url": f"{base}",
            "source_quote": f"See {golden_root.relative_to(REPO_ROOT)}/error-*.json for verbatim responses",
            "verified_via": f"{args.ehr}_public_sandbox",
            "verified_date": today,
        },
    }
    overlay["pagination_overlay"] = {
        **pagination_result,
        "verification": {
            "source_url": f"{base}",
            "source_quote": pagination_result.get("notes", ""),
            "verified_via": f"{args.ehr}_public_sandbox",
            "verified_date": today,
        },
    }
    overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    print(f"\n  updated {overlay_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
