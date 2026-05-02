"""Build the per-resource patient catalog for an EHR sandbox.

The Map's multi-patient sweep needs ≥3 patients per P0 probe. This tool walks the
sandbox Patient population, then probes each patient with `_count=1` against every
P0 resource type, recording the entry-count-per-(patient,resource) matrix. From that
matrix we pick the patients-per-probe selection.

Output:
    ehrs/{ehr}/sandbox_patients.json  — every discovered patient + their resource coverage
    ehrs/{ehr}/p0_patient_selection.json — per-probe patient list (≥3 per probe when available)

Usage:
    python -m tools.enumerate_sandbox_patients epic
    python -m tools.enumerate_sandbox_patients epic --max-patients=50  # cap walking
    python -m tools.enumerate_sandbox_patients epic --refresh           # ignore on-disk catalog
"""
import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path

import requests

from tools._env import load_env
from tools.oauth_handshake import EHR_CONFIG, get_access_token, get_access_token_with_meta

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) sandbox-patient-enumerator"

# Per-EHR known sandbox patient roster. Epic refuses unfiltered Patient search
# (returns 400 with "demographics or _id required"), so we can't auto-discover via
# pagination — we seed with documented test-patient FHIR IDs and let the prober
# verify each ID resolves to a real Patient resource.
#
# Epic IDs verified by `GET Patient/{id}` against fhir.epic.com (2026-04-26):
KNOWN_PATIENT_ROSTERS: dict[str, list[str]] = {
    "epic": [
        "eq081-VQEgP8drUUqCWzHfw3",  # Derrick Lin (canonical sandbox patient)
        "erXuFYUfucBZaryVksYEcMg3",  # Camila Maria Lopez
        "eD.LxhDyX35TntF77l7etUA3",  # Jason Argonaut (the SMART/Argonaut canonical)
        "evYS76EX6LLeCX2PlRLhvwg3",  # Jessica Argonaut
        "eAB3mDIBBcyUKviyzrxsnAw3",  # Desiree Caroline Powell
        "eIXesllypH3M9tAA5WdJftQ3",  # Linda Jane Ross
        "e0w0LEDCYtfckT6N.CkJKCw3",  # Warren James McGinnis
    ],
    # Cerner SMART-family sandbox patients on the open tier (fhir-open.cerner.com).
    # IDs verified by Patient/{id} returning 200 on tenant ec2458f2- on 2026-04-27.
    # 12724066 (Nancy Smart) is the canonical lab-rich patient — answers the
    # "do labs work?" question Epic's Derrick Lin couldn't (he had 0 labs).
    "cerner": [
        "12724066",  # Nancy Smart (canonical, lab-rich: 5+ Observation labs/vitals/etc.)
        "12742399",  # Sandy Smart
        "12744580",  # John Smart
        "12747063",  # Smart On Fhir
        "12752183",  # Joe Smarty Mayank Homenick
        "12753604",  # Fabian Smarty Khan
        "12821252",  # Nancy2 Smart
    ],
}

# Resource probes used to characterize coverage per patient. Aligned with P0_PROBES in
# measure_phase_b but stripped to slug→query — we only need entry counts here.
P0_COVERAGE_PROBES = [
    # `_summary=count` asks the server for Bundle.total without paging — the only
    # reliable way to get a true match count from Epic. With `_count=1`, Epic
    # returns Bundle.total=1 (it counts the page, not the result set).
    ("Patient",                   "Patient/{patient_id}"),
    ("AllergyIntolerance",        "AllergyIntolerance?patient={patient_id}&_summary=count"),
    ("Condition-problems",        "Condition?patient={patient_id}&category=problem-list-item&_summary=count"),
    ("Condition-encounter-diag",  "Condition?patient={patient_id}&category=encounter-diagnosis&_summary=count"),
    ("Condition-health-concern",  "Condition?patient={patient_id}&category=health-concern&_summary=count"),
    ("Observation-lab",           "Observation?patient={patient_id}&category=laboratory&_summary=count"),
    ("Observation-vital-signs",   "Observation?patient={patient_id}&category=vital-signs&_summary=count"),
    ("MedicationRequest",         "MedicationRequest?patient={patient_id}&_summary=count"),
    ("Encounter",                 "Encounter?patient={patient_id}&_summary=count"),
    ("Procedure",                 "Procedure?patient={patient_id}&_summary=count"),
    ("DiagnosticReport-lab",      "DiagnosticReport?patient={patient_id}&category=LAB&_summary=count"),
    ("Immunization",              "Immunization?patient={patient_id}&_summary=count"),
    ("DocumentReference",         "DocumentReference?patient={patient_id}&_summary=count"),
]

# Probes that need ≥1 selected patient with substantive data, mapped from probe slug
# above to the P0 profile they exercise. Drives the per-profile patient selection.
PROBE_TO_PROFILE = {
    "Patient":                   "us-core-patient",
    "AllergyIntolerance":        "us-core-allergyintolerance",
    "Condition-problems":        "us-core-condition-problems-health-concerns",
    "Condition-health-concern":  "us-core-condition-problems-health-concerns",
    "Observation-lab":           "us-core-observation-lab",
    "Observation-vital-signs":   "us-core-vital-signs",
    "MedicationRequest":         "us-core-medicationrequest",
    "Encounter":                 "us-core-encounter",
    "Procedure":                 "us-core-procedure",
    "DiagnosticReport-lab":      "us-core-diagnosticreport-lab",
}


def fhir_get(base: str, path: str, token: str | None) -> requests.Response:
    headers = {"Accept": "application/fhir+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.get(f"{base.rstrip('/')}/{path.lstrip('/')}", headers=headers, timeout=60)


def get_token(ehr: str) -> tuple[str, str]:
    return get_access_token(ehr)


def display_name(patient: dict) -> str:
    name = (patient.get("name") or [{}])[0]
    family = name.get("family", "")
    given = " ".join(name.get("given", []) or [])
    return f"{given} {family}".strip() or patient.get("id", "(no name)")


def walk_patients(base: str, token: str | None, max_patients: int) -> tuple[list[dict], dict]:
    """Walk Patient by following Bundle.link[next] until we either run out of pages
    or hit max_patients. Returns (patients, walk_diagnostics) where the diagnostics
    record what happened — Epic refuses unfiltered Patient search and that itself is
    a Map finding worth surfacing.
    """
    patients: list[dict] = []
    seen_ids: set[str] = set()
    next_url = f"{base.rstrip('/')}/Patient?_count=50"
    walk_diag: dict = {"first_page_status": None, "first_page_error": None, "pages_walked": 0}

    while next_url and len(patients) < max_patients:
        page_hdrs = {"Accept": "application/fhir+json", "User-Agent": USER_AGENT}
        if token:
            page_hdrs["Authorization"] = f"Bearer {token}"
        r = requests.get(next_url, headers=page_hdrs, timeout=60)
        if walk_diag["first_page_status"] is None:
            walk_diag["first_page_status"] = r.status_code
        if not r.ok:
            walk_diag["first_page_error"] = r.text[:300]
            print(f"  WARN: page request returned {r.status_code} ({r.text[:160]})", file=sys.stderr)
            break
        body = r.json()
        if body.get("resourceType") != "Bundle":
            print(f"  WARN: non-Bundle response from {next_url}", file=sys.stderr)
            break
        walk_diag["pages_walked"] += 1
        for entry in body.get("entry", []) or []:
            # Skip Epic's "no results" outcome entries that masquerade as Bundle.entry
            if (entry.get("search") or {}).get("mode") == "outcome":
                continue
            res = entry.get("resource") or {}
            if res.get("resourceType") != "Patient":
                continue
            pid = res.get("id")
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            patients.append({
                "id": pid,
                "display_name": display_name(res),
                "gender": res.get("gender"),
                "birthDate": res.get("birthDate"),
                "discovery_source": "patient-search-pagination",
            })
            if len(patients) >= max_patients:
                break
        next_url = next(
            (l.get("url") for l in body.get("link", []) or [] if l.get("relation") == "next"),
            None,
        )
        # Be polite with the sandbox
        time.sleep(0.25)

    return patients, walk_diag


def hydrate_known_roster(base: str, token: str, ehr: str) -> list[dict]:
    """For EHRs whose sandboxes refuse unfiltered Patient search (Epic), seed from
    a hand-curated list of known sandbox patient FHIR IDs and verify each resolves."""
    roster: list[dict] = []
    for pid in KNOWN_PATIENT_ROSTERS.get(ehr, []):
        r = fhir_get(base, f"Patient/{pid}", token)
        if not r.ok:
            print(f"  WARN: known-roster id {pid} returned HTTP {r.status_code} (skipping)", file=sys.stderr)
            continue
        res = r.json()
        if res.get("resourceType") != "Patient":
            print(f"  WARN: {pid} returned {res.get('resourceType')}, not Patient (skipping)", file=sys.stderr)
            continue
        roster.append({
            "id": res.get("id", pid),
            "display_name": display_name(res),
            "gender": res.get("gender"),
            "birthDate": res.get("birthDate"),
            "discovery_source": "known-roster",
        })
        time.sleep(0.1)
    return roster


def probe_coverage(base: str, token: str, patient_id: str) -> dict[str, int]:
    """For a given patient, count entries returned by each P0 coverage probe."""
    out: dict[str, int] = {}
    for slug, template in P0_COVERAGE_PROBES:
        path = template.format(patient_id=patient_id)
        r = fhir_get(base, path, token)
        if not r.ok:
            out[slug] = -r.status_code  # encode failure as negative HTTP code
            continue
        try:
            body = r.json()
        except ValueError:
            out[slug] = -999
            continue
        if body.get("resourceType") == "Bundle":
            # Try to use Bundle.total when present (some servers lie via _count cap; total is truth)
            total = body.get("total")
            if isinstance(total, int):
                out[slug] = total
            else:
                out[slug] = len(body.get("entry", []) or [])
        elif body.get("resourceType") == "Patient":
            out[slug] = 1
        else:
            out[slug] = 0
        time.sleep(0.1)  # spread requests
    return out


def select_patients_per_probe(catalog: list[dict], target: int = 3) -> dict[str, dict]:
    """For each probe slug, pick up to `target` patients that returned ≥1 entry,
    preferring patients with the highest entry counts. Records both the chosen IDs
    and the diagnostic ('lab-rich' vs 'no-data') for the editorial layer."""
    selection: dict[str, dict] = {}
    for slug, _ in P0_COVERAGE_PROBES:
        eligible = [
            (p["id"], p["display_name"], p["resource_coverage"][slug])
            for p in catalog
            if p["resource_coverage"].get(slug, 0) > 0
        ]
        eligible.sort(key=lambda x: x[2], reverse=True)
        chosen = eligible[:target]
        max_count = eligible[0][2] if eligible else 0
        selection[slug] = {
            "profile_id": PROBE_TO_PROFILE.get(slug, ""),
            "patient_ids": [c[0] for c in chosen],
            "patient_descriptions": [
                {"id": c[0], "display_name": c[1], "entry_count": c[2]} for c in chosen
            ],
            "max_entry_count_observed": max_count,
            "patients_with_data_count": len(eligible),
            "selection_rationale": (
                f"chose top-{len(chosen)} of {len(eligible)} sandbox patients with ≥1 entry"
                if eligible else
                "no sandbox patients returned data for this probe — finding by absence is itself a Map row"
            ),
        }
    return selection


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--max-patients", type=int, default=30)
    ap.add_argument("--refresh", action="store_true", help="re-walk even if catalog already on disk")
    args = ap.parse_args()

    load_env(strict=True)

    catalog_path = EHRS_DIR / args.ehr / "sandbox_patients.json"
    selection_path = EHRS_DIR / args.ehr / "p0_patient_selection.json"

    if catalog_path.exists() and not args.refresh:
        existing = json.loads(catalog_path.read_text())
        print(f"using cached catalog at {catalog_path.relative_to(REPO_ROOT)} ({len(existing.get('patients', []))} patients).")
        print("re-run with --refresh to re-enumerate.")
        catalog = existing.get("patients", [])
    else:
        print(f"== Enumerating {args.ehr} sandbox patients (max={args.max_patients}) ==")
        cfg = EHR_CONFIG[args.ehr]
        flow = cfg.get("flow", "client_credentials")

        # auth_code flows scope tokens to a single consenting patient; the patient ID
        # comes from the token meta (fhirUser / patient claim). Skip the unfiltered
        # Patient walker — it would 401 or scope-out.
        if flow == "auth_code":
            meta = get_access_token_with_meta(args.ehr)
            token = meta["access_token"]
            base = meta["fhir_base"]
            consented_pid = meta.get("patient")
            print(f"step 1/3 — auth_code flow: consented patient = {consented_pid!r}")
            walked, walk_diag = [], {"first_page_status": "skipped", "pages_walked": 0,
                                       "note": "skipped — auth_code flows scope to a single patient; no enumeration."}
            known_roster: list[dict] = []
            if consented_pid:
                # Hydrate that one patient via Patient/{id}
                r = fhir_get(base, f"Patient/{consented_pid}", token)
                if r.ok:
                    res = r.json()
                    known_roster.append({
                        "id": res.get("id", consented_pid),
                        "display_name": display_name(res),
                        "gender": res.get("gender"),
                        "birthDate": res.get("birthDate"),
                        "discovery_source": "auth_code-token-claim",
                    })
        else:
            no_auth = bool(cfg.get("phase_b_no_auth"))
            phase_b_base_var = cfg.get("phase_b_base_var")
            if no_auth:
                base = os.environ[phase_b_base_var or cfg["fhir_base_var"]]
                token = None
                print(f"step 1/3 — auth: skipped (phase_b_no_auth=True; base={base})")
            else:
                token, base = get_token(args.ehr)
                if phase_b_base_var:
                    base = os.environ[phase_b_base_var]

            print("step 1a/3 — try unfiltered Patient pagination")
            walked, walk_diag = walk_patients(base, token, args.max_patients)
            print(f"  walked {len(walked)} patients (HTTP {walk_diag['first_page_status']}, {walk_diag['pages_walked']} pages)")

            print("step 1b/3 — hydrate known sandbox patient roster")
            known_roster = hydrate_known_roster(base, token, args.ehr)
            print(f"  hydrated {len(known_roster)} patients from KNOWN_PATIENT_ROSTERS[{args.ehr!r}]")

        # Combine, dedup (known-roster wins on duplicates because the labels are curated)
        seen = set()
        roster: list[dict] = []
        for p in known_roster + walked:
            if p["id"] in seen:
                continue
            seen.add(p["id"])
            roster.append(p)
            if len(roster) >= args.max_patients:
                break

        print(f"step 2/3 — probe coverage for {len(roster)} patients × {len(P0_COVERAGE_PROBES)} probes")
        catalog: list[dict] = []
        for i, p in enumerate(roster, 1):
            cov = probe_coverage(base, token, p["id"])
            entry = {**p, "resource_coverage": cov}
            catalog.append(entry)
            non_zero = {k: v for k, v in cov.items() if v > 0}
            print(f"  [{i:>2}/{len(roster)}] {p['display_name'][:32]:<32} {p['id'][:24]:<24} → {len(non_zero)}/{len(P0_COVERAGE_PROBES)} probes have data")

        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        catalog_path.write_text(json.dumps({
            "ehr": args.ehr,
            "captured_date": datetime.date.today().isoformat(),
            "max_patients_attempted": args.max_patients,
            "user_agent": USER_AGENT,
            "unfiltered_patient_search_diagnostics": walk_diag,
            "patients": catalog,
        }, indent=2) + "\n")
        print(f"  wrote {catalog_path.relative_to(REPO_ROOT)}")

    print()
    print("step 3/3 — pick ≥3 patients per probe")
    selection = select_patients_per_probe(catalog, target=3)
    selection_doc = {
        "ehr": args.ehr,
        "captured_date": datetime.date.today().isoformat(),
        "selection_target_per_probe": 3,
        "probes": selection,
    }
    selection_path.write_text(json.dumps(selection_doc, indent=2) + "\n")
    print(f"  wrote {selection_path.relative_to(REPO_ROOT)}")
    print()

    print("== Selection summary ==")
    for slug, sel in selection.items():
        chosen = len(sel["patient_ids"])
        flag = "OK " if chosen >= 3 else ("LO " if chosen > 0 else "GAP")
        print(f"  {flag}  {slug:<28} {chosen} of 3 chosen, max entries seen={sel['max_entry_count_observed']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
