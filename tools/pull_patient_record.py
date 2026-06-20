"""Pull YOUR OWN record from a production Epic via a SMART patient-access launch.

Companion to tools.crosscheck_production. Authenticates with the configured EHR
(default: epic_unc — UNC Health production, patient-standalone launch via
tools.auth_flows), then fetches the signed-in patient's resources and writes them
to a local directory for the cross-check to analyze.

PHI: every file written here is REAL patient data. Point --out at a gitignored path
(default .phi/<ehr>/wire). Nothing here is ever committed; only the redacted report
produced by tools.crosscheck_production goes into the repo.

Two fetch modes:
  per-resource (default) — search each US Core resource type (robust; uses the same
                           probe set as the Phase B sweep), following `next` links.
  --everything           — try Patient/{id}/$everything first; fall back to
                           per-resource if the server doesn't support it.

Usage:
    # 1) register a public PKCE patient app on fhir.epic.com, set its client_id:
    #    export EPIC_UNC_PATIENT_CLIENT_ID=...   (see docs/PRODUCTION-CROSSCHECK-HANDOFF.md)
    # 2) authenticate + pull (opens browser for MyChart consent on first run):
    python -m tools.pull_patient_record --ehr epic_unc --out .phi/epic_unc/wire
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

from tools.auth_flows import EHR_CONFIG, get_access_token_with_meta
from tools.measure_phase_b import P0_PROBES, USER_AGENT, real_entries

REPO_ROOT = Path(__file__).resolve().parent.parent


def _get(url: str, token: str) -> requests.Response:
    return requests.get(
        url,
        headers={
            "Accept": "application/fhir+json",
            "User-Agent": USER_AGENT,
            "Authorization": f"Bearer {token}",
        },
        timeout=60,
    )


def _next_link(bundle: dict) -> str | None:
    for link in bundle.get("link", []) or []:
        if link.get("relation") == "next" and link.get("url"):
            return link["url"]
    return None


def fetch_search(fhir_base: str, rel_path: str, token: str, max_pages: int) -> dict:
    """GET a search, following `next` links up to max_pages; merge into one Bundle."""
    url = f"{fhir_base.rstrip('/')}/{rel_path.lstrip('/')}"
    merged: list[dict] = []
    pages = 0
    while url and pages < max_pages:
        r = _get(url, token)
        if not r.ok:
            print(f"    HTTP {r.status_code} on {rel_path} (page {pages + 1}) — stopping", file=sys.stderr)
            break
        body = r.json()
        merged.extend(real_entries(body))
        pages += 1
        url = _next_link(body)
        if url:
            time.sleep(0.2)  # be polite to a real production endpoint
    return {"resourceType": "Bundle", "type": "searchset", "entry": merged}


def pull_per_resource(fhir_base: str, patient_id: str, token: str, out: Path, max_pages: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    # The patient resource itself
    r = _get(f"{fhir_base.rstrip('/')}/Patient/{patient_id}", token)
    if r.ok:
        (out / "Patient.json").write_text(json.dumps(r.json(), indent=2) + "\n")
        counts["Patient"] = 1
    else:
        print(f"  Patient/{patient_id} -> HTTP {r.status_code}", file=sys.stderr)

    for slug, probe in P0_PROBES.items():
        if slug == "Patient":
            continue
        rel = probe["path_template"].format(patient_id=patient_id)
        bundle = fetch_search(fhir_base, rel, token, max_pages)
        n = len(bundle["entry"])
        if n:
            (out / f"{slug}.json").write_text(json.dumps(bundle, indent=2) + "\n")
        counts[slug] = n
        print(f"  {slug}: {n} resources")
    return counts


def pull_everything(fhir_base: str, patient_id: str, token: str, out: Path, max_pages: int) -> dict[str, int] | None:
    rel = f"Patient/{patient_id}/$everything"
    url = f"{fhir_base.rstrip('/')}/{rel}"
    r = _get(url, token)
    if not r.ok:
        print(f"  $everything not available (HTTP {r.status_code}); falling back to per-resource")
        return None
    bundle = fetch_search(fhir_base, rel, token, max_pages)
    (out / "everything.json").write_text(json.dumps(bundle, indent=2) + "\n")
    counts: dict[str, int] = {}
    for e in bundle["entry"]:
        rt = (e.get("resource") or {}).get("resourceType", "?")
        counts[rt] = counts.get(rt, 0) + 1
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ehr", default="epic_unc", choices=sorted(EHR_CONFIG),
                    help="EHR_CONFIG key for the production patient-access launch")
    ap.add_argument("--out", type=Path, default=None,
                    help="gitignored output dir for raw PHI (default .phi/<ehr>/wire)")
    ap.add_argument("--everything", action="store_true",
                    help="try Patient/{id}/$everything before per-resource search")
    ap.add_argument("--max-pages", type=int, default=20, help="pagination cap per search")
    ap.add_argument("--force-refresh", action="store_true", help="re-walk consent (ignore token cache)")
    args = ap.parse_args()

    out = args.out or (REPO_ROOT / ".phi" / args.ehr / "wire")
    out.mkdir(parents=True, exist_ok=True)

    print(f"== production patient-access pull: {args.ehr} ==")
    meta = get_access_token_with_meta(args.ehr, force_refresh=args.force_refresh)
    token = meta["access_token"]
    fhir_base = meta["fhir_base"]
    patient_id = meta.get("patient")
    if not patient_id:
        sys.exit("token response carried no `patient` claim — was launch/patient in scope? "
                 "A standalone patient launch must return the patient id.")
    print(f"  authenticated; patient claim present; fhir_base={fhir_base}")
    print(f"  writing raw resources -> {out}  (PHI — gitignored)")

    counts = None
    if args.everything:
        counts = pull_everything(fhir_base, patient_id, token, out, args.max_pages)
    if counts is None:
        counts = pull_per_resource(fhir_base, patient_id, token, out, args.max_pages)

    total = sum(counts.values())
    print(f"\npulled {total} resources across {len([k for k, v in counts.items() if v])} types")
    print("next: python -m tools.crosscheck_production --source wire "
          f"--in {out} --phi-out {out.parent}/crosscheck "
          "--report reports/production-crosscheck-epic-unc-$(date +%F).md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
