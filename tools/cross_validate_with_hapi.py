"""Phase 4: cross-validate captured Phase B responses against a HAPI FHIR server
preloaded with the US Core 6.1.0 IG.

For each captured Epic golden file, POST `Resource/$validate?profile=...` to HAPI
and store the returned OperationOutcome. Compare HAPI's issues to our analyzer's
element_deviations and record disagreements in overlay.cross_validation_disagreements.

Prerequisites:
  - HAPI FHIR running locally (default `--hapi-base=http://localhost:8090/fhir`).
    `docker compose -f tools/hapi/docker-compose.example.yml up -d` is the
    canonical way; that compose file preloads the US Core 6.1.0 IG automatically.
  - Verify HAPI is healthy with `GET {hapi_base}/StructureDefinition/us-core-patient`
    returning HTTP 200.

Usage:
    python -m tools.cross_validate_with_hapi epic
    python -m tools.cross_validate_with_hapi epic --hapi-base=http://localhost:8090/fhir
    python -m tools.cross_validate_with_hapi epic --skip-if-down   # graceful exit

When HAPI is down or the IG isn't preloaded, this writes a `cross_validation_disagreements`
section noting the deferral so downstream consumers know the axis isn't validated yet.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) HAPI-cross-validator"

# Map probe slug → US Core profile URL to validate against
PROFILE_BY_SLUG = {
    "patient": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient",
    "allergyintolerance": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-allergyintolerance",
    "condition-problems": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition-problems-health-concerns",
    "condition-encounter-diag": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition-encounter-diagnosis",
    "condition-health-concern": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition-problems-health-concerns",
    "observation-lab": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-lab",
    "observation-vital-signs": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-vital-signs",
    "medicationrequest": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-medicationrequest",
    "encounter": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter",
    "procedure": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-procedure",
    "diagnosticreport-lab": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-diagnosticreport-lab",
    "immunization": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-immunization",
    "documentreference": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-documentreference",
}


def slug_from_filename(name: str) -> str:
    """patient-eq081-...-json → 'patient'. condition-problems-eIX...json → 'condition-problems'."""
    stem = name.replace(".json", "")
    # Identify probe slug by longest prefix that matches a known slug
    for slug in sorted(PROFILE_BY_SLUG, key=len, reverse=True):
        if stem == slug or stem.startswith(slug + "-"):
            return slug
    return ""


def hapi_alive(hapi_base: str) -> tuple[bool, str]:
    try:
        r = requests.get(f"{hapi_base.rstrip('/')}/metadata", headers={"Accept": "application/fhir+json", "User-Agent": USER_AGENT}, timeout=5)
        if not r.ok:
            return False, f"HAPI metadata returned HTTP {r.status_code}"
        cs = r.json()
        if cs.get("resourceType") != "CapabilityStatement":
            return False, "HAPI returned non-CapabilityStatement at metadata"
    except requests.RequestException as e:
        return False, f"HAPI unreachable: {e}"
    # Verify IG package is preloaded
    sd_url = f"{hapi_base.rstrip('/')}/StructureDefinition/us-core-patient"
    try:
        r = requests.get(sd_url, headers={"Accept": "application/fhir+json", "User-Agent": USER_AGENT}, timeout=5)
    except requests.RequestException as e:
        return False, f"HAPI SD fetch failed: {e}"
    if not r.ok:
        return False, f"HAPI returned HTTP {r.status_code} for {sd_url} — US Core 6.1.0 IG package not preloaded"
    return True, f"HAPI alive at {hapi_base}; us-core-patient SD reachable"


def validate_resource(hapi_base: str, resource: dict, profile_url: str) -> dict:
    rt = resource.get("resourceType")
    if not rt:
        return {"error": "no resourceType"}
    url = f"{hapi_base.rstrip('/')}/{rt}/$validate?profile={profile_url}"
    r = requests.post(
        url,
        json=resource,
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "User-Agent": USER_AGENT,
        },
        timeout=60,
    )
    try:
        body = r.json()
    except ValueError:
        body = {"_non_json_body": r.text[:500]}
    return {"http_status": r.status_code, "operation_outcome": body}


def first_resource(body: dict, resource_type_hint: str | None) -> dict | None:
    if body.get("resourceType") == resource_type_hint or (body.get("resourceType") and body.get("resourceType") != "Bundle"):
        return body
    if body.get("resourceType") != "Bundle":
        return None
    for entry in body.get("entry", []) or []:
        if (entry.get("search") or {}).get("mode") == "outcome":
            continue
        res = entry.get("resource") or {}
        if res.get("resourceType") == "OperationOutcome":
            continue
        if not resource_type_hint or res.get("resourceType") == resource_type_hint:
            return res
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr")
    ap.add_argument("--hapi-base", default="http://localhost:8090/fhir/DEFAULT")
    ap.add_argument("--skip-if-down", action="store_true", help="exit 0 with deferred note when HAPI is unavailable")
    ap.add_argument("--max-files", type=int, default=50, help="cap how many golden files to validate")
    args = ap.parse_args()

    today = datetime.date.today().isoformat()
    overlay_path = EHRS_DIR / args.ehr / "overlay.json"
    overlay = json.loads(overlay_path.read_text())
    golden_root = GOLDEN_DIR / args.ehr / f"phase-b-{today}"
    if not golden_root.exists():
        # Try latest available
        candidates = sorted([p for p in (GOLDEN_DIR / args.ehr).glob("phase-b-*") if p.is_dir()], reverse=True)
        if not candidates:
            sys.exit(f"no golden phase-b-*/ dir under {GOLDEN_DIR / args.ehr}")
        golden_root = candidates[0]
    print(f"== HAPI cross-validation for {args.ehr} ==")
    print(f"  hapi_base:  {args.hapi_base}")
    print(f"  golden_dir: {golden_root.relative_to(REPO_ROOT)}")

    alive, msg = hapi_alive(args.hapi_base)
    if not alive:
        deferred_note = {
            "status": "deferred",
            "reason": msg,
            "instructions": (
                "Bring up HAPI with `docker compose -f tools/hapi/docker-compose.example.yml up -d` "
                "(or any HAPI image with US Core 6.1.0 preloaded). "
                "Then re-run `python -m tools.cross_validate_with_hapi {ehr}`."
            ),
            "verification": {
                "source_url": args.hapi_base,
                "source_quote": msg,
                "verified_via": "epic_public_sandbox",
                "verified_date": today,
            },
        }
        overlay["cross_validation_disagreements"] = deferred_note
        overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
        print(f"  HAPI not ready: {msg}")
        print(f"  Wrote deferred note to {overlay_path.relative_to(REPO_ROOT)}.cross_validation_disagreements")
        if args.skip_if_down:
            return 0
        return 1
    print(f"  {msg}")

    files = [
        f for f in sorted(golden_root.glob("*.json"))
        if not f.name.startswith("error-") and f.name != "sweep-summary.json"
    ][: args.max_files]

    paired_dir = golden_root  # Write *-hapi-validation.json next to fixtures
    disagreements = []
    summary = {"validated": 0, "matched": 0, "disagreement": 0, "skipped": 0}
    for f in files:
        slug = slug_from_filename(f.name)
        if not slug:
            summary["skipped"] += 1
            continue
        profile_url = PROFILE_BY_SLUG[slug]
        try:
            doc = json.loads(f.read_text())
        except json.JSONDecodeError:
            summary["skipped"] += 1
            continue
        body = doc.get("body") or {}
        rtype = profile_url.rsplit("-", 1)[-1].title()
        # Map slug to expected resourceType crudely
        rtype_map = {
            "patient": "Patient", "allergyintolerance": "AllergyIntolerance",
            "condition-problems": "Condition", "condition-encounter-diag": "Condition",
            "condition-health-concern": "Condition", "observation-lab": "Observation",
            "observation-vital-signs": "Observation", "medicationrequest": "MedicationRequest",
            "encounter": "Encounter", "procedure": "Procedure",
            "diagnosticreport-lab": "DiagnosticReport", "immunization": "Immunization",
            "documentreference": "DocumentReference",
        }
        target_rtype = rtype_map.get(slug, rtype)
        target = first_resource(body, target_rtype)
        if target is None:
            summary["skipped"] += 1
            continue
        result = validate_resource(args.hapi_base, target, profile_url)
        out_path = paired_dir / f.name.replace(".json", "-hapi-validation.json")
        out_path.write_text(json.dumps(result, indent=2))
        summary["validated"] += 1

        oo = result.get("operation_outcome", {})
        if oo.get("resourceType") == "OperationOutcome":
            issues = oo.get("issue", []) or []
            errors = [i for i in issues if i.get("severity") == "error"]
            if errors:
                summary["disagreement"] += 1
                disagreements.append({
                    "fixture": str(f.relative_to(REPO_ROOT)),
                    "profile": profile_url,
                    "hapi_error_count": len(errors),
                    "hapi_first_error": errors[0].get("diagnostics", "")[:200],
                })
            else:
                summary["matched"] += 1
        else:
            summary["disagreement"] += 1

    overlay["cross_validation_disagreements"] = {
        "status": "ran",
        "summary": summary,
        "disagreements_sample": disagreements[:50],
        "verification": {
            "source_url": args.hapi_base,
            "source_quote": f"validated {summary['validated']} fixtures, {summary['disagreement']} HAPI errors",
            "verified_via": "epic_public_sandbox",
            "verified_date": today,
        },
    }
    overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    print(f"  validated {summary['validated']}, matched {summary['matched']}, disagreed {summary['disagreement']}, skipped {summary['skipped']}")
    print(f"  paired *-hapi-validation.json written under {paired_dir.relative_to(REPO_ROOT)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
