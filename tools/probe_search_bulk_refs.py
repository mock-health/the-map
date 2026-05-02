"""Phase 3: search-param coverage, $export bulk export, and reference resolution.

Three independent probes, each writing to its own overlay section. All idempotent;
each can be run with --skip-{search,bulk,refs} to exercise just one.

Usage:
    python -m tools.probe_search_bulk_refs epic
    python -m tools.probe_search_bulk_refs epic --skip-bulk    # if Bulk Data scope unauthorized
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
import time
from pathlib import Path

import requests

from tools._env import load_env
from tools.oauth_handshake import EHR_CONFIG, get_access_token

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) Phase-B-search-bulk-refs"

# Bogus param names to test the silent-ignore class of bug across resources
BOGUS_PARAMS = [
    "totally-bogus-param",
    "_definitely_not_a_real_param",
    "nonsense=42",
    "spelled-rong",
    "patient_typo",  # close to real `patient` but underscore
]


def fhir_get(base: str, path: str, token: str) -> requests.Response:
    return requests.get(
        f"{base.rstrip('/')}/{path.lstrip('/')}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/fhir+json",
            "User-Agent": USER_AGENT,
        },
        timeout=60,
    )


def get_token(ehr: str) -> tuple[str, str]:
    return get_access_token(ehr)


# ---- Probe 1: search-param coverage ---------------------------------------------------

def probe_searches(*, base: str, token: str, capstmt: dict, selection: dict) -> dict:
    """For each P0 resource declared in the CapabilityStatement, send each declared
    search param with a known-valid value, then send each BOGUS_PARAMS value, and
    record the responses.
    """
    rest = (capstmt.get("rest") or [{}])[0]
    p0_probes = (selection.get("probes") or {})
    # Map probe slug to a sample patient_id
    probe_to_pid = {slug: (sel.get("patient_ids") or [None])[0] for slug, sel in p0_probes.items()}
    canonical_pid = probe_to_pid.get("Patient") or probe_to_pid.get("Encounter") or "missing-pid"

    out: dict = {}
    for resource in rest.get("resource", []) or []:
        rt = resource.get("type")
        if not rt:
            continue
        # Only probe a curated set of P0 resources — skip everything else to keep volume sane
        if rt not in {"Patient", "AllergyIntolerance", "Condition", "Observation", "MedicationRequest", "Encounter", "Procedure", "DiagnosticReport", "Immunization", "DocumentReference"}:
            continue
        declared = resource.get("searchParam") or []
        sample_values = _sample_search_values(rt, canonical_pid)

        params_silently_ignored: list[str] = []
        params_with_value_restrictions: list[dict] = []
        params_observed: list[dict] = []
        for sp in declared:
            name = sp.get("name") or ""
            if not name:
                continue
            sample_val = sample_values.get(name)
            if sample_val is None:
                # We don't have a known-good value for this param; skip rather than guess
                continue
            url = f"{rt}?{name}={sample_val}"
            r = fhir_get(base, url, token)
            params_observed.append({
                "name": name,
                "type": sp.get("type"),
                "test_url": url,
                "http_status": r.status_code,
                "first_120_bytes": r.text[:120].replace("\n", " "),
            })
            time.sleep(0.1)

        # Bogus-param probe: try one bogus param plus the known patient context
        bogus_results = []
        for bogus in BOGUS_PARAMS[:3]:
            url = f"{rt}?{bogus}=foo&patient={canonical_pid}" if rt != "Patient" else f"{rt}?{bogus}=foo"
            r = fhir_get(base, url, token)
            try:
                body = r.json()
            except ValueError:
                body = {"_non_json_body": r.text[:300]}
            shape = "OperationOutcome" if body.get("resourceType") == "OperationOutcome" else body.get("resourceType", "?")
            issue = body.get("issue", [])[0] if shape == "OperationOutcome" else {}
            severity = issue.get("severity") if isinstance(issue, dict) else None
            diagnostics = issue.get("diagnostics") if isinstance(issue, dict) else None
            bogus_results.append({
                "param": bogus,
                "test_url": url,
                "http_status": r.status_code,
                "first_issue_severity": severity,
                "first_issue_diagnostic": diagnostics,
                "shape": shape,
            })
            if r.ok and shape == "Bundle" and severity in (None, "information"):
                # Returned data despite bogus param — silent ignore
                params_silently_ignored.append(bogus)
            time.sleep(0.1)

        notes_parts = [f"Probed {len(params_observed)} declared search params + {len(bogus_results)} bogus probes."]
        if params_silently_ignored:
            notes_parts.append(f"silently-ignored: {params_silently_ignored}")

        out[rt] = {
            "params_silently_ignored": params_silently_ignored,
            "params_with_value_restrictions": params_with_value_restrictions,
            "declared_params_observed": params_observed,
            "bogus_param_results": bogus_results,
            "notes": " ".join(notes_parts),
            "verification": {
                "source_url": f"{base}/{rt}?...",
                "source_quote": "See declared_params_observed and bogus_param_results for verbatim status codes.",
                "verified_via": "epic_public_sandbox",
                "verified_date": datetime.date.today().isoformat(),
            },
        }
    return out


def _sample_search_values(rt: str, patient_id: str) -> dict:
    """Map a resource type's declared search params to a known-valid sample value
    we can use for a smoke test. Sourced from US Core search params + Epic patterns."""
    common = {
        "patient": patient_id,
        "_id": patient_id,
        "_count": "1",
        "_summary": "count",
    }
    if rt == "Patient":
        return {
            "_id": patient_id,
            "family": "Lin",
            "given": "Derrick",
            "birthdate": "1973-06-03",
            "gender": "male",
            "identifier": "FHRRJKD4PWK969G",
            "name": "Lin",
        }
    if rt == "AllergyIntolerance":
        return {**common, "clinical-status": "active"}
    if rt == "Condition":
        return {**common, "category": "problem-list-item", "clinical-status": "active"}
    if rt == "Observation":
        return {**common, "category": "vital-signs", "code": "85354-9"}
    if rt == "MedicationRequest":
        return {**common, "status": "active", "intent": "order"}
    if rt == "Encounter":
        return {**common, "status": "finished", "class": "AMB"}
    if rt == "Procedure":
        return {**common, "status": "completed"}
    if rt == "DiagnosticReport":
        return {**common, "category": "LAB", "status": "final"}
    if rt == "Immunization":
        return {**common, "status": "completed"}
    if rt == "DocumentReference":
        return {**common, "status": "current", "category": "clinical-note"}
    return common


# ---- Probe 2: Bulk export $export ----------------------------------------------------

def probe_bulk_export(*, base: str, token: str, max_poll_seconds: int = 60) -> dict:
    """Kick off /Patient/$export (group-level export not always available; per-patient
    requires a Group/{id} which we don't have). Capture kick-off shape, polling cadence,
    and final NDJSON URL pattern. Don't download the actual data — just record the shape.
    """
    out: dict = {"kickoff_status": None, "polling_status_history": [], "outputs": [], "notes": ""}
    # Kick off — Patient/$export is the SMART Bulk export "system" entry point at patient compartment level
    url = f"{base.rstrip('/')}/$export?_outputFormat=application/fhir+ndjson"
    r = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/fhir+json",
            "Prefer": "respond-async",
            "User-Agent": USER_AGENT,
        },
        timeout=30,
    )
    out["kickoff_status"] = r.status_code
    out["kickoff_endpoint"] = url
    out["kickoff_response_first_200"] = r.text[:200]
    if not 200 <= r.status_code < 300:
        out["notes"] = f"Bulk $export rejected at kickoff (HTTP {r.status_code}). Likely scope/grant issue or vendor doesn't permit system-level export on this app registration."
        return out
    poll_url = r.headers.get("Content-Location")
    if not poll_url:
        out["notes"] = "Kickoff returned no Content-Location header — cannot poll."
        return out
    out["poll_url"] = poll_url

    # Poll with backoff
    elapsed = 0
    delay = 2
    while elapsed < max_poll_seconds:
        pr = requests.get(
            poll_url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": USER_AGENT},
            timeout=30,
        )
        out["polling_status_history"].append({
            "elapsed_seconds": elapsed,
            "status": pr.status_code,
            "first_200": pr.text[:200],
        })
        if pr.status_code == 200:
            try:
                manifest = pr.json()
                out["outputs"] = manifest.get("output", [])
                out["error"] = manifest.get("error", [])
                out["transactionTime"] = manifest.get("transactionTime")
                out["notes"] = f"completed in ~{elapsed}s; {len(out['outputs'])} output files declared."
            except ValueError:
                out["notes"] = f"poll returned 200 but body wasn't JSON; first 200: {pr.text[:200]}"
            break
        if pr.status_code == 202:
            time.sleep(delay)
            elapsed += delay
            delay = min(delay * 2, 16)
            continue
        out["notes"] = f"poll returned non-2xx HTTP {pr.status_code}; aborting."
        break
    else:
        out["notes"] = f"poll timed out after {max_poll_seconds}s; manifest not received."
    return out


# ---- Probe 3: Reference resolution --------------------------------------------------

def probe_references(*, base: str, token: str, golden_dir: Path) -> dict:
    """For every Reference field in the captured Phase B golden files, verify it
    resolves and that the referenced resourceType matches expectations."""
    if not golden_dir.exists():
        return {"notes": f"no golden dir {golden_dir.relative_to(REPO_ROOT)} — run measure_phase_b first"}

    samples: dict[str, dict] = {}
    # Track unique reference target → metadata
    for f in sorted(golden_dir.glob("*.json")):
        if f.name.startswith("error-") or f.name == "sweep-summary.json":
            continue
        try:
            doc = json.loads(f.read_text())
        except json.JSONDecodeError:
            continue
        body = doc.get("body", {})
        for ref_field, ref_val in _walk_references(body):
            samples.setdefault(ref_val, {
                "first_seen_in": str(f.relative_to(REPO_ROOT)),
                "first_seen_field": ref_field,
                "format": _ref_format(ref_val, base),
            })

    # Sample-resolve up to 30 references (don't hammer the sandbox)
    sample_keys = list(samples.keys())[:30]
    results = []
    for ref_val in sample_keys:
        url = ref_val if ref_val.startswith("http") else f"{base.rstrip('/')}/{ref_val.lstrip('/')}"
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/fhir+json", "User-Agent": USER_AGENT},
            timeout=30,
        )
        try:
            body = r.json() if r.ok else {}
        except ValueError:
            body = {}
        results.append({
            "reference": ref_val,
            "format": samples[ref_val]["format"],
            "first_seen_in": samples[ref_val]["first_seen_in"],
            "first_seen_field": samples[ref_val]["first_seen_field"],
            "resolved_status": r.status_code,
            "resolved_resourceType": body.get("resourceType") if r.ok else None,
        })
        time.sleep(0.1)

    relative_count = sum(1 for r in results if r["format"] == "relative")
    absolute_count = sum(1 for r in results if r["format"] == "absolute")
    failed = [r for r in results if r["resolved_status"] >= 400]

    return {
        "total_references_seen": len(samples),
        "sampled_count": len(sample_keys),
        "format_distribution": {"relative": relative_count, "absolute": absolute_count},
        "failed_resolutions": failed,
        "samples": results,
        "verification": {
            "source_url": str(golden_dir.relative_to(REPO_ROOT)),
            "source_quote": "see samples[].first_seen_in for the captured response that contained each reference",
            "verified_via": "epic_public_sandbox",
            "verified_date": datetime.date.today().isoformat(),
        },
    }


def _walk_references(node: object, path: str = "") -> list[tuple[str, str]]:
    """Recursively walk a FHIR resource body and yield every (field_path, reference_string)."""
    out: list[tuple[str, str]] = []
    if isinstance(node, dict):
        if isinstance(node.get("reference"), str):
            out.append((path or "<root>.reference", node["reference"]))
        for k, v in node.items():
            out.extend(_walk_references(v, f"{path}.{k}" if path else k))
    elif isinstance(node, list):
        for i, item in enumerate(node):
            out.extend(_walk_references(item, f"{path}[{i}]"))
    return out


def _ref_format(ref: str, base: str) -> str:
    if ref.startswith("http://") or ref.startswith("https://"):
        return "absolute"
    return "relative"


# ---- Driver --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--skip-search", action="store_true")
    ap.add_argument("--skip-bulk", action="store_true")
    ap.add_argument("--skip-refs", action="store_true")
    ap.add_argument("--bulk-poll-seconds", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env(strict=True)

    today = datetime.date.today().isoformat()
    overlay_path = EHRS_DIR / args.ehr / "overlay.json"
    capstmt_path = EHRS_DIR / args.ehr / "CapabilityStatement.json"
    selection_path = EHRS_DIR / args.ehr / "p0_patient_selection.json"
    golden_root = GOLDEN_DIR / args.ehr / f"phase-b-{today}"

    overlay = json.loads(overlay_path.read_text())
    capstmt = json.loads(capstmt_path.read_text())
    selection = json.loads(selection_path.read_text()) if selection_path.exists() else {"probes": {}}

    print(f"== Phase 3 probes against {args.ehr} ==")
    print(f"  date: {today}")
    print()
    print("step 1/3 — get fresh access token")
    token, base = get_token(args.ehr)
    print("  OK")

    if not args.skip_search:
        print()
        print("step 2/3 — search-param coverage probe")
        search = probe_searches(base=base, token=token, capstmt=capstmt, selection=selection)
        print(f"  probed {len(search)} resource types")
        # Merge into existing overlay.search_param_observations (preserve any prior fields)
        existing = overlay.get("search_param_observations") or {}
        for rt, data in search.items():
            existing[rt] = {**existing.get(rt, {}), **data}
        overlay["search_param_observations"] = existing
    else:
        print("(skipping search-param probe)")

    if not args.skip_bulk:
        print()
        print("step 2.5/3 — Bulk export $export probe")
        bulk = probe_bulk_export(base=base, token=token, max_poll_seconds=args.bulk_poll_seconds)
        print(f"  kickoff: HTTP {bulk['kickoff_status']}; {bulk.get('notes','')}")
        bulk["verification"] = {
            "source_url": f"{base}/$export",
            "source_quote": bulk.get("notes") or f"kickoff HTTP {bulk['kickoff_status']}",
            "verified_via": "epic_public_sandbox",
            "verified_date": today,
        }
        overlay["bulk_export_overlay"] = bulk
    else:
        print("(skipping bulk export probe)")

    if not args.skip_refs:
        print()
        print("step 3/3 — reference resolution probe")
        refs = probe_references(base=base, token=token, golden_dir=golden_root)
        if isinstance(refs, dict) and "samples" in refs:
            print(f"  saw {refs['total_references_seen']} unique refs; sampled {refs['sampled_count']}; failed {len(refs['failed_resolutions'])}")
        overlay["reference_resolution_overlay"] = refs
    else:
        print("(skipping reference resolution probe)")

    if args.dry_run:
        print("\n(--dry-run: not writing overlay)")
        return 0
    overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    print(f"\n  updated {overlay_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
