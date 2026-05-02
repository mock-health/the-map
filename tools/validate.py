"""Validate every ehrs/{ehr}/{CapabilityStatement.json,overlay.json} pair.

Rules:
  CapStmt:
    1. Must be a valid FHIR R4 CapabilityStatement (resourceType + fhirVersion).
    2. Must have a non-empty rest[0].resource list.
  Overlay:
    3. Validates against schema/overlay.schema.json.
    4. overlay.ehr matches its parent directory name.
    5. overlay.capability_statement_fetched_date matches the date the sibling
       CapStmt was retrieved from `date` field if present (warning, not error).
   5b. Every verification.verified_date in the overlay is within 540 days of
       today. ERROR by default; suppressible with --allow-stale (use only for
       conscious overrides such as documenting a vendor doc that hasn't moved).
    6. Every overlay verification.source_quote is non-STUB if the surrounding
       block claims real data (i.e., element_deviations entries can't be STUB).
   6b. row_id values across element_deviations are unique within a file
       (citation anchors must be stable).
   6c. Every element_deviations[*].profile_id resolves to a profile defined in
       us-core/us-core-6.1-baseline.json.
    7. capability_statement_url is reachable (opt-in, --check-urls).
    8. Synthesis succeeds without error.
  Production fleet (ehrs/{ehr}/production_fleet.json, optional):
    9. Validates against schema/production_fleet.schema.json + ehr matches dir +
       captured_date is fresh (warns at >90 days; ERROR at >14 days when
       THE_MAP_LAUNCH_GATE=1).
   9b. harvest_summary.endpoints_attempted == capstmt_fetched_ok + capstmt_fetch_failed.
   9c. Exactly one cluster per access_scope is marked modal, and the modal cluster
       is the largest within its scope.
  Hospital resolution (data/hospital-overrides/{vendor}-pos.json, optional):
   10. Every provider-tier fleet endpoint has a resolution row (ERROR if file
       present and missing rows; WARN only if file absent).
   11. Any row with match_strategy=manual_override carries an override_reason or notes.

Usage:
    python -m tools.validate
    python -m tools.validate epic
    python -m tools.validate --check-urls
    python -m tools.validate --allow-stale
"""
import argparse
import datetime
import json
import os
import sys
from pathlib import Path

try:
    import jsonschema
except ImportError:
    sys.stderr.write("ERROR: pip install jsonschema\n")
    sys.exit(2)

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
OVERLAY_SCHEMA_PATH = REPO_ROOT / "schema" / "overlay.schema.json"
FLEET_SCHEMA_PATH = REPO_ROOT / "schema" / "production_fleet.schema.json"
HOSPITAL_RESOLUTION_SCHEMA_PATH = REPO_ROOT / "schema" / "hospital_resolution.schema.json"
HOSPITAL_OVERRIDES_DIR = REPO_ROOT / "data" / "hospital-overrides"
US_CORE_BASELINE_PATH = REPO_ROOT / "us-core" / "us-core-6.1-baseline.json"

STALE_DAYS = 18 * 30  # 540 days; iron-rule citation freshness ceiling
FLEET_STALE_DAYS = 90  # production_fleet snapshots refresh quarterly
LAUNCH_GATE_DAYS = 14


def load_overlay_schema() -> dict:
    return json.loads(OVERLAY_SCHEMA_PATH.read_text())


def load_fleet_schema() -> dict | None:
    if not FLEET_SCHEMA_PATH.exists():
        return None
    return json.loads(FLEET_SCHEMA_PATH.read_text())


def _us_core_profile_ids() -> set[str]:
    if not US_CORE_BASELINE_PATH.exists():
        return set()
    return {p["profile_id"] for p in json.loads(US_CORE_BASELINE_PATH.read_text()).get("profiles", [])}


def _walk_verifications(ov: dict):
    """Yield (path_label, verification_dict) for every verification block."""
    for top in ("auth_overlay", "operation_outcome_overlay", "pagination_overlay",
                "phase_b_findings", "multi_patient_coverage", "access_scope_notes"):
        block = ov.get(top)
        if isinstance(block, dict) and isinstance(block.get("verification"), dict):
            yield top, block["verification"]
    for i, dev in enumerate(ov.get("element_deviations", []) or []):
        v = dev.get("verification")
        if isinstance(v, dict):
            yield f"element_deviations[{i}]", v
    spo = ov.get("search_param_observations") or {}
    if isinstance(spo, dict):
        for resource_type, block in spo.items():
            if isinstance(block, dict) and isinstance(block.get("verification"), dict):
                yield f"search_param_observations.{resource_type}", block["verification"]


def validate_one(ehr_dir: Path, schema: dict, check_urls: bool, allow_stale: bool, launch_gate: bool) -> list[str]:
    errors: list[str] = []
    warnings: list[str] = []
    name = ehr_dir.name

    cs_path = ehr_dir / "CapabilityStatement.json"
    ov_path = ehr_dir / "overlay.json"

    if not cs_path.exists():
        errors.append(f"{name}: missing CapabilityStatement.json (run `python -m tools.fetch_capability {name}`)")
    if not ov_path.exists():
        errors.append(f"{name}: missing overlay.json")
    if errors:
        return errors

    # Rule 1-2: CapStmt sanity
    try:
        cs = json.loads(cs_path.read_text())
    except json.JSONDecodeError as e:
        errors.append(f"{name}/CapabilityStatement.json: invalid JSON: {e}")
        return errors

    if cs.get("resourceType") != "CapabilityStatement":
        errors.append(f"{name}/CapabilityStatement.json: resourceType is {cs.get('resourceType')!r}, expected 'CapabilityStatement'")
    if not cs.get("fhirVersion", "").startswith("4."):
        errors.append(f"{name}/CapabilityStatement.json: fhirVersion {cs.get('fhirVersion')!r} is not R4")
    rest = cs.get("rest", [])
    if not rest or not rest[0].get("resource"):
        errors.append(f"{name}/CapabilityStatement.json: rest[0].resource is empty")

    # Rule 3-4: Overlay schema + ehr match
    try:
        ov = json.loads(ov_path.read_text())
    except json.JSONDecodeError as e:
        errors.append(f"{name}/overlay.json: invalid JSON: {e}")
        return errors

    try:
        jsonschema.validate(ov, schema)
    except jsonschema.ValidationError as e:
        errors.append(f"{name}/overlay.json: schema violation at {'/'.join(str(p) for p in e.absolute_path)}: {e.message}")

    if ov.get("ehr") != name:
        errors.append(f"{name}/overlay.json: ehr field {ov.get('ehr')!r} does not match directory name {name!r}")

    # Rule 5: fetched_date sanity vs capstmt date
    cs_date = (cs.get("date") or "")[:10]
    ov_fetched = ov.get("capability_statement_fetched_date", "")
    if cs_date and ov_fetched and cs_date != ov_fetched:
        warnings.append(f"{name}: overlay.capability_statement_fetched_date={ov_fetched} differs from CapStmt.date={cs_date} (acceptable if EHR returns generation-time date, not fetch-time)")

    # Rule 6: element_deviations must not be STUB
    for i, dev in enumerate(ov.get("element_deviations", []) or []):
        v = dev.get("verification", {})
        if v.get("source_quote", "").strip().upper().startswith("STUB"):
            errors.append(f"{name}/overlay.json: element_deviations[{i}] (profile={dev.get('profile_id')}, path={dev.get('path')}) has STUB source_quote — must be real evidence")

    # Rule 6b: row_id uniqueness across element_deviations
    devs = ov.get("element_deviations", []) or []
    row_ids = [d.get("row_id") for d in devs if d.get("row_id")]
    if len(row_ids) != len(set(row_ids)):
        dupes = sorted({r for r in row_ids if row_ids.count(r) > 1})
        errors.append(f"{name}/overlay.json: duplicate element_deviations row_ids: {dupes}")

    # Rule 6c: profile_id cross-ref against us-core baseline
    valid_profiles = _us_core_profile_ids()
    if valid_profiles:
        bad_profiles = sorted({d.get("profile_id") for d in devs if d.get("profile_id") and d["profile_id"] not in valid_profiles})
        if bad_profiles:
            errors.append(f"{name}/overlay.json: element_deviations reference profile_ids not in us-core baseline: {bad_profiles}")

    # Rule 5 (continued) + 5b: staleness across all verification blocks
    today = datetime.date.today()
    for label, v in _walk_verifications(ov):
        raw = v.get("verified_date")
        if raw is None:
            continue
        try:
            d = datetime.date.fromisoformat(raw)
        except ValueError:
            errors.append(f"{name}: invalid verified_date in {label}: {raw!r}")
            continue
        age = (today - d).days
        if age > STALE_DAYS:
            msg = f"{name}: STALE {label}.verified_date ({age} days old, ceiling {STALE_DAYS})"
            if allow_stale:
                warnings.append(msg + " [--allow-stale]")
            else:
                errors.append(msg + " — re-verify or pass --allow-stale to acknowledge")

    # Rule 8: synthesis succeeds
    try:
        sys.path.insert(0, str(REPO_ROOT))
        from tools import synthesize
        synthesize.synthesize_ehr(name)
    except Exception as e:
        errors.append(f"{name}: synthesis failed: {e}")

    # Rules 9 / 9b / 9c / 12: production_fleet.json (optional)
    fleet_path = ehr_dir / "production_fleet.json"
    if fleet_path.exists():
        fleet_schema = load_fleet_schema()
        try:
            fleet = json.loads(fleet_path.read_text())
        except json.JSONDecodeError as e:
            errors.append(f"{name}/production_fleet.json: invalid JSON: {e}")
            fleet = None
        if fleet is not None and fleet_schema is not None:
            try:
                jsonschema.validate(fleet, fleet_schema)
            except jsonschema.ValidationError as e:
                errors.append(f"{name}/production_fleet.json: schema violation at {'/'.join(str(p) for p in e.absolute_path)}: {e.message}")
            if fleet.get("ehr") != name:
                errors.append(f"{name}/production_fleet.json: ehr field {fleet.get('ehr')!r} does not match directory {name!r}")

            # Rule 9b: harvest reconciliation
            h = fleet.get("harvest_summary") or {}
            attempted = h.get("endpoints_attempted")
            ok = h.get("capstmt_fetched_ok")
            failed = h.get("capstmt_fetch_failed")
            if attempted is not None and ok is not None and failed is not None and attempted != ok + failed:
                errors.append(f"{name}/production_fleet.json: harvest_summary attempted={attempted} != ok={ok}+failed={failed}")

            # Rule 9c: modal cluster invariant per access_scope
            clusters = fleet.get("capstmt_shape_clusters") or []
            by_scope: dict[str, list[dict]] = {}
            for c in clusters:
                by_scope.setdefault(c.get("access_scope") or "_default", []).append(c)
            for scope, items in by_scope.items():
                modal = [c for c in items if c.get("modal") is True]
                if len(modal) != 1:
                    errors.append(f"{name}/production_fleet.json: scope={scope} expected 1 modal cluster, got {len(modal)}")
                elif items:
                    largest = max(items, key=lambda c: c.get("endpoint_count", 0))
                    if modal[0].get("cluster_id") != largest.get("cluster_id"):
                        errors.append(
                            f"{name}/production_fleet.json: scope={scope} modal cluster "
                            f"{modal[0].get('cluster_id')} ({modal[0].get('endpoint_count')}) is not the "
                            f"largest ({largest.get('cluster_id')} = {largest.get('endpoint_count')})"
                        )

            # Rule 9 / 12: captured_date freshness
            try:
                d = datetime.date.fromisoformat(fleet.get("captured_date") or "")
                age = (today - d).days
                if launch_gate and age > LAUNCH_GATE_DAYS:
                    errors.append(
                        f"{name}: production_fleet.captured_date is {age} days old "
                        f"(launch gate requires ≤ {LAUNCH_GATE_DAYS}). Re-run "
                        f"`python -m tools.harvest_production_capstmts {name} && "
                        f"python -m tools.analyze_fleet_drift {name}`."
                    )
                elif age > FLEET_STALE_DAYS:
                    warnings.append(f"{name}: STALE production_fleet.captured_date ({age} days old) — re-run `python -m tools.harvest_production_capstmts {name}` and `python -m tools.analyze_fleet_drift {name}`")
            except ValueError:
                errors.append(f"{name}/production_fleet.json: invalid captured_date: {fleet.get('captured_date')!r}")

            # Rule 10: every provider-tier fleet endpoint has a resolution row
            res_path = HOSPITAL_OVERRIDES_DIR / f"{name}-pos.json"
            if res_path.exists():
                try:
                    res = json.loads(res_path.read_text())
                except json.JSONDecodeError as e:
                    errors.append(f"{res_path.relative_to(REPO_ROOT)}: invalid JSON: {e}")
                    res = None
                if res is not None:
                    cluster_scope = {c["cluster_id"]: c.get("access_scope") for c in clusters}
                    fleet_addrs: set[str] = set()
                    for c in clusters:
                        if c.get("access_scope") == "patient":
                            continue
                        for ep in c.get("endpoints", []) or []:
                            if ep.get("address"):
                                fleet_addrs.add(ep["address"].rstrip("/"))
                    for ep in fleet.get("outlier_endpoints", []) or []:
                        if cluster_scope.get(ep.get("cluster_id")) == "patient":
                            continue
                        if ep.get("address"):
                            fleet_addrs.add(ep["address"].rstrip("/"))
                    res_addrs = {
                        ep["endpoint_address"].rstrip("/")
                        for ep in res.get("endpoints", []) or []
                        if ep.get("endpoint_address")
                    }
                    missing = fleet_addrs - res_addrs
                    slack = max(1, int(0.02 * len(fleet_addrs)))
                    if len(missing) > slack:
                        errors.append(
                            f"{name}: {len(missing)} provider-tier fleet endpoints have no hospital-resolution row "
                            f"in {res_path.relative_to(REPO_ROOT)} (slack {slack}). "
                            f"Run `python -m tools.resolve_endpoints_to_pos {name}`. "
                            f"First missing: {sorted(missing)[:3]}"
                        )
            else:
                warnings.append(
                    f"{name}: no {res_path.relative_to(REPO_ROOT)} — run "
                    f"`python -m tools.resolve_endpoints_to_pos {name}` to enable hospital lookups"
                )

    # Rule 7: URL reachability (opt-in)
    if check_urls:
        try:
            import requests
        except ImportError:
            errors.append("--check-urls requires `pip install requests`")
        else:
            url = ov.get("capability_statement_url")
            if url:
                try:
                    r = requests.get(url, headers={"Accept": "application/fhir+json"}, timeout=15)
                    if r.status_code >= 400:
                        warnings.append(f"{name}: capability_statement_url returned {r.status_code}: {url}")
                except Exception as e:
                    warnings.append(f"{name}: capability_statement_url unreachable: {e}: {url}")

    for w in warnings:
        sys.stderr.write(f"WARN: {w}\n")
    return errors


def validate_hospital_resolutions() -> list[str]:
    """Validate every data/hospital-overrides/{vendor}-pos.json against the
    hospital_resolution.schema.json. Returns a list of error strings.

    Also enforces Rule 11: manual_override rows must carry an override_reason
    or notes field — the schema doesn't require it, but the iron rule does.
    """
    if not HOSPITAL_OVERRIDES_DIR.is_dir():
        return []
    if not HOSPITAL_RESOLUTION_SCHEMA_PATH.exists():
        return []
    schema = json.loads(HOSPITAL_RESOLUTION_SCHEMA_PATH.read_text())
    errors: list[str] = []
    files = sorted(HOSPITAL_OVERRIDES_DIR.glob("*-pos.json"))
    for f in files:
        try:
            payload = json.loads(f.read_text())
        except json.JSONDecodeError as e:
            errors.append(f"{f.relative_to(REPO_ROOT)}: invalid JSON: {e}")
            continue
        try:
            jsonschema.validate(payload, schema)
        except jsonschema.ValidationError as e:
            errors.append(
                f"{f.relative_to(REPO_ROOT)}: schema violation at "
                f"{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
            )
            continue
        # Iron rule: every endpoint must carry a parseable verified_date.
        # Rule 11: manual_override rows must document why.
        for i, ep in enumerate(payload.get("endpoints", [])):
            try:
                datetime.date.fromisoformat(ep.get("verified_date", ""))
            except ValueError:
                errors.append(
                    f"{f.relative_to(REPO_ROOT)}: endpoints[{i}] invalid verified_date "
                    f"{ep.get('verified_date')!r}"
                )
            if ep.get("match_strategy") == "manual_override":
                if not (ep.get("override_reason") or ep.get("notes")):
                    errors.append(
                        f"{f.relative_to(REPO_ROOT)}: endpoints[{i}] match_strategy=manual_override "
                        f"but no override_reason/notes (Rule 11)"
                    )
        print(f"{'OK  ' if not errors else 'FAIL'}  {f.relative_to(REPO_ROOT)}  "
              f"({payload.get('match_summary', {}).get('matched_unique', '?')} matched / "
              f"{payload.get('match_summary', {}).get('endpoints_total', '?')} total)")
    return errors


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", nargs="?")
    ap.add_argument("--check-urls", action="store_true")
    ap.add_argument(
        "--allow-stale",
        action="store_true",
        help="Demote stale-date errors (>540 days) to warnings. Use only when consciously acknowledging un-refreshed citations.",
    )
    args = ap.parse_args()

    launch_gate = os.environ.get("THE_MAP_LAUNCH_GATE") == "1"
    if launch_gate:
        sys.stderr.write("INFO: THE_MAP_LAUNCH_GATE=1 — fleet captured_date >14 days will fail.\n")

    schema = load_overlay_schema()

    if args.ehr:
        targets = [EHRS_DIR / args.ehr]
    else:
        targets = sorted([d for d in EHRS_DIR.iterdir() if d.is_dir()])

    if not targets:
        print("No ehrs/*/ directories found.")
        return 1

    all_errors: list[str] = []
    for t in targets:
        if not t.exists() or not t.is_dir():
            all_errors.append(f"{t.name}: directory not found")
            continue
        errs = validate_one(t, schema, args.check_urls, args.allow_stale, launch_gate)
        all_errors.extend(errs)
        print(f"{'OK  ' if not errs else 'FAIL'}  {t.name}/  ({len(errs)} error{'s' if len(errs) != 1 else ''})")

    # Validate hospital resolution overrides (independent of any single EHR).
    if not args.ehr:
        hr_errors = validate_hospital_resolutions()
        all_errors.extend(hr_errors)

    for e in all_errors:
        print(f"  - {e}")

    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main())
