"""Analyze MEDITECH Greenfield Phase B fixtures into element_deviation rows.

Reads every resource captured under
`tests/golden/meditech/phase-b-2026-04-28/` and emits structured
deviation observations comparing observed Greenfield shape against US
Core expectations. Output is a JSON document (stdout or `--output PATH`)
matching the `element_deviations[]` shape in the Cerner overlay so rows
can be appended to MEDITECH's overlay directly.

This is the "measurement" half of KATA-MEDITECH-PHASE-B-MEASUREMENT (#133):
the consent flow was unblocked 2026-04-28, the fixtures were captured,
this tool produces the structured deviations from the captured corpus.

Detection categories the tool emits:
  - `missing-coding` — a CodeableConcept on a USCDI-required path has
    `coding: []` or `coding: null`, leaving only `text`. US Core requires
    a binding-conformant Coding.
  - `nonstandard-system` — a coding uses a system URI not in the US Core
    binding's allowed list (e.g. `http://hl7.org/fhir/ndfrt` instead of
    `http://www.nlm.nih.gov/research/umls/rxnorm`).
  - `duplicate-coding` — the same `(system, code)` appears twice in a
    `.coding[]` array. Real Epic + Cerner deduplicate; MEDITECH sometimes
    emits identical codings repeated, which is a wire fingerprint.
  - `missing-meta` — top-level `meta` field absent on a returned
    resource. US Core doesn't strictly require it, but most real EHRs
    emit it (lastUpdated, versionId); absence is observable.

Run:
    cd the-map && python3 tools/analyze_meditech_phase_b.py
    python3 tools/analyze_meditech_phase_b.py --output meditech_deviations.json

The output's `row_id` field is a short hash of (profile_id, path,
deviation_category) so re-running the tool produces stable IDs that an
operator can match against existing overlay rows for an idempotent merge.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

THE_MAP = Path(__file__).resolve().parent.parent
FIXTURES_DIR = THE_MAP / "tests" / "golden" / "meditech" / "phase-b-2026-04-28"
DATE_VERIFIED = "2026-04-28"
SOURCE = "meditech_greenfield_phase_b"

# Resource type → US Core profile_id used in element_deviation rows.
PROFILE_MAP: dict[str, str] = {
    "Patient": "us-core-patient",
    "Condition": "us-core-condition-problems-health-concerns",
    "AllergyIntolerance": "us-core-allergyintolerance",
    "Observation": "us-core-observation-lab",  # refined below by category
    "Encounter": "us-core-encounter",
    "Immunization": "us-core-immunization",
    "MedicationRequest": "us-core-medicationrequest",
    "DiagnosticReport": "us-core-diagnosticreport-lab",
    "DocumentReference": "us-core-documentreference",
    "CarePlan": "us-core-careplan",
    "CareTeam": "us-core-careteam",
    "Goal": "us-core-goal",
    "Procedure": "us-core-procedure",
}

# US Core "preferred" code systems per common path. Used to detect
# `nonstandard-system` rows. Not exhaustive — focused on paths where the
# captured fixtures show observable deviations.
ALLOWED_SYSTEMS_BY_PATH: dict[str, set[str]] = {
    "AllergyIntolerance.code": {
        "http://www.nlm.nih.gov/research/umls/rxnorm",
        "http://snomed.info/sct",
        "http://hl7.org/fhir/sid/icd-10-cm",
        # US Core allows nullFlavor & some others; the tool flags NDF-RT
        # specifically because it's the deprecated drug terminology
        # MEDITECH is observed to emit.
    },
    "Condition.code": {
        "http://snomed.info/sct",
        "http://hl7.org/fhir/sid/icd-10-cm",
    },
    "Observation.code": {
        "http://loinc.org",
    },
}


def _row_id(profile_id: str, path: str, category: str) -> str:
    """Stable 12-char hex hash so re-runs produce identical IDs."""
    h = hashlib.sha256(f"{profile_id}|{path}|{category}".encode()).hexdigest()
    return h[:12]


def _load_resource(path: Path) -> dict | None:
    """Load a fixture file. Returns the inner resource (handles `body`
    wrapper from the harness)."""
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    return raw.get("body", raw) if isinstance(raw, dict) else None


def _resource_iter(payload: dict) -> list[dict]:
    """Yield resources from a payload — either a single resource or every
    `Bundle.entry[*].resource`."""
    if not isinstance(payload, dict):
        return []
    if payload.get("resourceType") == "Bundle":
        return [
            e.get("resource") for e in (payload.get("entry") or [])
            if isinstance(e, dict) and isinstance(e.get("resource"), dict)
        ]
    return [payload] if payload.get("resourceType") else []


def _coding_systems(cc: Any) -> list[str]:
    if not isinstance(cc, dict):
        return []
    return [
        c.get("system") for c in (cc.get("coding") or [])
        if isinstance(c, dict) and isinstance(c.get("system"), str)
    ]


def _observation_profile_id(resource: dict) -> str:
    """Refine Observation profile by category — labs vs vital-signs vs
    social-history vs other."""
    for cat in resource.get("category") or []:
        if not isinstance(cat, dict):
            continue
        for c in cat.get("coding") or []:
            code = c.get("code") if isinstance(c, dict) else None
            if code == "vital-signs":
                return "us-core-vital-signs"
            if code == "laboratory":
                return "us-core-observation-lab"
            if code == "social-history":
                return "us-core-smokingstatus"  # closest US Core social-history profile
    return "us-core-observation-lab"  # fallback


def _profile_id_for(resource: dict) -> str:
    rt = resource.get("resourceType")
    if rt == "Observation":
        return _observation_profile_id(resource)
    return PROFILE_MAP.get(rt or "", f"us-core-{(rt or '').lower()}")


def _check_missing_coding(
    resource: dict, path: str,
) -> tuple[str, str] | None:
    """Return (deviation_category, observed_quote) iff the CC at `path`
    has no Coding (only text)."""
    parts = path.split(".")
    cur: Any = resource
    for p in parts[1:]:  # skip resourceType
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            cur = cur[0].get(p) if cur and isinstance(cur[0], dict) else None
        else:
            return None
    # If the path resolved to a CodeableConcept-like dict with empty coding…
    if isinstance(cur, dict):
        codings = cur.get("coding")
        if not codings:
            txt = cur.get("text") or "(no text)"
            return ("missing-coding", f"coding[] empty/null; text={txt!r}")
    return None


def _check_nonstandard_system(
    resource: dict, path: str,
) -> tuple[str, str] | None:
    """Return (deviation_category, observed_quote) iff the CC carries a
    coding with a system outside the allowed set for `path`."""
    allowed = ALLOWED_SYSTEMS_BY_PATH.get(path)
    if not allowed:
        return None
    parts = path.split(".")
    cur: Any = resource
    for p in parts[1:]:
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            cur = cur[0].get(p) if cur and isinstance(cur[0], dict) else None
        else:
            return None
    systems = _coding_systems(cur)
    nonstandard = [s for s in systems if s and s not in allowed]
    if nonstandard:
        return ("nonstandard-system", f"system={nonstandard[0]!r}; expected one of {sorted(allowed)}")
    return None


def _check_duplicate_coding(
    resource: dict, path: str,
) -> tuple[str, str] | None:
    """Return (deviation_category, observed_quote) iff the CC has two
    codings with the same (system, code)."""
    parts = path.split(".")
    cur: Any = resource
    for p in parts[1:]:
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list):
            cur = cur[0].get(p) if cur and isinstance(cur[0], dict) else None
        else:
            return None
    if not isinstance(cur, dict):
        return None
    pairs = [
        (c.get("system"), c.get("code"))
        for c in (cur.get("coding") or [])
        if isinstance(c, dict)
    ]
    counts = Counter(pairs)
    dupes = [k for k, v in counts.items() if v > 1]
    if dupes:
        return ("duplicate-coding", f"{dupes[0]!r} appears {counts[dupes[0]]}× in coding[]")
    return None


def _check_missing_meta(resource: dict) -> tuple[str, str] | None:
    """Return (category, quote) iff top-level meta is absent or null."""
    if not resource.get("meta"):
        return ("missing-meta", "top-level `meta` absent or null")
    return None


CHECKS: list[tuple[str, str, Any]] = [
    # (path, expected_per_us_core_note, check_fn)
    ("AllergyIntolerance.code",
     "USCDI requirement; bound to RxNorm/SNOMED",
     _check_nonstandard_system),
    ("Condition.code",
     "USCDI requirement; bound to SNOMED/ICD-10-CM",
     _check_missing_coding),
    ("Condition.code",
     "USCDI requirement; bound to SNOMED/ICD-10-CM",
     _check_nonstandard_system),
    ("Observation.code",
     "USCDI requirement; bound to LOINC",
     _check_missing_coding),
    ("Observation.code",
     "USCDI requirement; bound to LOINC",
     _check_duplicate_coding),
    # Top-level meta — applies per resource regardless of path.
    ("Patient",            "meta SHOULD carry lastUpdated/versionId", "meta"),
    ("Condition",          "meta SHOULD carry lastUpdated/versionId", "meta"),
    ("Observation",        "meta SHOULD carry lastUpdated/versionId", "meta"),
    ("Encounter",          "meta SHOULD carry lastUpdated/versionId", "meta"),
    ("AllergyIntolerance", "meta SHOULD carry lastUpdated/versionId", "meta"),
]


def analyze() -> list[dict]:
    if not FIXTURES_DIR.exists():
        print(f"fixtures not found at {FIXTURES_DIR}", file=sys.stderr)
        return []

    # Aggregate (path, deviation_category) → list of fixture filenames where seen.
    seen: dict[tuple[str, str, str], list[str]] = {}

    for fixture in sorted(FIXTURES_DIR.glob("*.json")):
        payload = _load_resource(fixture)
        if not payload:
            continue
        for resource in _resource_iter(payload):
            rt = resource.get("resourceType")
            if not rt:
                continue
            profile_id = _profile_id_for(resource)

            for path, expected_note, check in CHECKS:
                # Only apply checks whose path matches this resource type.
                if path.split(".")[0] != rt:
                    continue
                if check == "meta":
                    result = _check_missing_meta(resource)
                    full_path = f"{rt}.meta"
                else:
                    result = check(resource, path)
                    full_path = path
                if result is None:
                    continue
                deviation_category, observed_quote = result
                key = (profile_id, full_path, deviation_category)
                seen.setdefault(key, []).append(fixture.name)

    # Emit as element_deviations rows.
    rows: list[dict] = []
    for (profile_id, path, deviation_category), fixtures in sorted(seen.items()):
        rows.append({
            "profile_id": profile_id,
            "path": path,
            "deviation_category": deviation_category,
            "expected_per_us_core": (
                "USCDI requirement / US Core binding — see profile spec"
            ),
            "observed_in_ehr": (
                f"{deviation_category} observed in {len(fixtures)} fixture(s) "
                f"under tests/golden/meditech/phase-b-2026-04-28/"
            ),
            "verification": {
                "source_url": (
                    "https://greenfield-prod-apis.meditech.com/v2/uscore/R4/"
                ),
                "source_quote": f"Observed in fixture(s): {fixtures[0]}",
                "verified_via": SOURCE,
                "verified_date": DATE_VERIFIED,
            },
            "multi_patient_evidence": {
                "category": "single-patient-canonical",
                "fixtures_observed_in": fixtures,
            },
            "row_id": _row_id(profile_id, path, deviation_category),
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--output", type=Path, help="write JSON output to PATH")
    args = ap.parse_args()

    rows = analyze()
    blob = json.dumps(rows, indent=2)
    if args.output:
        args.output.write_text(blob + "\n")
        print(f"wrote {len(rows)} deviations → {args.output}", file=sys.stderr)
    else:
        print(blob)
    return 0


if __name__ == "__main__":
    sys.exit(main())
