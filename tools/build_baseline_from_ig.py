"""Build us-core/us-core-6.1-baseline.json from the official IG package.

Reads `us-core/ig-package/package/StructureDefinition-us-core-*.json` (download via
tools.fetch_us_core_ig) plus the server CapabilityStatement, and emits the per-profile
must-support + supported-search baseline that drives the conformance analyzer.

Conformance set per profile = (mustSupport=true) ∪ (USCDI-requirement extension flag).

Why both flags: in US Core 6.1 the differential element entries for extension slices
(Patient.extension:race, etc.) are NOT marked mustSupport=true on the parent profile;
they're flagged via `extension[url=…/uscdi-requirement][valueBoolean=true]` instead.
The hand-curated baseline correctly listed those as must-support; the IG-derived
baseline must too.

For each conformance element, we emit:
  - `path`            FHIRPath ready for the analyzer (extension slices use `Resource.extension(slug)` form)
  - `cardinality`     "min..max"
  - `min`             integer
  - `max`             "*", "0", or integer-as-string
  - `type`            list of FHIR primitive/complex codes (Quantity, CodeableConcept, …)
  - `target_profiles` list of profile URLs for Reference()/Extension() types
  - `binding`         {strength, valueSet, valueSet_id, description} when present
  - `slice_name`      when this is a sliced element
  - `must_support`    true/false from the SD JSON
  - `uscdi_requirement` true/false from the USCDI flag extension
  - `source_id`       SD element.id (auditable hook)

Search params come from CapabilityStatement-us-core-server.json. Each is annotated with
its `capabilitystatement-expectation` (SHALL/SHOULD/MAY).

Idempotent — re-runnable when US Core 7.x lands.

Usage:
    python -m tools.build_baseline_from_ig
    python -m tools.build_baseline_from_ig --version=6.1.0  # written into baseline header
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
IG_DIR = REPO_ROOT / "us-core" / "ig-package" / "package"
BASELINE_PATH = REPO_ROOT / "us-core" / "us-core-6.1-baseline.json"

USCDI_REQUIREMENT_EXT = "http://hl7.org/fhir/us/core/StructureDefinition/uscdi-requirement"
EXPECTATION_EXT = "http://hl7.org/fhir/StructureDefinition/capabilitystatement-expectation"

# US Core profile ID → editorial priority. P0 = the 9 USCDI-anchored profiles per PLAN.md.
# Anything not listed defaults to P2.
PRIORITY: dict[str, str] = {
    # P0 — USCDI v3 anchors (the launch set)
    "us-core-patient": "P0",
    "us-core-allergyintolerance": "P0",
    "us-core-condition-problems-health-concerns": "P0",
    "us-core-medicationrequest": "P0",
    "us-core-encounter": "P0",
    "us-core-procedure": "P0",
    "us-core-observation-lab": "P0",
    "us-core-diagnosticreport-lab": "P0",
    # Vital-signs umbrella + the per-vital profiles that constitute it. The umbrella + its
    # 9 component profiles are all P0 because integrators query Observation?category=vital-signs
    # and expect any of the 9 sub-profiles to validate.
    "us-core-vital-signs": "P0",
    "us-core-blood-pressure": "P0",
    "us-core-heart-rate": "P0",
    "us-core-respiratory-rate": "P0",
    "us-core-body-temperature": "P0",
    "us-core-body-height": "P0",
    "us-core-body-weight": "P0",
    "us-core-bmi": "P0",
    "us-core-pulse-oximetry": "P0",
    "us-core-head-circumference": "P0",
    # P1 — second wave (per PLAN.md "p1_profiles_pending_v2")
    "us-core-immunization": "P1",
    "us-core-smokingstatus": "P1",
    "us-core-careteam": "P1",
    "us-core-practitioner": "P1",
    "us-core-practitionerrole": "P1",
    "us-core-organization": "P1",
    "us-core-location": "P1",
    "us-core-documentreference": "P1",
    "us-core-coverage": "P1",
    "us-core-medication": "P1",
    "us-core-medicationdispense": "P1",
    "us-core-relatedperson": "P1",
    "us-core-specimen": "P1",
}

# USCDI v3 data classes covered by each profile (carried from the prior hand-curated
# baseline; the IG package itself doesn't carry this mapping).
USCDI_COVERAGE: dict[str, list[str]] = {
    "us-core-patient": ["patient-demographics"],
    "us-core-observation-lab": ["laboratory"],
    "us-core-diagnosticreport-lab": ["laboratory"],
    "us-core-medicationrequest": ["medications"],
    "us-core-medication": ["medications"],
    "us-core-medicationdispense": ["medications"],
    "us-core-allergyintolerance": ["allergies-intolerances"],
    "us-core-condition-problems-health-concerns": ["problems", "health-concerns"],
    "us-core-condition-encounter-diagnosis": ["problems"],
    "us-core-encounter": ["encounter-information", "patient-summary"],
    "us-core-vital-signs": ["vital-signs"],
    "us-core-blood-pressure": ["vital-signs"],
    "us-core-heart-rate": ["vital-signs"],
    "us-core-respiratory-rate": ["vital-signs"],
    "us-core-body-temperature": ["vital-signs"],
    "us-core-body-height": ["vital-signs"],
    "us-core-body-weight": ["vital-signs"],
    "us-core-bmi": ["vital-signs"],
    "us-core-pulse-oximetry": ["vital-signs"],
    "us-core-head-circumference": ["vital-signs"],
    "us-core-procedure": ["procedures"],
    "us-core-immunization": ["immunizations"],
    "us-core-smokingstatus": ["smoking-status"],
    "us-core-careteam": ["care-team-members"],
    "us-core-documentreference": ["clinical-notes"],
    "us-core-coverage": ["health-insurance-information"],
    "us-core-implantable-device": ["unique-device-identifiers"],
    "us-core-goal": ["goals"],
    "us-core-careplan": ["assessment-and-plan-of-treatment"],
    "us-core-provenance": ["provenance"],
    "us-core-diagnosticreport-note": ["diagnostic-imaging"],
    "us-core-observation-clinical-result": ["health-status-assessments"],
    "us-core-observation-pregnancystatus": ["health-status-assessments"],
    "us-core-observation-pregnancyintent": ["health-status-assessments"],
    "us-core-observation-occupation": ["health-status-assessments"],
    "us-core-observation-screening-assessment": ["health-status-assessments"],
    "us-core-observation-sexual-orientation": ["sexual-orientation-and-gender-identity"],
    "us-core-relatedperson": ["patient-summary"],
    "us-core-practitioner": ["patient-summary"],
    "us-core-practitionerrole": ["patient-summary"],
    "us-core-organization": ["patient-summary"],
    "us-core-location": ["patient-summary"],
    "us-core-specimen": ["laboratory"],
    "us-core-questionnaireresponse": ["questionnaire-responses"],
}


def is_resource_profile(sd: dict) -> bool:
    return (
        sd.get("kind") == "resource"
        and sd.get("type") != "Extension"
        and sd.get("derivation") == "constraint"
    )


def is_uscdi_requirement(elem: dict) -> bool:
    for ext in elem.get("extension", []) or []:
        if ext.get("url") == USCDI_REQUIREMENT_EXT and ext.get("valueBoolean"):
            return True
    return False


def slug_from_extension_profile(profile_url: str) -> str:
    """`http://.../StructureDefinition/us-core-race` → `us-core-race`."""
    return profile_url.rsplit("/", 1)[-1] if profile_url else ""


def render_path(elem: dict, resource_type: str) -> str:
    """Convert an SD element to a FHIRPath the analyzer can walk.

    Rules:
      - Plain element: use `element.path` verbatim, e.g. `Patient.name.family`.
      - Choice type (`[x]`): keep verbatim — e.g. `Patient.deceased[x]`.
      - Slice on an extension: emit `ResourceType.extension(slug)` where slug is
        derived from the slice's type[0].profile[0].
    """
    path = elem.get("path", "")
    slice_name = elem.get("sliceName")

    if slice_name and path.endswith(".extension"):
        # Find the bound profile slug (the extension's StructureDefinition id)
        slug = ""
        for t in elem.get("type", []) or []:
            for p in t.get("profile", []) or []:
                slug = slug_from_extension_profile(p)
                if slug:
                    break
            if slug:
                break
        if slug:
            parent = path.rsplit(".extension", 1)[0]  # strip trailing .extension
            return f"{parent}.extension({slug})"
        return f"{path}({slice_name})"

    # Non-extension slices (e.g. Observation.category:us-core) — keep the marker so
    # the analyzer can distinguish unsliced parent from sliced refinement.
    if slice_name:
        return f"{path}:{slice_name}"

    return path or f"{resource_type}.?"


def extract_binding(elem: dict) -> dict | None:
    b = elem.get("binding")
    if not b:
        return None
    out = {
        "strength": b.get("strength"),
        "valueSet": b.get("valueSet"),
    }
    if b.get("valueSet"):
        # Strip optional |version
        url = b["valueSet"].split("|", 1)[0]
        out["valueSet_id"] = url.rsplit("/", 1)[-1]
    if b.get("description"):
        out["description"] = b["description"]
    return out


def extract_must_support(sd: dict) -> list[dict]:
    """Walk the snapshot, return every element that's mustSupport OR uscdi-requirement.

    De-dupes by (path, slice_name) — we walk snapshot, not differential, to capture
    inherited cardinality. Suppress non-conformance descendants of conformance roots
    that the SD didn't itself flag (FHIR's mustSupport doesn't auto-propagate).
    """
    out: list[dict] = []
    resource_type = sd.get("type", "")
    seen_keys: set[tuple[str, str]] = set()

    for elem in sd.get("snapshot", {}).get("element", []) or []:
        ms = bool(elem.get("mustSupport"))
        uscdi = is_uscdi_requirement(elem)
        if not (ms or uscdi):
            continue

        path = render_path(elem, resource_type)
        slice_name = elem.get("sliceName") or ""
        key = (path, slice_name)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        types = []
        target_profiles = []
        for t in elem.get("type", []) or []:
            code = t.get("code")
            if code:
                types.append(code)
            for p in t.get("profile", []) or []:
                target_profiles.append(p)
            for tp in t.get("targetProfile", []) or []:
                target_profiles.append(tp)

        min_v = elem.get("min", 0)
        max_v = elem.get("max", "*")

        entry: dict = {
            "path": path,
            "cardinality": f"{min_v}..{max_v}",
            "min": min_v,
            "max": max_v,
            "type": types,
            "must_support": ms,
            "uscdi_requirement": uscdi,
            "source_id": elem.get("id", ""),
        }
        if target_profiles:
            entry["target_profiles"] = target_profiles
        if slice_name:
            entry["slice_name"] = slice_name
        binding = extract_binding(elem)
        if binding:
            entry["binding"] = binding
        # Short text from the SD is useful editorial flavor
        if elem.get("short"):
            entry["short"] = elem["short"]
        out.append(entry)

    return out


def extract_search_params(server_capstmt: dict, resource_type: str, profile_url: str) -> list[dict]:
    rest = (server_capstmt.get("rest") or [{}])[0]
    for r in rest.get("resource", []) or []:
        if r.get("type") != resource_type:
            continue
        # Only emit search params for resources where THIS profile is in supportedProfile
        if profile_url not in (r.get("supportedProfile") or []):
            continue
        out = []
        for sp in r.get("searchParam", []) or []:
            expectation = ""
            for ext in sp.get("extension", []) or []:
                if ext.get("url") == EXPECTATION_EXT:
                    expectation = ext.get("valueCode", "")
                    break
            out.append({
                "name": sp.get("name"),
                "type": sp.get("type"),
                "expectation": expectation,  # SHALL | SHOULD | MAY
                "documentation": (sp.get("documentation") or "").strip(),
            })
        return out
    return []


def extract_interactions(server_capstmt: dict, resource_type: str, profile_url: str) -> list[dict]:
    rest = (server_capstmt.get("rest") or [{}])[0]
    for r in rest.get("resource", []) or []:
        if r.get("type") != resource_type:
            continue
        if profile_url not in (r.get("supportedProfile") or []):
            continue
        out = []
        for it in r.get("interaction", []) or []:
            expectation = ""
            for ext in it.get("extension", []) or []:
                if ext.get("url") == EXPECTATION_EXT:
                    expectation = ext.get("valueCode", "")
                    break
            out.append({"code": it.get("code"), "expectation": expectation})
        return out
    return []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--version", default="6.1.0")
    args = ap.parse_args()

    if not IG_DIR.exists():
        sys.exit(f"ERROR: {IG_DIR} not found. Run `python -m tools.fetch_us_core_ig` first.")

    server_cs_path = IG_DIR / "CapabilityStatement-us-core-server.json"
    server_cs = json.loads(server_cs_path.read_text())

    package_meta = json.loads((IG_DIR / "package.json").read_text())
    if package_meta.get("version") != args.version:
        print(
            f"  WARNING: requested version {args.version} but on-disk IG is {package_meta.get('version')}",
            file=sys.stderr,
        )

    profiles_out: list[dict] = []
    for sd_path in sorted(IG_DIR.glob("StructureDefinition-us-core-*.json")):
        sd = json.loads(sd_path.read_text())
        if not is_resource_profile(sd):
            continue
        pid = sd["id"]
        ms = extract_must_support(sd)
        searches = extract_search_params(server_cs, sd["type"], sd["url"])
        interactions = extract_interactions(server_cs, sd["type"], sd["url"])

        profiles_out.append({
            "profile_id": pid,
            "profile_url": sd["url"],
            "profile_version": sd.get("version", args.version),
            "resource_type": sd["type"],
            "priority": PRIORITY.get(pid, "P2"),
            "covers_uscdi_data_classes": USCDI_COVERAGE.get(pid, []),
            "must_support": ms,
            "supported_searches": searches,
            "supported_interactions": interactions,
            "source_structure_definition": str(sd_path.relative_to(REPO_ROOT)),
        })

    output = {
        "version": f"US Core {args.version}",
        "version_url": f"https://hl7.org/fhir/us/core/STU{args.version[:3]}/",
        "publication_date": package_meta.get("date", ""),
        "captured_date": datetime.date.today().isoformat(),
        "fhir_version": package_meta.get("fhirVersions", [None])[0] or "4.0.1",
        "uscdi_version_aligned": "USCDI v3",
        "ig_package": {
            "name": package_meta.get("name"),
            "version": package_meta.get("version"),
            "canonical": package_meta.get("canonical"),
        },
        "notes": (
            "Auto-generated from the official US Core IG package "
            "(packages.fhir.org/hl7.fhir.us.core/<version>) by tools/build_baseline_from_ig.py. "
            "Conformance set per profile = mustSupport=true OR carries the "
            "uscdi-requirement extension flag. Re-run when US Core ships a new minor."
        ),
        "profiles": profiles_out,
    }

    BASELINE_PATH.write_text(json.dumps(output, indent=2) + "\n")

    # Quick stats for the operator
    p0 = [p for p in profiles_out if p["priority"] == "P0"]
    p1 = [p for p in profiles_out if p["priority"] == "P1"]
    p2 = [p for p in profiles_out if p["priority"] == "P2"]
    print(f"wrote {BASELINE_PATH.relative_to(REPO_ROOT)}")
    print(f"  profiles: {len(profiles_out)} total ({len(p0)} P0, {len(p1)} P1, {len(p2)} P2)")
    for prio_name, group in (("P0", p0), ("P1", p1), ("P2", p2)):
        for p in group:
            print(f"    {prio_name}  {p['profile_id']:<48} {len(p['must_support']):>3} ms-elements, {len(p['supported_searches']):>2} searches")
    return 0


if __name__ == "__main__":
    sys.exit(main())
