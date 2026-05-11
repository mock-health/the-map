"""Fetch a CapabilityStatement directly from an EHR's public sandbox.

This is Phase A primary source. The CapabilityStatement is the authoritative spec —
fetched verbatim, stored in ehrs/{ehr}/CapabilityStatement.json, and used directly
by the rest of the pipeline (no intermediate schema). We also save a dated copy
to tests/golden/{ehr}/ for auditability.

Usage:
    python -m tools.fetch_capability epic
    python -m tools.fetch_capability cerner --base-url=https://fhir-open.cerner.com/r4
    python -m tools.fetch_capability epic --dry-run    # just print, don't write
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"

# Anonymous sandbox base URLs for the weekly `refresh-capstmts` cron.
# Each must return a R4 CapabilityStatement from `${base}/metadata` without auth.
#
# - Cerner / Oracle Health: open sandbox tenant `ec2458f2-…` is the demo tenant
#   Oracle publishes in dev-center docs; `/r4` without a tenant 404s.
# - MEDITECH: aliased to Greenfield — the only stable singular sandbox MEDITECH
#   publishes. MEDITECH Expanse is the 542-customer-endpoint world tracked
#   separately via the brands-bundle harvest, not refreshable on a single URL.
KNOWN_BASES = {
    "epic": "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4",
    "cerner": "https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d",
    "meditech": "https://greenfield-prod-apis.meditech.com/v2/uscore/STU6",
    "meditech-greenfield": "https://greenfield-prod-apis.meditech.com/v2/uscore/STU6",
}


def fetch(url: str) -> dict:
    try:
        import requests
    except ImportError:
        sys.exit("ERROR: pip install requests")
    headers = {
        "Accept": "application/fhir+json",
        # Honest, identifying UA. Some vendor CDN WAFs block default
        # `python-requests/*` with a 403 — that's itself a Map data point we
        # should capture in the overlay.
        "User-Agent": "mockhealth-map/0.1 (+https://mock.health) Phase-A-CapStmt-fetcher",
    }
    r = requests.get(f"{url.rstrip('/')}/metadata", headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr")
    ap.add_argument("--base-url", help="Override the known sandbox base URL")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = args.base_url or KNOWN_BASES.get(args.ehr)
    if not base:
        sys.exit(f"ERROR: no known sandbox URL for {args.ehr}; pass --base-url=...")

    print(f"Fetching CapabilityStatement from {base}/metadata")
    capstmt = fetch(base)

    sw = capstmt.get("software", {})
    print(f"  software: {sw.get('name')} {sw.get('version')} (released {sw.get('releaseDate')})")
    print(f"  fhirVersion: {capstmt.get('fhirVersion')}")
    print(f"  status: {capstmt.get('status')}")
    rest = capstmt.get("rest", [{}])[0]
    print(f"  resources documented: {len(rest.get('resource', []))}")

    if args.dry_run:
        print("(dry-run — not writing)")
        return 0

    today = datetime.date.today().isoformat()
    ehr_dir = EHRS_DIR / args.ehr
    ehr_dir.mkdir(parents=True, exist_ok=True)
    # ensure_ascii=False keeps non-ASCII chars (em-dash, →, etc.) literal in
    # overlay.json narratives instead of escaping them to \uXXXX. JSON parsers
    # round-trip both forms identically; the difference is human readability.
    primary = ehr_dir / "CapabilityStatement.json"
    primary.write_text(json.dumps(capstmt, indent=2, ensure_ascii=False) + "\n")
    print(f"  wrote {primary.relative_to(REPO_ROOT)}")

    golden_dir = GOLDEN_DIR / args.ehr
    golden_dir.mkdir(parents=True, exist_ok=True)
    archive = golden_dir / f"CapabilityStatement-{today}.json"
    archive.write_text(json.dumps(capstmt, indent=2, ensure_ascii=False) + "\n")
    print(f"  archived {archive.relative_to(REPO_ROOT)}")

    overlay_path = ehr_dir / "overlay.json"
    if overlay_path.exists():
        overlay = json.loads(overlay_path.read_text())
        overlay["capability_statement_fetched_date"] = today
        overlay["capability_statement_url"] = f"{base.rstrip('/')}/metadata"
        if "version" in sw:
            overlay["ehr_version_validated"] = f"{sw.get('name', args.ehr.title())} {sw['version']}"
        overlay_path.write_text(json.dumps(overlay, indent=2, ensure_ascii=False) + "\n")
        print(f"  bumped {overlay_path.relative_to(REPO_ROOT)} fetched_date and version")

    return 0


if __name__ == "__main__":
    sys.exit(main())
