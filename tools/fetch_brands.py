"""Fetch a vendor's SMART STU 2.2 / HTI-1 §170.404 brands & endpoints bundle.

Each ONC-certified EHR must publish a public registry of customer FHIR endpoints.
Three different publication strategies are in use as of 2026:
  - Epic ships a `Bundle` of `Endpoint` only (no `Organization`) at open.epic.com
  - Oracle Health ships separate patient + provider `Bundle`s on GitHub
  - MEDITECH ships a SMART STU 2.2-conformant `Bundle` (Organization + Endpoint)

This tool fetches each, validates it parses, and archives a dated copy under
`tests/golden/cross-vendor/` (the same convention `tools/fetch_capability.py`
uses for sandbox CapStmts). The harvester (`tools/harvest_production_capstmts.py`)
reads from this dated archive.

Usage:
    python -m tools.fetch_brands epic
    python -m tools.fetch_brands cerner            # alias for oracle-health-provider
    python -m tools.fetch_brands oracle-health-patient
    python -m tools.fetch_brands meditech
    python -m tools.fetch_brands --all
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_CROSS_VENDOR = REPO_ROOT / "tests" / "golden" / "cross-vendor"

USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) brands-bundle-fetcher"

# Vendor identifier → (display_label, brands_bundle_url, archive_filename_stem)
BRANDS_SOURCES = {
    "epic": (
        "Epic R4",
        "https://open.epic.com/Endpoints/R4",
        "epic-r4-endpoints",
    ),
    "epic-dstu2": (
        "Epic DSTU2",
        "https://open.epic.com/Endpoints/DSTU2",
        "epic-dstu2-endpoints",
    ),
    "cerner": (
        "Oracle Health (provider)",
        "https://raw.githubusercontent.com/oracle-samples/ignite-endpoints/refs/heads/main/oracle_health_fhir_endpoints/millennium_provider_r4_endpoints.json",
        "oracle-health-provider-r4-endpoints",
    ),
    "oracle-health-patient": (
        "Oracle Health (patient)",
        "https://raw.githubusercontent.com/oracle-samples/ignite-endpoints/refs/heads/main/oracle_health_fhir_endpoints/millennium_patient_r4_endpoints.json",
        "oracle-health-patient-r4-endpoints",
    ),
    "meditech": (
        "MEDITECH",
        "https://fhir-apis.meditech.com/v1/brands",
        "meditech-brands",
    ),
}


def fetch_bundle(url: str) -> dict:
    try:
        import requests
    except ImportError:
        sys.exit("ERROR: pip install requests")
    headers = {
        "Accept": "application/json, application/fhir+json",
        "User-Agent": USER_AGENT,
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


def summarize_bundle(bundle: dict) -> dict:
    by_type: dict[str, int] = {}
    sample_endpoint_address = None
    for entry in bundle.get("entry", []):
        r = entry.get("resource", {}) if isinstance(entry, dict) else {}
        t = r.get("resourceType", "?")
        by_type[t] = by_type.get(t, 0) + 1
        if t == "Endpoint" and not sample_endpoint_address:
            sample_endpoint_address = r.get("address")
    return {
        "resourceType": bundle.get("resourceType"),
        "type": bundle.get("type"),
        "total": bundle.get("total"),
        "entries": len(bundle.get("entry", [])),
        "by_type": by_type,
        "sample_endpoint_address": sample_endpoint_address,
    }


def fetch_one(vendor: str, *, dry_run: bool = False) -> Path | None:
    if vendor not in BRANDS_SOURCES:
        sys.exit(f"ERROR: unknown vendor {vendor!r}; known: {sorted(BRANDS_SOURCES)}")
    label, url, stem = BRANDS_SOURCES[vendor]
    print(f"\nFetching {label} brands bundle from {url}")
    bundle = fetch_bundle(url)
    summary = summarize_bundle(bundle)
    print(f"  resourceType: {summary['resourceType']} | total: {summary['total']} | entries: {summary['entries']}")
    print(f"  by_type: {summary['by_type']}")
    if summary["sample_endpoint_address"]:
        print(f"  sample endpoint: {summary['sample_endpoint_address']}")

    if dry_run:
        print("  (dry-run — not writing)")
        return None

    today = datetime.date.today().isoformat()
    GOLDEN_CROSS_VENDOR.mkdir(parents=True, exist_ok=True)
    out_path = GOLDEN_CROSS_VENDOR / f"{stem}-{today}.json"
    out_path.write_text(json.dumps(bundle, indent=2) + "\n")
    print(f"  archived {out_path.relative_to(REPO_ROOT)}")
    return out_path


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=list(BRANDS_SOURCES))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    targets = list(BRANDS_SOURCES) if args.all else ([args.vendor] if args.vendor else None)
    if not targets:
        ap.error("pass a vendor identifier or --all")

    for v in targets:
        fetch_one(v, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
