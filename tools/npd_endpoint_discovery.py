"""Augment vendor brands bundles with net-new endpoints discovered in CMS NPD.

For each tracked EHR, diffs the latest official (or luxera-augmented) brands
bundle against the NPD endpoint identity index and emits a `*-npd-augmented-
{date}.json` superset that tools.harvest_production_capstmts can consume to
probe net-new endpoints.

Discovery source provenance is preserved on each synthesized Endpoint via
`discovery-source: npd-{release_date}` extensions (mirrors the luxera
discovery pattern).

Per-target host filters limit NPD endpoints to those plausibly served by the
target EHR's tenant infrastructure — without a host filter we'd pull Athena
or eClinicalWorks endpoints into the Epic bundle.

Usage:
    python -m tools.npd_endpoint_discovery --all
    python -m tools.npd_endpoint_discovery --ehr epic
    python -m tools.npd_endpoint_discovery --ehr cerner --dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

from tools.luxera_endpoint_discovery import normalize_address

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_CROSS_VENDOR = REPO_ROOT / "tests" / "golden" / "cross-vendor"
NPD_INDEX_DIR = REPO_ROOT / "data" / "cms-npd"

DISCOVERY_SOURCE_URL = "https://github.com/mock-health/the-map/discovery-source"

# Per-target host filter regex. NPD endpoint URLs are bucketed into a vendor
# slot only if the hostname matches the regex; this is conservative — we'd
# rather miss a stray Epic endpoint on an unrecognized host than pull an
# Athena endpoint into the Epic bundle and corrupt cluster analysis.
HARVEST_TARGETS: list[tuple[str, str, re.Pattern, str | None]] = [
    # (harvest_slug, brands_stem, host_regex, host_filter_for_diff_report)
    (
        "epic",
        "epic-r4-endpoints",
        re.compile(
            r"(?:^|\.)("
            r"epichosted\.com"
            r"|epicstaff\.com"
            r"|epicproxy\."
            r"|fhir\.epic\.com"
            r"|mychart\.com"
            r")",
            re.IGNORECASE,
        ),
        None,
    ),
    (
        "cerner",
        "oracle-health-provider-r4-endpoints",
        re.compile(r"^fhir-ehr\.cerner\.com$", re.IGNORECASE),
        "fhir-ehr.cerner.com",
    ),
    (
        "cerner-patient",
        "oracle-health-patient-r4-endpoints",
        re.compile(r"^fhir-myrecord\.cerner\.com$", re.IGNORECASE),
        "fhir-myrecord.cerner.com",
    ),
    # MEDITECH intentionally omitted — NPD publishes 0 meditech.cloud endpoints
    # as of 2026-05-07 (confirmed empirically). Adding the target would be a
    # no-op and is misleading.
]


# ─────────────────── inputs ──────────────────────────────────────────────────
def load_npd_index() -> tuple[Path, dict]:
    candidates = sorted(NPD_INDEX_DIR.glob("endpoint-identity-*.json"))
    if not candidates:
        sys.exit(
            "ERROR: no NPD endpoint index found. Run "
            "`python -m tools.build_npd_endpoint_index` first."
        )
    p = candidates[-1]
    return p, json.loads(p.read_text())


def latest_input_bundle(stem: str) -> Path:
    """Pick the most recent bundle for `stem`, preferring an already-augmented
    bundle (luxera) over the raw official one — so the NPD augmentation layers
    on top of any prior discovery source.
    """
    all_candidates = sorted(GOLDEN_CROSS_VENDOR.glob(f"{stem}-2*.json"))
    # Don't re-augment a previous NPD output
    all_candidates = [p for p in all_candidates if "npd-augmented" not in p.name]
    if not all_candidates:
        sys.exit(f"ERROR: no brands bundle matching {stem}-*.json under {GOLDEN_CROSS_VENDOR.relative_to(REPO_ROOT)}/")
    return all_candidates[-1]


def load_official_endpoints(brands_path: Path) -> tuple[list[dict], dict, set[str]]:
    bundle = json.loads(brands_path.read_text())
    entries = list(bundle.get("entry") or [])
    normalized: set[str] = set()
    for entry in entries:
        r = (entry or {}).get("resource") or {}
        if r.get("resourceType") != "Endpoint":
            continue
        addr = r.get("address")
        if isinstance(addr, str):
            normalized.add(normalize_address(addr))
    return entries, bundle, normalized


# ─────────────────── augmented-bundle synthesis ──────────────────────────────
def build_endpoint_resource(npd_entry: dict, npd_release: str) -> dict:
    """Synthesize a minimal FHIR Endpoint resource from an NPD record. Only
    fields the harvester reads (address, id, name, managingOrganization) are
    populated meaningfully. The NPD-derived NPI rides along on an extension
    so downstream provenance is preserved without polluting standard fields.
    """
    addr = npd_entry["address_raw"]
    npd_id = npd_entry["npd_endpoint_id"] or normalize_address(addr).replace("://", "_").replace("/", "_")
    org = npd_entry.get("managing_org") or {}
    res = {
        "resourceType": "Endpoint",
        "id": f"npd-{npd_id}",
        "status": "active",
        "connectionType": {
            "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
            "code": "hl7-fhir-rest",
        },
        "name": org.get("name") or npd_entry.get("npd_endpoint_name") or None,
        "address": addr,
        "payloadType": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/endpoint-payload-type",
                        "code": "any",
                    }
                ]
            }
        ],
        "extension": [
            {"url": DISCOVERY_SOURCE_URL, "valueCode": f"npd-{npd_release}"},
            {"url": f"{DISCOVERY_SOURCE_URL}/npd-endpoint-id", "valueString": npd_entry["npd_endpoint_id"] or ""},
        ],
    }
    if org.get("npi"):
        res["extension"].append({"url": f"{DISCOVERY_SOURCE_URL}/npi", "valueString": org["npi"]})
    return res


def assemble_augmented_bundle(
    *,
    official_bundle: dict,
    new_endpoints: list[dict],
    captured_date: str,
    harvest_slug: str,
    npd_release: str,
) -> dict:
    return {
        "resourceType": "Bundle",
        "type": official_bundle.get("type") or "collection",
        "id": f"{harvest_slug}-npd-augmented-{captured_date}",
        "timestamp": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "meta": {
            "tag": [
                {
                    "system": DISCOVERY_SOURCE_URL,
                    "code": "npd-augmented",
                    "display": f"Brands bundle augmented with net-new endpoints discovered via CMS NPD release {npd_release} on {captured_date}",
                }
            ]
        },
        "entry": list(official_bundle.get("entry") or []) + [{"resource": r} for r in new_endpoints],
    }


# ─────────────────── per-target processing ───────────────────────────────────
def process_target(
    *,
    harvest_slug: str,
    brands_stem: str,
    host_regex: re.Pattern,
    npd_index: dict,
    captured_date: str,
    dry_run: bool,
) -> dict:
    print(f"\n→ {harvest_slug}: brands_stem={brands_stem}")
    official_path = latest_input_bundle(brands_stem)
    _, official_bundle, official_addrs = load_official_endpoints(official_path)
    print(f"    official bundle: {official_path.relative_to(REPO_ROOT)} ({len(official_addrs)} endpoints)")

    npd_release = npd_index.get("release_date", "unknown")
    npd_matched: list[dict] = []
    for e in npd_index["endpoints"]:
        host = (urlparse(e.get("address_raw") or "").hostname or "").lower()
        if host and host_regex.search(host):
            npd_matched.append(e)
    print(f"    NPD matched by host filter: {len(npd_matched)}")

    npd_norm_to_entry: dict[str, dict] = {}
    for e in npd_matched:
        n = e.get("address_normalized")
        if n and n not in npd_norm_to_entry:
            npd_norm_to_entry[n] = e
    npd_addrs = set(npd_norm_to_entry.keys())

    overlap = official_addrs & npd_addrs
    net_new = npd_addrs - official_addrs
    only_in_official = official_addrs - npd_addrs
    print(f"    overlap: {len(overlap)} | net-new (NPD-only): {len(net_new)} | only-in-official: {len(only_in_official)}")

    new_endpoint_resources = [
        build_endpoint_resource(npd_norm_to_entry[n], npd_release) for n in sorted(net_new)
    ]

    diff_report = {
        "harvest_slug": harvest_slug,
        "captured_date": captured_date,
        "npd_release": npd_release,
        "input_bundle": str(official_path.relative_to(REPO_ROOT)),
        "input_endpoint_count": len(official_addrs),
        "npd_matched_count": len(npd_addrs),
        "overlap_count": len(overlap),
        "net_new_count": len(net_new),
        "only_in_official_count": len(only_in_official),
        "sample_net_new": [
            {
                "address": npd_norm_to_entry[n]["address_raw"],
                "name": (npd_norm_to_entry[n].get("managing_org") or {}).get("name"),
                "npi": (npd_norm_to_entry[n].get("managing_org") or {}).get("npi"),
                "state": ((npd_norm_to_entry[n].get("managing_org") or {}).get("address") or {}).get("state"),
            }
            for n in sorted(net_new)[:20]
        ],
        "sample_only_in_official": sorted(only_in_official)[:20],
    }

    if dry_run:
        print("    [dry-run] not writing augmented bundle")
        return diff_report

    augmented = assemble_augmented_bundle(
        official_bundle=official_bundle,
        new_endpoints=new_endpoint_resources,
        captured_date=captured_date,
        harvest_slug=harvest_slug,
        npd_release=npd_release,
    )

    out_bundle = GOLDEN_CROSS_VENDOR / f"{brands_stem}-npd-augmented-{captured_date}.json"
    out_diff = GOLDEN_CROSS_VENDOR / f"{brands_stem}-npd-augmented-{captured_date}.diff.json"
    out_bundle.write_text(json.dumps(augmented, indent=2) + "\n")
    out_diff.write_text(json.dumps(diff_report, indent=2) + "\n")
    print(f"    wrote {out_bundle.relative_to(REPO_ROOT)} (entries={len(augmented['entry'])})")
    print(f"    wrote {out_diff.relative_to(REPO_ROOT)}")
    return diff_report


# ─────────────────── CLI ────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--ehr", choices=["epic", "cerner"], help="Restrict to one EHR. cerner runs both provider+patient slugs.")
    ap.add_argument("--all", action="store_true", help="Process all configured targets (epic, cerner provider, cerner patient).")
    ap.add_argument("--dry-run", action="store_true", help="Compute diff but don't write augmented bundle (still writes diff report).")
    ap.add_argument("--captured-date", default=dt.date.today().isoformat(), help="Date stamp on output filenames (default: today).")
    args = ap.parse_args()

    if not args.ehr and not args.all:
        ap.error("must pass --ehr {epic|cerner} or --all")

    targets = HARVEST_TARGETS
    if args.ehr == "cerner":
        targets = [t for t in HARVEST_TARGETS if t[0] in {"cerner", "cerner-patient"}]
    elif args.ehr == "epic":
        targets = [t for t in HARVEST_TARGETS if t[0] == "epic"]

    _, npd_index = load_npd_index()
    print(f"NPD discovery — release={npd_index.get('release_date')} captured_date={args.captured_date} dry_run={args.dry_run}")
    GOLDEN_CROSS_VENDOR.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for harvest_slug, brands_stem, host_regex, _host_filter in targets:
        try:
            r = process_target(
                harvest_slug=harvest_slug,
                brands_stem=brands_stem,
                host_regex=host_regex,
                npd_index=npd_index,
                captured_date=args.captured_date,
                dry_run=args.dry_run,
            )
            results.append(r)
        except Exception as e:
            print(f"    ERROR processing {harvest_slug}: {e}", file=sys.stderr)
            results.append({"harvest_slug": harvest_slug, "error": str(e)})

    print("\n=== summary ===")
    for r in results:
        if "error" in r:
            print(f"  {r['harvest_slug']:<18} ERROR: {r['error']}")
        else:
            print(
                f"  {r['harvest_slug']:<18} input={r['input_endpoint_count']:>5}"
                f"  npd={r['npd_matched_count']:>5}"
                f"  overlap={r['overlap_count']:>5}"
                f"  net_new={r['net_new_count']:>5}"
                f"  only_in_official={r['only_in_official_count']:>5}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
