"""Discover net-new EHR endpoints from Luxera's FHIR Directory and emit
augmented brands bundles for harvest.

Luxera (https://fhir-api.luxera.io) maintains a daily-refreshed census of public
FHIR endpoints across 30 ONC-certified vendors. For Epic, Oracle Health/Cerner,
and MEDITECH, their catalog complements the HTI-1 §170.404 vendor brands bundles:
where the official bundles miss long-tail customer endpoints, Luxera often has
them.

This tool is **discovery-only**: we pull endpoint URLs from Luxera, diff against
the most recent official brands bundle, and emit a strict superset bundle that
the existing tools.harvest_production_capstmts can consume. Luxera's pre-probed
CapabilityStatements are NOT mirrored — every endpoint is re-probed by us so
published evidence remains first-party (and respects Luxera's no-redistribute
clause for raw data).

Outputs (per harvest-input slug):
    tests/golden/cross-vendor/{stem}-luxera-augmented-{YYYY-MM-DD}.json
        FHIR Bundle: official entries verbatim + net-new Endpoint resources from
        Luxera tagged with extension {discovery-source: luxera-api-v1}.
    tests/golden/cross-vendor/{stem}-luxera-augmented-{YYYY-MM-DD}.diff.json
        Counts + sample of net-new addresses for human review.

Usage:
    python -m tools.luxera_endpoint_discovery --probe          # confirm vendor IDs
    python -m tools.luxera_endpoint_discovery --all            # emit all four bundles
    python -m tools.luxera_endpoint_discovery --ehr meditech
    python -m tools.luxera_endpoint_discovery --ehr cerner --dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_CROSS_VENDOR = REPO_ROOT / "tests" / "golden" / "cross-vendor"

LUXERA_API_BASE = "https://fhir-api.luxera.io"
DISCOVERY_SOURCE_URL = "https://github.com/mock-health/the-map/discovery-source"
USER_AGENT = (
    "mockhealth-map/0.1 (+https://mock.health; nate@mock.health) "
    "luxera-discovery"
)
TIMEOUT_SECONDS = 30
PAGE_SIZE = 100  # Luxera caps at limit<=100 per validation rule

# (harvest_slug, luxera_vendor_id, official_brands_stem, host_filter_or_None)
# host_filter selects which Luxera rows belong to this slug. None = take all
# rows for the vendor. For multi-bundle vendors (Cerner) we split rows between
# provider (fhir-ehr.cerner.com) and patient (fhir-myrecord.cerner.com) tiers.
HARVEST_TARGETS: list[tuple[str, str, str, str | None]] = [
    ("epic",          "epic",     "epic-r4-endpoints",                    None),
    ("cerner",        "cerner",   "oracle-health-provider-r4-endpoints",  "fhir-ehr.cerner.com"),
    ("cerner-patient","cerner",   "oracle-health-patient-r4-endpoints",   "fhir-myrecord.cerner.com"),
    ("meditech",      "meditech", "meditech-brands",                      None),
]


# ─────────────────── HTTP with rate-limit awareness ──────────────────────────
def _maybe_throttle(resp: requests.Response) -> None:
    """If Luxera's RateLimit-Remaining header signals we're close to the cap,
    sleep until reset. Headers exposed: RateLimit-Limit, RateLimit-Remaining,
    RateLimit-Reset (epoch seconds)."""
    remaining = resp.headers.get("ratelimit-remaining") or resp.headers.get("RateLimit-Remaining")
    reset = resp.headers.get("ratelimit-reset") or resp.headers.get("RateLimit-Reset")
    try:
        rem = int(remaining) if remaining is not None else None
        rst = int(reset) if reset is not None else None
    except ValueError:
        return
    if rem is None or rem > 2:
        return
    if rst is None:
        time.sleep(2.0)
        return
    sleep_for = max(0.0, rst - time.time()) + 0.5
    if sleep_for > 0:
        print(f"    rate-limit headroom low (remaining={rem}); sleeping {sleep_for:.1f}s", file=sys.stderr)
        time.sleep(min(sleep_for, 65.0))


def _luxera_get(path: str, params: dict | None = None) -> dict:
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
    api_key = os.environ.get("LUXERA_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    url = f"{LUXERA_API_BASE}{path}"
    for attempt in range(3):
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT_SECONDS)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After", "5")
            try:
                delay = max(1.0, min(float(retry_after), 60.0))
            except ValueError:
                delay = 5.0
            print(f"    HTTP 429 from Luxera; sleeping {delay}s", file=sys.stderr)
            time.sleep(delay)
            continue
        r.raise_for_status()
        body = r.json()
        if not body.get("success", True):
            raise RuntimeError(f"Luxera returned success=false for {url}: {body}")
        _maybe_throttle(r)
        return body
    raise RuntimeError(f"Luxera repeatedly rate-limited at {url}")


# ─────────────────── Luxera API ──────────────────────────────────────────────
def probe_vendors() -> dict[str, dict]:
    """Returns {vendor_id: {name, endpointCount}} for every vendor Luxera tracks."""
    body = _luxera_get("/api/v1/vendors")
    return {v["id"]: v for v in body["data"]}


def list_endpoints(vendor_id: str, fhir_version: str = "R4"):
    """Generator yielding every endpoint Luxera has for (vendor, fhir_version).
    Pages with limit=PAGE_SIZE; honors meta.total."""
    page = 1
    while True:
        body = _luxera_get(
            "/api/v1/endpoints",
            params={
                "vendor": vendor_id,
                "fhirVersion": fhir_version,
                "page": page,
                "limit": PAGE_SIZE,
            },
        )
        rows = body.get("data") or []
        if not rows:
            return
        yield from rows
        meta = body.get("meta") or {}
        total = int(meta.get("total") or 0)
        if page * PAGE_SIZE >= total:
            return
        page += 1


# ─────────────────── URL normalization + diff ────────────────────────────────
def normalize_address(addr: str) -> str:
    """Canonical form for URL identity. Lowercases host, drops default ports,
    strips query/fragment, strips trailing slash, collapses `//` in path. Two
    URLs that mean the same FHIR base produce identical normalized strings."""
    if not addr:
        return ""
    p = urlparse(addr.strip())
    if not p.scheme or not p.hostname:
        return addr.strip().lower().rstrip("/")
    host = p.hostname.lower()
    port = p.port
    default = (p.scheme == "https" and port == 443) or (p.scheme == "http" and port == 80)
    netloc = host if (port is None or default) else f"{host}:{port}"
    path = p.path or ""
    # collapse repeated slashes (but keep leading)
    while "//" in path:
        path = path.replace("//", "/")
    path = path.rstrip("/")
    return f"{p.scheme}://{netloc}{path}"


def load_official_endpoints(brands_path: Path) -> tuple[list[dict], dict]:
    """Returns (existing_entries_verbatim, normalized_address_set)."""
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
    return entries, {"normalized_addresses": normalized, "bundle": bundle}


def latest_official_bundle(stem: str) -> Path:
    candidates = sorted(GOLDEN_CROSS_VENDOR.glob(f"{stem}-2*.json"))
    candidates = [p for p in candidates if "luxera-augmented" not in p.name]
    if not candidates:
        sys.exit(f"ERROR: no official brands bundle matching {stem}-*.json under {GOLDEN_CROSS_VENDOR.relative_to(REPO_ROOT)}/")
    return candidates[-1]


# ─────────────────── augmented-bundle synthesis ──────────────────────────────
def build_endpoint_resource(luxera_row: dict) -> dict:
    """Synthesize a minimal-but-valid FHIR Endpoint resource from a Luxera row.
    Carries a discovery-source extension so downstream provenance is preserved.
    Only fields the harvester reads (address, id, name, managingOrganization)
    are populated meaningfully; the rest is FHIR-shape-correct boilerplate."""
    addr = luxera_row["url"]
    luxera_id = luxera_row.get("id") or normalize_address(addr).replace("://", "_").replace("/", "_")
    return {
        "resourceType": "Endpoint",
        "id": f"luxera-{luxera_id}",
        "status": "active",
        "connectionType": {
            "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
            "code": "hl7-fhir-rest",
        },
        "name": luxera_row.get("organizationName") or None,
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
            {"url": DISCOVERY_SOURCE_URL, "valueCode": "luxera-api-v1"},
            {
                "url": f"{DISCOVERY_SOURCE_URL}/luxera-id",
                "valueString": luxera_row.get("id") or "",
            },
            {
                "url": f"{DISCOVERY_SOURCE_URL}/luxera-status",
                "valueCode": luxera_row.get("status") or "unknown",
            },
        ],
    }


def assemble_augmented_bundle(
    *,
    official_bundle: dict,
    new_endpoints: list[dict],
    captured_date: str,
    harvest_slug: str,
) -> dict:
    """Return a FHIR Bundle = official entries verbatim + net-new Endpoint
    entries appended (each with discovery-source extension). Top-level Bundle
    metadata (resourceType, type) preserved from the official bundle."""
    out = {
        "resourceType": "Bundle",
        "type": official_bundle.get("type") or "collection",
        "id": f"{harvest_slug}-luxera-augmented-{captured_date}",
        "timestamp": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "meta": {
            "tag": [
                {
                    "system": DISCOVERY_SOURCE_URL,
                    "code": "luxera-augmented",
                    "display": f"Official brands bundle augmented with net-new endpoints discovered via Luxera FHIR Directory on {captured_date}",
                }
            ]
        },
        "entry": list(official_bundle.get("entry") or []),
    }
    for r in new_endpoints:
        out["entry"].append({"resource": r})
    return out


# ─────────────────── per-target processing ───────────────────────────────────
def process_target(
    *,
    harvest_slug: str,
    luxera_vendor: str,
    brands_stem: str,
    host_filter: str | None,
    captured_date: str,
    dry_run: bool,
) -> dict:
    print(f"\n→ {harvest_slug}: querying Luxera vendor={luxera_vendor} host_filter={host_filter or '(none)'}")
    official_path = latest_official_bundle(brands_stem)
    _official_entries, official_meta = load_official_endpoints(official_path)
    official_addrs: set[str] = official_meta["normalized_addresses"]
    print(f"    official bundle: {official_path.relative_to(REPO_ROOT)} ({len(official_addrs)} endpoints)")

    luxera_rows: list[dict] = []
    for row in list_endpoints(luxera_vendor):
        url = row.get("url") or ""
        if not url:
            continue
        if host_filter:
            host = (urlparse(url).hostname or "").lower()
            if host != host_filter:
                continue
        luxera_rows.append(row)
    print(f"    luxera matched: {len(luxera_rows)} rows")

    luxera_norm_to_row: dict[str, dict] = {}
    for row in luxera_rows:
        n = normalize_address(row["url"])
        if n and n not in luxera_norm_to_row:
            luxera_norm_to_row[n] = row
    luxera_addrs = set(luxera_norm_to_row.keys())

    overlap = official_addrs & luxera_addrs
    net_new_norm = luxera_addrs - official_addrs
    only_in_official = official_addrs - luxera_addrs

    print(f"    overlap: {len(overlap)} | net-new (luxera-only): {len(net_new_norm)} | only-in-official: {len(only_in_official)}")

    new_endpoint_resources = [
        build_endpoint_resource(luxera_norm_to_row[n]) for n in sorted(net_new_norm)
    ]

    diff_report = {
        "harvest_slug": harvest_slug,
        "luxera_vendor_id": luxera_vendor,
        "host_filter": host_filter,
        "captured_date": captured_date,
        "official_bundle": str(official_path.relative_to(REPO_ROOT)),
        "official_endpoint_count": len(official_addrs),
        "luxera_matched_count": len(luxera_addrs),
        "overlap_count": len(overlap),
        "net_new_count": len(net_new_norm),
        "only_in_official_count": len(only_in_official),
        "sample_net_new": [
            {"address": luxera_norm_to_row[n]["url"], "organization": luxera_norm_to_row[n].get("organizationName")}
            for n in sorted(net_new_norm)[:20]
        ],
        "sample_only_in_official": sorted(only_in_official)[:20],
    }

    if dry_run:
        print("    [dry-run] not writing augmented bundle")
        return diff_report

    augmented = assemble_augmented_bundle(
        official_bundle=official_meta["bundle"],
        new_endpoints=new_endpoint_resources,
        captured_date=captured_date,
        harvest_slug=harvest_slug,
    )

    out_bundle = GOLDEN_CROSS_VENDOR / f"{brands_stem}-luxera-augmented-{captured_date}.json"
    out_diff = GOLDEN_CROSS_VENDOR / f"{brands_stem}-luxera-augmented-{captured_date}.diff.json"
    out_bundle.write_text(json.dumps(augmented, indent=2) + "\n")
    out_diff.write_text(json.dumps(diff_report, indent=2) + "\n")
    print(f"    wrote {out_bundle.relative_to(REPO_ROOT)} (entries={len(augmented['entry'])})")
    print(f"    wrote {out_diff.relative_to(REPO_ROOT)}")
    return diff_report


# ─────────────────── CLI ────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--ehr", choices=["epic", "cerner", "meditech"], help="Restrict to one EHR (cerner runs both provider+patient slugs).")
    ap.add_argument("--all", action="store_true", help="Process all configured targets.")
    ap.add_argument("--probe", action="store_true", help="Hit /api/v1/vendors, print Luxera's vendor table, and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Compute diff but don't write augmented bundle (still writes diff report).")
    ap.add_argument("--captured-date", default=dt.date.today().isoformat(), help="Date stamp on output filenames (default: today).")
    args = ap.parse_args()

    if args.probe:
        vendors = probe_vendors()
        print(f"Luxera tracks {len(vendors)} vendors:\n")
        rows = sorted(vendors.values(), key=lambda v: -int(v.get("endpointCount") or 0))
        for v in rows:
            mark = " ◀ in scope" if v["id"] in {"epic", "cerner", "meditech"} else ""
            print(f"  {v['id']:<20} {int(v.get('endpointCount') or 0):>7} endpoints  {v['name']}{mark}")
        return 0

    if not args.ehr and not args.all:
        ap.error("must pass --ehr {epic|cerner|meditech}, --all, or --probe")

    targets = HARVEST_TARGETS
    if args.ehr:
        if args.ehr == "cerner":
            targets = [t for t in HARVEST_TARGETS if t[0] in {"cerner", "cerner-patient"}]
        else:
            targets = [t for t in HARVEST_TARGETS if t[0] == args.ehr]

    print(f"Luxera discovery — captured_date={args.captured_date} dry_run={args.dry_run}")
    GOLDEN_CROSS_VENDOR.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for harvest_slug, luxera_vendor, brands_stem, host_filter in targets:
        try:
            r = process_target(
                harvest_slug=harvest_slug,
                luxera_vendor=luxera_vendor,
                brands_stem=brands_stem,
                host_filter=host_filter,
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
                f"  {r['harvest_slug']:<18} official={r['official_endpoint_count']:>5}"
                f"  luxera={r['luxera_matched_count']:>5}"
                f"  overlap={r['overlap_count']:>5}"
                f"  net_new={r['net_new_count']:>5}"
                f"  only_in_official={r['only_in_official_count']:>5}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
