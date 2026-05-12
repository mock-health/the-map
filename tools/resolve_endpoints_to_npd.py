"""Resolve the-map's fleet endpoints to CMS NPD entries.

Joins each Endpoint in `tests/golden/cross-vendor/{stem}-{date}.json` against
the NPD endpoint identity index at `data/cms-npd/endpoint-identity-{date}.json`
and emits a per-vendor overlay at `data/hospital-overlays/{vendor}-npd.json`.

Match strategy (first hit wins):
  1. canonical_url   normalize_address() exact match against NPD's address_normalized
                     → high confidence. Both sides go through the same normalizer.
  2. hostname        same hostname, NPD also has an entry under that host. Picks
                     the first NPD entry with managing_org populated, else any
                     entry. → medium confidence. Useful for tenant-id drift.

Companion to `tools.resolve_endpoints_to_pos`: that resolver attributes a CCN
(authoritative facility certification number) via name+address Jaccard against
the POS hospital catalog. This one attributes an NPI (authoritative provider
identity) via the NPD's published endpoint→organization graph. The two are
complementary — NPD covers Epic/Cerner-on-the-cloud; POS covers MEDITECH and
on-prem deployments NPD never indexed.

Output: `data/hospital-overlays/{vendor}-npd.json`. Re-runnable.

Usage:
    python -m tools.resolve_endpoints_to_npd epic
    python -m tools.resolve_endpoints_to_npd cerner
    python -m tools.resolve_endpoints_to_npd meditech
    python -m tools.resolve_endpoints_to_npd --all
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from tools.luxera_endpoint_discovery import normalize_address

REPO_ROOT = Path(__file__).resolve().parent.parent
NPD_INDEX_DIR = REPO_ROOT / "data" / "cms-npd"
OUT_DIR = REPO_ROOT / "data" / "hospital-overlays"
EHRS_DIR = REPO_ROOT / "ehrs"

VENDORS = ("epic", "cerner", "meditech")


def _hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def load_latest_npd_index() -> tuple[Path, dict]:
    candidates = sorted(NPD_INDEX_DIR.glob("endpoint-identity-*.json"))
    if not candidates:
        sys.exit(
            "ERROR: no NPD index found under data/cms-npd/. Run "
            "`python -m tools.build_npd_endpoint_index` first."
        )
    p = candidates[-1]
    return p, json.loads(p.read_text())


def build_lookups(npd_index: dict) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    """Build (by_address_normalized, by_hostname) lookups from NPD index."""
    by_addr: dict[str, dict] = {}
    by_host: dict[str, list[dict]] = defaultdict(list)
    for entry in npd_index["endpoints"]:
        a = entry.get("address_normalized") or ""
        if a:
            by_addr[a] = entry
        host = _hostname(entry.get("address_raw") or "")
        if host:
            by_host[host].append(entry)
    return by_addr, dict(by_host)


def iter_fleet_endpoints(fleet: dict) -> list[dict]:
    out: list[dict] = []
    for cluster in fleet.get("capstmt_shape_clusters") or []:
        for ep in cluster.get("endpoints") or []:
            out.append({
                "endpoint_id": ep.get("endpoint_id"),
                "address": ep.get("address"),
                "managing_organization_name": ep.get("managing_organization_name"),
                "cluster_id": cluster.get("cluster_id"),
            })
    return out


def resolve_one(ep: dict, by_addr: dict[str, dict], by_host: dict[str, list[dict]]) -> dict | None:
    """Return a match record or None if no match found."""
    addr_raw = ep.get("address") or ""
    addr_norm = normalize_address(addr_raw)

    # Strategy 1: canonical URL match (high confidence)
    if addr_norm and addr_norm in by_addr:
        return _format_match(ep, by_addr[addr_norm], strategy="canonical_url", confidence="high")

    # Strategy 2: hostname registration — the hostname appears in NPD but this
    # specific URL does not. Useful for "this is a known Cerner/Oracle tenant
    # host" assertion but DOES NOT attribute NPI: every tenant on a shared host
    # is a different organization with a different NPI. The match record carries
    # `npi: null` to make this explicit.
    host = _hostname(addr_raw)
    if host and host in by_host:
        return {
            "endpoint_id": ep.get("endpoint_id"),
            "endpoint_address": ep.get("address"),
            "match_strategy": "hostname_registered",
            "confidence": "low",
            "npd_endpoint_id": None,
            "npd_endpoint_name": None,
            "npi": None,
            "org_name_npd": None,
            "parent_org_npi": None,
            "parent_org_name": None,
            "state": None,
            "city": None,
            "postal_code": None,
            "hostname_npd_endpoint_count": len(by_host[host]),
        }

    return None


def _format_match(ep: dict, npd_entry: dict, *, strategy: str, confidence: str) -> dict:
    org = npd_entry.get("managing_org") or {}
    org_address = org.get("address") or {}
    return {
        "endpoint_id": ep.get("endpoint_id"),
        "endpoint_address": ep.get("address"),
        "match_strategy": strategy,
        "confidence": confidence,
        "npd_endpoint_id": npd_entry.get("npd_endpoint_id"),
        "npd_endpoint_name": npd_entry.get("npd_endpoint_name"),
        "npi": org.get("npi"),
        "org_name_npd": org.get("name"),
        "parent_org_npi": org.get("parent_npi"),
        "parent_org_name": org.get("parent_name"),
        "state": org_address.get("state"),
        "city": org_address.get("city"),
        "postal_code": org_address.get("postalCode"),
    }


def resolve_vendor(vendor: str, by_addr: dict, by_host: dict, npd_release: str, npd_index_path: Path) -> dict:
    fleet_path = EHRS_DIR / vendor / "production_fleet.json"
    if not fleet_path.exists():
        sys.exit(f"ERROR: missing {fleet_path}. Run analyze_fleet_drift first.")
    fleet = json.loads(fleet_path.read_text())
    endpoints = iter_fleet_endpoints(fleet)

    matches: list[dict] = []
    by_strategy: dict[str, int] = defaultdict(int)
    n_with_npi = 0
    for ep in endpoints:
        m = resolve_one(ep, by_addr, by_host)
        if m is None:
            continue
        matches.append(m)
        by_strategy[m["match_strategy"]] += 1
        if m.get("npi"):
            n_with_npi += 1

    return {
        "vendor": vendor,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "source_npd_release": npd_release,
        "source_npd_index": str(npd_index_path.relative_to(REPO_ROOT)),
        "summary": {
            "fleet_total": len(endpoints),
            "matched": len(matches),
            "matched_with_npi": n_with_npi,
            "unmatched": len(endpoints) - len(matches),
            "by_strategy": dict(by_strategy),
        },
        "matches": matches,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=VENDORS, help="Single vendor to resolve")
    ap.add_argument("--all", action="store_true", help="Resolve all vendors")
    ap.add_argument("--index", help="Path to NPD endpoint identity index JSON (default: latest under data/cms-npd/)")
    args = ap.parse_args()

    if not args.vendor and not args.all:
        ap.error("specify a vendor or --all")

    if args.index:
        npd_index_path = Path(args.index)
        npd_index = json.loads(npd_index_path.read_text())
    else:
        npd_index_path, npd_index = load_latest_npd_index()

    npd_release = npd_index.get("release_date", "unknown")
    by_addr, by_host = build_lookups(npd_index)
    print(f"NPD release {npd_release}: {len(by_addr):,} canonical URLs, {len(by_host):,} hostnames")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    vendors = list(VENDORS) if args.all else [args.vendor]
    for vendor in vendors:
        result = resolve_vendor(vendor, by_addr, by_host, npd_release, npd_index_path)
        out_path = OUT_DIR / f"{vendor}-npd.json"
        out_path.write_text(json.dumps(result, indent=2) + "\n")
        s = result["summary"]
        total = s["fleet_total"] or 1
        print(f"  {vendor}: matched {s['matched']}/{s['fleet_total']} ({100 * s['matched'] // total}%), "
              f"with NPI {s['matched_with_npi']} ({100 * s['matched_with_npi'] // total}%), "
              f"strategies={s['by_strategy']}")
        print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
