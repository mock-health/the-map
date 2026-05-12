"""Build a per-endpoint identity index from CMS NPD bulk releases.

Streams the NPD NDJSON.zst files written by `tools.fetch_cms_npd` and emits a
single small JSON index that resolves each FHIR REST endpoint to:
  - canonical address (via normalize_address)
  - NPD endpoint id and name (when present)
  - managing organization (NPI, name, address) when discoverable
  - parent organization (NPI, name) via OrganizationAffiliation hierarchy

NPI extraction trick: NPD encodes the NPI in the Organization resource ID
itself (`Organization-1780113084` → NPI 1780113084). The 10-digit NPI is the
trailing segment; we strip the `Organization-` prefix and validate length. No
need to walk the verbose `identifier[]` array.

Two endpoint→organization linking paths exist in the data — both used:
  1. Forward: `Endpoint.managingOrganization.reference` → Organization
     (~17% of FHIR endpoints in the 2026-05-07 release)
  2. Reverse: `Organization.endpoint[]` → Endpoint
     (~2% of organizations carry these back-references)
The remaining ~81% of FHIR endpoints have no NPD-internal Org link and stay
in the index with `managing_org: null`. Joining the-map's fleet against those
falls back to hostname/name match in `tools.resolve_endpoints_to_npd`.

Parent-organization linking is derived from OrganizationAffiliation, NOT
Organization.partOf. The NDH-style NPD publication leaves `partOf` empty
~100% of the time; the parent-system relationship lives in
OrganizationAffiliation as a bare `organization`↔`participatingOrganization`
edge (no `code` to disambiguate). See `pass_affiliations` for the fan-out
quality heuristic that picks the right parent when an org has many.

Output (default): `data/cms-npd/endpoint-identity-{release_date}.json` —
~30-50 MB JSON, one entry per FHIR REST endpoint in NPD.

Usage:
    python -m tools.build_npd_endpoint_index
    python -m tools.build_npd_endpoint_index --release 2026-05-07
    python -m tools.build_npd_endpoint_index --release 2026-05-07 --out custom.json
"""
from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import zstandard

from tools.luxera_endpoint_discovery import normalize_address

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "cms-npd"

ORG_REF_RE = re.compile(r"^Organization/(Organization-(\d{10}))$")
EP_REF_RE = re.compile(r"^Endpoint/(Endpoint-[A-Za-z0-9-]+)$")
NPI_FROM_ID_RE = re.compile(r"^Organization-(\d{10})$")


def default_storage_dir() -> Path:
    env = os.environ.get("THE_MAP_CMS_NPD_DIR")
    if env:
        return Path(env).expanduser()
    return Path("~/back/data/cms-npd").expanduser()


def find_latest_release(storage_dir: Path) -> str:
    candidates = [p.name for p in storage_dir.iterdir() if p.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", p.name)]
    if not candidates:
        sys.exit(f"ERROR: no release directories under {storage_dir}. Run `python -m tools.fetch_cms_npd` first.")
    return sorted(candidates)[-1]


def stream_ndjson_zst(path: Path) -> Iterator[dict]:
    with open(path, "rb") as f:
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def npi_from_org_id(org_id: str) -> str | None:
    m = NPI_FROM_ID_RE.match(org_id or "")
    return m.group(1) if m else None


def org_id_from_reference(ref: str) -> str | None:
    """`Organization/Organization-1780113084` → `Organization-1780113084`."""
    if not ref:
        return None
    if ref.startswith("Organization/"):
        return ref[len("Organization/"):]
    return None


def ep_id_from_reference(ref: str) -> str | None:
    if not ref:
        return None
    if ref.startswith("Endpoint/"):
        return ref[len("Endpoint/"):]
    return None


def _format_address(addr_block) -> dict | None:
    """Pick a single physical address dict if present, dropping period/extension."""
    if not addr_block:
        return None
    items = addr_block if isinstance(addr_block, list) else [addr_block]
    chosen = None
    for a in items:
        if not isinstance(a, dict):
            continue
        if a.get("use") == "old":
            continue
        chosen = a
        # Prefer "physical" over "postal" if type is set
        if a.get("type") in (None, "physical", "both"):
            break
    if not chosen:
        return None
    return {
        "line": chosen.get("line"),
        "city": chosen.get("city"),
        "state": chosen.get("state"),
        "postalCode": chosen.get("postalCode"),
        "country": chosen.get("country"),
    }


def pass1_endpoints(endpoint_path: Path) -> tuple[list[dict], set[str], dict[str, str]]:
    """Pass 1: filter FHIR REST endpoints and collect referenced org IDs.

    Returns (fhir_endpoints, forward_referenced_org_ids, endpoint_id_to_address_norm).
    """
    fhir_endpoints: list[dict] = []
    forward_org_ids: set[str] = set()
    ep_id_to_addr_norm: dict[str, str] = {}

    print(f"  Pass 1: streaming {endpoint_path.name}")
    t0 = time.monotonic()
    seen = 0
    for rec in stream_ndjson_zst(endpoint_path):
        seen += 1
        ct = (rec.get("connectionType") or {}).get("code")
        if ct != "hl7-fhir-rest":
            continue
        ep_id = rec.get("id") or ""
        address_raw = rec.get("address") or ""
        address_norm = normalize_address(address_raw)
        mgmt_ref = (rec.get("managingOrganization") or {}).get("reference")
        mgmt_org_id = org_id_from_reference(mgmt_ref) if mgmt_ref else None
        if mgmt_org_id:
            forward_org_ids.add(mgmt_org_id)
        fhir_endpoints.append({
            "npd_endpoint_id": ep_id,
            "npd_endpoint_name": rec.get("name"),
            "address_raw": address_raw,
            "address_normalized": address_norm,
            "managing_org_id": mgmt_org_id,
        })
        ep_id_to_addr_norm[ep_id] = address_norm
        if seen % 200000 == 0:
            print(f"    {seen:,} records scanned, {len(fhir_endpoints):,} FHIR REST kept ({int(time.monotonic() - t0)}s)")
    print(f"    done: {seen:,} total records, {len(fhir_endpoints):,} FHIR REST, {len(forward_org_ids):,} referenced orgs ({int(time.monotonic() - t0)}s)")
    return fhir_endpoints, forward_org_ids, ep_id_to_addr_norm


def pass2_organizations(
    org_path: Path,
    forward_org_ids: set[str],
    fhir_endpoint_ids: set[str],
) -> tuple[dict[str, dict], dict[str, str]]:
    """Pass 2: stream Organizations, keep ones referenced by FHIR endpoints OR
    that themselves reference a FHIR endpoint. Returns:
      - org_index: {org_id: {name, npi, address, partOf_id}}
      - backref_endpoint_to_org: {endpoint_id: org_id} from Org.endpoint[]
    """
    org_index: dict[str, dict] = {}
    backref: dict[str, str] = {}

    print(f"  Pass 2: streaming {org_path.name}")
    t0 = time.monotonic()
    seen = 0
    for rec in stream_ndjson_zst(org_path):
        seen += 1
        org_id = rec.get("id") or ""
        keep = org_id in forward_org_ids
        ep_backrefs: list[str] = []
        for ep_ref in rec.get("endpoint") or []:
            ep_id = ep_id_from_reference((ep_ref or {}).get("reference") or "")
            if ep_id and ep_id in fhir_endpoint_ids:
                ep_backrefs.append(ep_id)
        if ep_backrefs:
            keep = True
        if not keep:
            if seen % 500000 == 0:
                print(f"    {seen:,} scanned, {len(org_index):,} kept ({int(time.monotonic() - t0)}s)")
            continue
        part_of_id = org_id_from_reference((rec.get("partOf") or {}).get("reference") or "")
        org_index[org_id] = {
            "name": rec.get("name"),
            "npi": npi_from_org_id(org_id),
            "address": _format_address(rec.get("address")),
            "part_of_id": part_of_id,
        }
        for ep_id in ep_backrefs:
            backref.setdefault(ep_id, org_id)
        if seen % 500000 == 0:
            print(f"    {seen:,} scanned, {len(org_index):,} kept ({int(time.monotonic() - t0)}s)")
    print(f"    done: {seen:,} total orgs scanned, {len(org_index):,} kept, {len(backref):,} backref links ({int(time.monotonic() - t0)}s)")
    return org_index, backref


# National chains (Walgreens, CVS) appear as `organization` (parent side) in
# tens of thousands of affiliations because every retail pharmacy lists the
# parent corp; that's not the "AdventHealth has 22 facilities" parent we want
# for cluster narrative. Health systems peak around ~1500 facilities (HCA is
# the largest in our data with ~180 hospitals + ~1500 ambulatory). 2000 is a
# generous ceiling that excludes the obvious chains.
NATIONAL_CHAIN_FANOUT_THRESHOLD = 2000


def pass_affiliations(aff_path: Path) -> tuple[dict[str, str], Counter]:
    """Pass over OrganizationAffiliation; return (child→best_parent, host_fanout).

    NDH publishes parent-system relationships as bare OrganizationAffiliation
    edges. There's no `code` (we checked: 0/1,086,694 records carry one), no
    `network` (also 0/1,086,694). The only signal is FHIR R4's directional
    semantics:
      - `organization` is "where the role is available"   (parent / host side)
      - `participatingOrganization` is "participating"    (member / child side)

    Empirically the directionality matches: in the 2026-05-07 release host-side
    fan-out peaks at 32,515 (Walgreens) while member-side fan-out peaks at 67.
    That gap (~500×) makes the directionality unambiguous.

    When a member has multiple parents (common — facilities affiliate with
    payers, networks, AND their parent system), prefer the largest fan-out
    parent below the chain threshold. That's the most-system-like option:
    AdventHealth (fan-out 22) beats a peer clinic group (fan-out 3) and is
    correctly preferred over Walgreens (fan-out 32K, suppressed). When no
    candidate is below the threshold we still pick one (smallest chain, just
    in case it's the only signal), but mark the index's coverage stats so
    callers can see how often the heuristic had to fall back.
    """
    print(f"  Pass: streaming {aff_path.name}")
    t0 = time.monotonic()
    host_fanout: Counter = Counter()
    edges: dict[str, set[str]] = {}
    seen = 0
    for rec in stream_ndjson_zst(aff_path):
        seen += 1
        org_ref = (rec.get("organization") or {}).get("reference") or ""
        part_ref = (rec.get("participatingOrganization") or {}).get("reference") or ""
        host_id = org_id_from_reference(org_ref)
        member_id = org_id_from_reference(part_ref)
        if not host_id or not member_id or host_id == member_id:
            continue
        host_fanout[host_id] += 1
        edges.setdefault(member_id, set()).add(host_id)
        if seen % 200000 == 0:
            print(f"    {seen:,} affiliations scanned, {len(edges):,} member orgs with parents ({int(time.monotonic() - t0)}s)")

    def _quality(parent_id: str) -> tuple[int, int]:
        f = host_fanout[parent_id]
        if f > NATIONAL_CHAIN_FANOUT_THRESHOLD:
            # Chain tier: still selectable as last resort; prefer smaller chains.
            return (0, -f)
        return (1, f)

    child_to_parent: dict[str, str] = {}
    for member_id, parents in edges.items():
        best = max(parents, key=lambda p: (_quality(p), p))
        child_to_parent[member_id] = best
    print(f"    done: {seen:,} affiliations, {len(child_to_parent):,} parent edges ({int(time.monotonic() - t0)}s)")
    return child_to_parent, host_fanout


def build_index(release_dir: Path) -> dict:
    endpoint_path = release_dir / "Endpoint.ndjson.zst"
    org_path = release_dir / "Organization.ndjson.zst"
    aff_path = release_dir / "OrganizationAffiliation.ndjson.zst"

    for p in (endpoint_path, org_path, aff_path):
        if not p.exists():
            sys.exit(f"ERROR: required file missing: {p}")

    print(f"Building NPD endpoint identity index from {release_dir}")

    fhir_endpoints, forward_org_ids, _ = pass1_endpoints(endpoint_path)
    fhir_endpoint_ids = {ep["npd_endpoint_id"] for ep in fhir_endpoints}

    # Affiliations first: we need to know which orgs are parents so pass2 can
    # keep their names (parents won't usually be in forward_org_ids — a parent
    # health system rarely IS a FHIR endpoint's managing org directly).
    child_to_parent, _host_fanout = pass_affiliations(aff_path)
    potential_parent_ids = set(child_to_parent.values())

    org_index, backref = pass2_organizations(
        org_path, forward_org_ids | potential_parent_ids, fhir_endpoint_ids
    )

    # Parent-org hierarchy via OrganizationAffiliation. Organization.partOf
    # populates a parent_id for ~7% of orgs in this release but the partOf
    # targets are UUID-style records we don't keep (and that often have no
    # `name`), so the chain reliably yields parent_name=null. We've kept the
    # part_of_id capture in pass2 for forensic interest but no longer wire it
    # into the published index.
    parent_map: dict[str, str] = {}
    for oid in org_index:
        if oid in child_to_parent and child_to_parent[oid] in org_index:
            parent_map[oid] = child_to_parent[oid]

    # Compose output records
    print("  Composing output records")
    out_endpoints: list[dict] = []
    n_with_org = 0
    n_with_npi = 0
    n_with_parent = 0
    by_state: dict[str, int] = {}
    for ep in fhir_endpoints:
        org_id = ep["managing_org_id"] or backref.get(ep["npd_endpoint_id"])
        org = org_index.get(org_id) if org_id else None
        rec = {
            "address_normalized": ep["address_normalized"],
            "address_raw": ep["address_raw"],
            "npd_endpoint_id": ep["npd_endpoint_id"],
            "npd_endpoint_name": ep["npd_endpoint_name"],
        }
        if org:
            n_with_org += 1
            if org["npi"]:
                n_with_npi += 1
            state = (org.get("address") or {}).get("state")
            if state:
                by_state[state] = by_state.get(state, 0) + 1
            rec["managing_org"] = {
                "npd_id": org_id,
                "name": org["name"],
                "npi": org["npi"],
                "address": org["address"],
            }
            parent_id = parent_map.get(org_id) if org_id else None
            if parent_id:
                parent = org_index.get(parent_id)
                rec["managing_org"]["parent_id"] = parent_id
                rec["managing_org"]["parent_name"] = parent["name"] if parent else None
                rec["managing_org"]["parent_npi"] = npi_from_org_id(parent_id)
                if parent and parent.get("name"):
                    n_with_parent += 1
        out_endpoints.append(rec)

    return {
        "release_date": release_dir.name,
        "total_fhir_endpoints": len(out_endpoints),
        "summary": {
            "endpoints_with_org_link": n_with_org,
            "endpoints_with_npi": n_with_npi,
            "endpoints_with_parent_org": n_with_parent,
            "endpoints_orphan": len(out_endpoints) - n_with_org,
            "by_state": dict(sorted(by_state.items(), key=lambda kv: -kv[1])),
        },
        "endpoints": out_endpoints,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir",
        help="Raw NPD storage dir (default: $THE_MAP_CMS_NPD_DIR or ~/back/data/cms-npd)",
    )
    ap.add_argument("--release", help="Release date (YYYY-MM-DD); defaults to most recent under storage dir")
    ap.add_argument(
        "--out",
        help=f"Output JSON path (default: {DEFAULT_OUT_DIR.relative_to(REPO_ROOT)}/endpoint-identity-{{release}}.json)",
    )
    args = ap.parse_args()

    storage_dir = Path(args.dir).expanduser() if args.dir else default_storage_dir()
    release = args.release or find_latest_release(storage_dir)
    release_dir = storage_dir / release

    DEFAULT_OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (DEFAULT_OUT_DIR / f"endpoint-identity-{release}.json")

    index = build_index(release_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(index, indent=2) + "\n")

    s = index["summary"]
    total = index["total_fhir_endpoints"]
    print(f"\nWrote {out_path.relative_to(REPO_ROOT)}")
    print(f"  total FHIR endpoints: {total:,}")
    print(f"  with managing org:    {s['endpoints_with_org_link']:,} ({100 * s['endpoints_with_org_link'] // max(1, total)}%)")
    print(f"  with NPI:             {s['endpoints_with_npi']:,} ({100 * s['endpoints_with_npi'] // max(1, total)}%)")
    print(f"  with parent org:      {s['endpoints_with_parent_org']:,} ({100 * s['endpoints_with_parent_org'] // max(1, total)}%)")
    print(f"  orphan:               {s['endpoints_orphan']:,}")
    top_states = list(s["by_state"].items())[:5]
    if top_states:
        print(f"  top states:           {', '.join(f'{st}={n}' for st, n in top_states)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
