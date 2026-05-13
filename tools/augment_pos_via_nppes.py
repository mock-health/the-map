"""Tier-3 POS augmentation — use NPPES-resolved addresses to find more CCNs.

The standard POS resolver (`tools.resolve_endpoints_to_pos`) needs a state +
zip5 hint to filter POS candidates before name-Jaccard scoring. That hint
comes from the brands-bundle Organization.address — but Cerner's patient
bundle and many ambulatory Epic endpoints publish Endpoint resources with
no Organization (or with empty addresses), so the deterministic resolver
skips them entirely.

This tool takes the *second pass*: for every fleet endpoint that already
has an NPPES-resolved NPI but no CCN, look up the NPI's practice address
in the NPPES org index (state, zip5, normalized name), then run the same
Jaccard match against the POS catalog. When the score clears the
auto-accept threshold, attribute the CCN.

Output: `data/hospital-overrides/{vendor}-pos-nppes-augmented.json`. The
analyze_fleet_drift overlay loader picks this up as a second POS layer
(after `{vendor}-pos.json`).

Why a separate file rather than mutating the existing pos.json:
  - re-running `resolve_endpoints_to_pos` overwrites pos.json and would
    silently drop these matches; keeping them in a sibling file makes the
    provenance explicit.
  - the `verified_via` for these matches is "cms_pos_via_nppes_address"
    — a distinct citation chain from "cms_pos_csv".

Usage:
    python -m tools.augment_pos_via_nppes cerner
    python -m tools.augment_pos_via_nppes --all
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from collections import defaultdict
from pathlib import Path

from tools.resolve_endpoints_to_pos import (
    CANDIDATE_FLOOR,
    THRESH_STATE_ONLY,
    THRESH_STATE_ZIP5,
    jaccard,
    load_latest_catalog,
    name_tokens,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
NPPES_OVERLAY_DIR = REPO_ROOT / "data" / "hospital-overlays"
NPPES_ORG_DIR = REPO_ROOT / "data" / "cms-nppes"
POS_OUT_DIR = REPO_ROOT / "data" / "hospital-overrides"
EHRS_DIR = REPO_ROOT / "ehrs"

VENDORS = ("epic", "cerner", "meditech")


def _load_nppes_overlay(vendor: str) -> dict[str, dict]:
    """Index NPPES overlay matches by endpoint_address — same key shape the
    analyze_fleet_drift overlay loader uses, so this aligns naturally."""
    p = NPPES_OVERLAY_DIR / f"{vendor}-nppes.json"
    if not p.exists():
        return {}
    payload = json.loads(p.read_text())
    out: dict[str, dict] = {}
    for m in payload.get("matches") or []:
        addr = m.get("endpoint_address") or ""
        if addr:
            out[addr] = m
    return out


def _load_pos_overlay(vendor: str) -> dict[str, dict]:
    """Index existing pos overlay by endpoint_address for "already has CCN" check."""
    p = POS_OUT_DIR / f"{vendor}-pos.json"
    if not p.exists():
        return {}
    payload = json.loads(p.read_text())
    out: dict[str, dict] = {}
    for e in payload.get("endpoints") or []:
        addr = e.get("endpoint_address") or ""
        if addr:
            out[addr] = e
    return out


def _load_needed_nppes_org_records(needed_npis: set[str]) -> dict[str, dict]:
    """Stream NPPES orgs.jsonl, keep only the NPIs we need.

    NPPES orgs.jsonl is ~440MB / 1.9M rows; we typically need a few hundred
    to a few thousand specific NPIs. Streaming filter avoids loading the
    whole file into a 380MB dict.
    """
    candidates = sorted(NPPES_ORG_DIR.glob("orgs-*.jsonl"))
    if not candidates:
        sys.exit(
            "ERROR: no NPPES orgs index found under data/cms-nppes/. Run "
            "`python -m tools.build_nppes_index` first."
        )
    p = candidates[-1]
    out: dict[str, dict] = {}
    with p.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("_header"):
                continue
            if rec["npi"] in needed_npis:
                out[rec["npi"]] = rec
                if len(out) == len(needed_npis):
                    break
    return out


def _iter_fleet_endpoints(vendor: str):
    """Yield {endpoint_id, endpoint_address, managing_organization_name} from
    ehrs/{vendor}/production_fleet.json. We walk this rather than the brands
    bundle so post-augmentation NPPES NPIs are visible."""
    fleet_path = EHRS_DIR / vendor / "production_fleet.json"
    if not fleet_path.exists():
        sys.exit(f"ERROR: {fleet_path} not found. Run analyze_fleet_drift first.")
    fleet = json.loads(fleet_path.read_text())
    for cluster in fleet.get("capstmt_shape_clusters") or []:
        for ep in cluster.get("endpoints") or []:
            yield {
                "endpoint_id": ep.get("endpoint_id"),
                "endpoint_address": ep.get("address"),
                "managing_organization_name": ep.get("managing_organization_name"),
                "fleet_npi": ep.get("npi"),
                "fleet_ccn": ep.get("ccn"),
            }


def augment_vendor(vendor: str, pos_idx: dict, today: str) -> dict:
    print(f"\n[{vendor}] augmenting POS via NPPES-resolved addresses")

    nppes_overlay = _load_nppes_overlay(vendor)
    pos_overlay = _load_pos_overlay(vendor)
    print(f"  NPPES overlay: {len(nppes_overlay)} matched endpoints")
    print(f"  POS overlay:   {len(pos_overlay)} rows already resolved")

    # Phase 1: pick endpoints that have NPPES NPI and aren't already covered
    # by the primary POS resolver (deterministic + manual + LLM). We consult
    # the pos overlay's CCN — not the fleet's CCN — because the fleet's CCN
    # might itself have come from a prior augment run, in which case skipping
    # would flush our own output on re-execution. (Hierarchy: pos overlay
    # wins over pos-nppes-augmented in analyze, so augmented CCNs for
    # already-pos-resolved endpoints would be ignored anyway.)
    candidates: list[dict] = []
    needed_npis: set[str] = set()
    for ep in _iter_fleet_endpoints(vendor):
        addr = ep.get("endpoint_address") or ""
        if pos_overlay.get(addr, {}).get("ccn"):
            continue
        npi = ep.get("fleet_npi")
        if not npi:
            continue
        # The fleet's NPI may have come from NPD or NPPES — for this Tier-3
        # join we want the NPPES org address regardless of which source
        # attributed the NPI, so look up NPPES orgs by NPI directly.
        candidates.append({**ep, "npi": npi})
        needed_npis.add(npi)
    print(f"  fleet endpoints needing CCN: {len(candidates)} (distinct NPIs: {len(needed_npis)})")

    if not candidates:
        print("  nothing to do — no NPI-having, CCN-missing endpoints")
        return {"endpoints": [], "summary": {"endpoints_total": 0, "matched": 0, "candidates": 0}}

    # Phase 2: pull NPPES org records for those NPIs.
    print("  loading NPPES org records for needed NPIs (streaming)...")
    org_by_npi = _load_needed_nppes_org_records(needed_npis)
    print(f"  resolved {len(org_by_npi)}/{len(needed_npis)} NPIs in NPPES org index")

    # Phase 3: for each candidate, look up POS by (state, zip5) → Jaccard name.
    out_rows: list[dict] = []
    matched_unique = 0
    matched_multi = 0
    no_pos_candidates = 0
    no_nppes_org = 0
    for ep in candidates:
        org = org_by_npi.get(ep["npi"])
        if not org:
            no_nppes_org += 1
            continue
        state = (org.get("state") or "").upper()
        zip5 = (org.get("postal") or "")[:5]
        # Prefer the NPPES org name for matching POS; fall back to the
        # endpoint's managing_organization_name. They're often the same
        # post-normalization but NPPES is the more authoritative legal name.
        join_name = org.get("name") or ep.get("managing_organization_name") or ""

        # POS candidate pool: (state, zip5) first, then (state,) fallback for
        # nearby zip mismatches.
        pool: list[dict] = []
        if state and zip5:
            pool = pos_idx["by_state_zip"].get((state, zip5), [])
        if not pool and state:
            pool = pos_idx["by_state"].get(state, [])

        if not pool:
            no_pos_candidates += 1
            continue

        # Score with same name-token Jaccard the deterministic resolver uses.
        qt = name_tokens(join_name)
        if not qt:
            no_pos_candidates += 1
            continue
        scored = sorted(
            ((jaccard(qt, name_tokens(h["name"])), h) for h in pool),
            key=lambda x: -x[0],
        )
        # Drop everything below the candidate floor.
        scored = [(s, h) for s, h in scored if s >= CANDIDATE_FLOOR]
        if not scored:
            no_pos_candidates += 1
            continue

        top_score, top_h = scored[0]
        threshold = THRESH_STATE_ZIP5 if zip5 else THRESH_STATE_ONLY
        # If the top score clears the threshold AND no other candidate is
        # within 0.1 of it, we auto-accept. Else we surface candidates only.
        runner_up = scored[1][0] if len(scored) > 1 else 0.0
        if top_score >= threshold and (top_score - runner_up) >= 0.05:
            row = {
                "endpoint_id": ep["endpoint_id"],
                "endpoint_address": ep["endpoint_address"],
                "ccn": top_h["ccn"],
                "name_pos": top_h["name"],
                "city": top_h["city"],
                "state": top_h["state"],
                "zip5": top_h["zip"],
                "category_code": top_h.get("category_code"),
                "category_label": top_h.get("category_label"),
                "fac_subtype_code": top_h.get("fac_subtype_code"),
                "score": round(top_score, 4),
                "match_strategy": "name_jaccard_via_nppes_address",
                "verified_via": "cms_pos_via_nppes_address",
                "verified_date": today,
                "nppes_npi": ep["npi"],
                "nppes_name": org.get("name"),
                "nppes_state": state,
                "nppes_zip5": zip5,
            }
            out_rows.append(row)
            matched_unique += 1
        else:
            matched_multi += 1

    print(f"  results: {matched_unique} matched | {matched_multi} multi-candidate | "
          f"{no_pos_candidates} no POS pool | {no_nppes_org} no NPPES org record")

    out_path = POS_OUT_DIR / f"{vendor}-pos-nppes-augmented.json"
    payload = {
        "vendor": vendor,
        "captured_date": today,
        "match_summary": {
            "endpoints_considered": len(candidates),
            "matched": matched_unique,
            "multi_candidate_skipped": matched_multi,
            "no_pos_candidates": no_pos_candidates,
            "no_nppes_org_record": no_nppes_org,
        },
        "endpoints": out_rows,
        "verification": {
            "source_url": "data/cms-pos/ + data/cms-nppes/",
            "source_quote": (
                "For endpoints with an NPPES-resolved NPI but no POS CCN from "
                "the brands-bundle address join, used the NPPES org's practice "
                "address (state, zip5, legal name) to filter the POS catalog "
                "and run the same name-token Jaccard match."
            ),
            "verified_via": "cms_pos_via_nppes_address",
            "verified_date": today,
        },
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=VENDORS)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--captured-date", help="ISO date (default: today)")
    args = ap.parse_args()
    if not args.vendor and not args.all:
        ap.error("specify a vendor or --all")

    catalog_path, catalog = load_latest_catalog()
    cnt = catalog.get("facility_count") or catalog.get("hospital_count") or len(catalog.get("hospitals", []))
    print(f"POS catalog: {catalog_path.relative_to(REPO_ROOT)} ({cnt} facilities)")
    pos_idx = {
        "by_state_zip": defaultdict(list),
        "by_state": defaultdict(list),
    }
    for h in catalog["hospitals"]:
        st = h["state"].upper()
        z = h["zip"]
        pos_idx["by_state_zip"][(st, z)].append(h)
        pos_idx["by_state"][st].append(h)

    today = args.captured_date or datetime.date.today().isoformat()
    POS_OUT_DIR.mkdir(parents=True, exist_ok=True)
    vendors = list(VENDORS) if args.all else [args.vendor]
    for v in vendors:
        augment_vendor(v, pos_idx, today)
    return 0


if __name__ == "__main__":
    sys.exit(main())
