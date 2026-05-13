"""Resolve the-map's fleet endpoints to CMS NPPES entries.

Joins each Endpoint in `ehrs/{vendor}/production_fleet.json` against the NPPES
indexes at `data/cms-nppes/{fhir-endpoints,orgs}-{release}.json[l]` and emits a
per-vendor overlay at `data/hospital-overlays/{vendor}-nppes.json`.

Strategy ordering — first-hit-wins per endpoint. We try strongest evidence
first; once we have an NPI we stop. Each strategy carries a `confidence`
label so downstream consumers can be choosy.

  1. canonical_url            normalize_address(fleet.address) == normalize_address(nppes.url)
                              → high confidence. URL identity is the strongest
                              signal: the org REGISTERED this URL with CMS as
                              its endpoint. Direct NPI attribution.
  2. hostname_registered      The fleet endpoint's hostname appears in NPPES
                              under a SINGLE NPI. Different URL paths on the
                              same host all map to the same managing NPI.
                              → high confidence when 1 NPI; downgraded to
                              `medium` when 2-5; skipped when >5 (shared
                              tenant hosts like fhir-myrecord.cerner.com
                              would pollute matches otherwise).
  3. org_name_state           normalize_org_name(fleet.mgmt_org_name) matches
                              an NPPES org's name_norm AND state matches a
                              state hint (from a prior NPD/POS overlay).
                              → high confidence when 1 match; medium when ≤3.
  4. org_name                 Same name normalization, no state hint. Only
                              emitted when the name has a UNIQUE NPPES org
                              (1 match nationally).
                              → medium confidence.
  5. org_name_other           Same as (4) but matching NPPES `other_name`
                              (DBA / alternative legal name).
                              → medium confidence.

The resolver also uses state hints derived from POS/NPD overlays where
available. The signal that an Epic endpoint labeled "AdventHealth" is in FL
narrows NPPES from "5 distinct AdventHealth* orgs nationally" to "the one
AdventHealth in FL." Without state context, an ambiguous name skips
the lookup rather than guessing.

Output: `data/hospital-overlays/{vendor}-nppes.json`. Re-runnable.

Usage:
    python -m tools.resolve_endpoints_to_nppes epic
    python -m tools.resolve_endpoints_to_nppes --all
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from rapidfuzz import fuzz, process

from tools.build_nppes_index import normalize_org_name
from tools.luxera_endpoint_discovery import normalize_address

REPO_ROOT = Path(__file__).resolve().parent.parent
NPPES_DIR = REPO_ROOT / "data" / "cms-nppes"
OVERLAYS_DIR = REPO_ROOT / "data" / "hospital-overlays"
POS_OVERLAYS_DIR = REPO_ROOT / "data" / "hospital-overrides"
EHRS_DIR = REPO_ROOT / "ehrs"

VENDORS = ("epic", "cerner", "meditech")

# Hostname-match cutoff. A host with more than this many distinct NPIs in
# NPPES is a shared tenant front (fhir-myrecord.cerner.com, api.epic.com)
# — match would be ambiguous, so we skip.
HOST_NPI_AMBIGUITY_CUTOFF = 5

# Fuzzy match acceptance threshold (rapidfuzz token_set_ratio scale 0-100).
# 85 is empirically the inflection point where false positives become rare —
# "RWJ BARNABAS HEALTH" -> "ROBERT WOOD JOHNSON BARNABAS HEALTH" scores ~88,
# while "TRINITY HEALTH" vs "TRINITY HEALTH CARE INC" scores 98. Anything
# below 85 starts pulling in generic "HEALTH" prefix matches.
FUZZY_NAME_THRESHOLD = 85

# Infrastructure-name patterns. These are vendor sandbox / tenancy labels
# that look like org names but never resolve to a real NPI: matching them is
# guaranteed wrong, so skip the NPPES lookup entirely. Patterns are
# case-insensitive and matched via re.search (anywhere in the name).
INFRA_JARGON_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bPod\s+\d+\b",                # "MaaS MEDITECH Hosted Pod 009"
        r"\bDomain\s+[A-Z]\b",           # "Continuum Of Care Domain A"
        r"\bTime\s+Zone\s+[A-Z]\b",      # "CPHY Production Central Time Zone B"
        r"\bTenant\s+\d+\b",             # "Tenant 123"
        r"\bSandbox\b",                  # "*Sandbox*"
        r"\bHosted\s+Pod\b",
        r"\bMaaS\b",                     # MEDITECH-as-a-Service infrastructure
        r"\bTest(?:ing)?\s+(?:Tenant|Customer|Org)\b",
        r"^FHIR\s+URL$",                 # placeholder label
    ]
]


def _is_infrastructure_jargon(name: str) -> bool:
    if not name:
        return False
    return any(p.search(name) for p in INFRA_JARGON_PATTERNS)


def _hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def load_nppes_indexes(release: str | None = None) -> tuple[str, dict, dict]:
    """Load NPPES indexes from data/cms-nppes/. Returns (release, fhir_endpoints, orgs_by_npi)."""
    if release is None:
        cands = sorted(NPPES_DIR.glob("fhir-endpoints-*.json"))
        if not cands:
            sys.exit(
                "ERROR: no NPPES FHIR endpoint index found. Run "
                "`python -m tools.build_nppes_index` first."
            )
        release = cands[-1].stem.replace("fhir-endpoints-", "")
    ep_path = NPPES_DIR / f"fhir-endpoints-{release}.json"
    org_path = NPPES_DIR / f"orgs-{release}.jsonl"
    if not ep_path.exists() or not org_path.exists():
        sys.exit(f"ERROR: NPPES index files missing for release {release}")
    print(f"Loading NPPES indexes (release {release})")
    fhir = json.loads(ep_path.read_text())
    return release, fhir, _load_orgs_jsonl(org_path)


def _load_orgs_jsonl(path: Path) -> dict[str, dict]:
    """Load orgs.jsonl into a dict keyed by NPI."""
    out: dict[str, dict] = {}
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("_header"):
                continue
            out[rec["npi"]] = rec
    print(f"  loaded {len(out):,} NPPES orgs")
    return out


def build_lookups(
    fhir_index: dict,
    orgs_by_npi: dict,
) -> tuple[dict, dict, dict, dict, dict]:
    """Build the five NPPES lookups used by the resolver:
      - by_url_norm: url_norm -> [endpoint records]
      - by_host: hostname -> {NPI: count} (we score ambiguity by # of NPIs)
      - by_name_state: (name_norm, state) -> [npi]
      - by_name: name_norm -> [npi]   (legal name + other_name combined)
      - by_state: state -> [(name_norm, npi)]  — for state-narrowed fuzzy match
    """
    by_url_norm: dict[str, list[dict]] = defaultdict(list)
    by_host: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for ep in fhir_index.get("endpoints", []):
        url_norm = ep.get("url_norm") or ""
        if url_norm:
            by_url_norm[url_norm].append(ep)
        host = _hostname(ep.get("url") or "")
        if host:
            by_host[host][ep["npi"]] += 1

    by_name_state: dict[tuple[str, str], list[str]] = defaultdict(list)
    by_name: dict[str, list[str]] = defaultdict(list)
    by_state: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for npi, rec in orgs_by_npi.items():
        state = rec.get("state") or ""
        for key in ("name_norm", "other_name_norm"):
            n = rec.get(key) or ""
            if not n or n == "<UNAVAIL>":
                continue
            by_name[n].append(npi)
            if state:
                by_name_state[(n, state)].append(npi)
                by_state[state].append((n, npi))
    print(
        f"  built lookups: {len(by_url_norm):,} URLs, {len(by_host):,} hosts, "
        f"{len(by_name):,} unique names, {len(by_name_state):,} (name,state) pairs, "
        f"{len(by_state):,} states ({sum(len(v) for v in by_state.values()):,} name-state rows)"
    )
    return (
        dict(by_url_norm),
        {h: dict(npis) for h, npis in by_host.items()},
        dict(by_name_state),
        dict(by_name),
        dict(by_state),
    )


def _state_hint_for_endpoint(
    fleet_ep: dict,
    pos_overlay_by_addr: dict[str, dict],
    npd_overlay_by_addr: dict[str, dict],
) -> str | None:
    """Pick the strongest state hint from already-known overlays."""
    addr_norm = normalize_address(fleet_ep.get("address") or "")
    for src in (npd_overlay_by_addr, pos_overlay_by_addr):
        hit = src.get(addr_norm) or {}
        if hit.get("state"):
            return hit["state"]
    return None


def _load_overlay_by_addr(path: Path, address_key: str = "endpoint_address") -> dict[str, dict]:
    """Index an overlay JSON's `matches` (or `endpoints`) by normalized address."""
    if not path.exists():
        return {}
    payload = json.loads(path.read_text())
    out: dict[str, dict] = {}
    for rec in payload.get("matches") or payload.get("endpoints") or []:
        addr = rec.get(address_key) or rec.get("address") or rec.get("endpoint_address")
        if not addr:
            continue
        out[normalize_address(addr)] = rec
    return out


def _iter_fleet(fleet: dict):
    for cluster in fleet.get("capstmt_shape_clusters") or []:
        for ep in cluster.get("endpoints") or []:
            yield {
                "endpoint_id": ep.get("endpoint_id"),
                "address": ep.get("address"),
                "managing_organization_name": ep.get("managing_organization_name"),
                "cluster_id": cluster.get("cluster_id"),
                "state_hint_from_fleet": ep.get("state"),
            }


def _identify_shared_tenant_hosts(fleet_endpoints: list[dict]) -> set[str]:
    """A host that serves ≥2 distinct fleet URLs is shared tenant infrastructure
    (e.g., `fhir-ehr.cerner.com`, `epicproxy-pub.et*.epichosted.com`). Hostname
    matches on these hosts would mis-attribute every tenant's endpoint to the
    NPI of whichever single tenant happened to register their URL with NPPES.
    Skip hostname strategy for these; URL exact is still safe."""
    host_count: dict[str, int] = defaultdict(int)
    for ep in fleet_endpoints:
        h = _hostname(ep.get("address") or "")
        if h:
            host_count[h] += 1
    return {h for h, n in host_count.items() if n >= 2}


def resolve_one(
    ep: dict,
    state_hint: str | None,
    by_url_norm: dict,
    by_host: dict,
    by_name_state: dict,
    by_name: dict,
    by_state: dict,
    orgs_by_npi: dict,
    shared_tenant_hosts: set[str],
) -> dict | None:
    addr_raw = ep.get("address") or ""
    addr_norm = normalize_address(addr_raw)

    # 1. URL exact
    if addr_norm and addr_norm in by_url_norm:
        candidates = by_url_norm[addr_norm]
        # If multiple NPPES rows for one URL, pick the row whose NPI we have
        # the richest org info for (proxy: shortest non-deactivated NPI present
        # in orgs_by_npi). Practically rare — same URL registered by 1 NPI.
        chosen = next((c for c in candidates if c["npi"] in orgs_by_npi), candidates[0])
        return _format_match(ep, chosen, strategy="canonical_url", confidence="high", orgs_by_npi=orgs_by_npi)

    # 2. Hostname — only on hosts that are NOT shared tenant infrastructure
    host = _hostname(addr_raw)
    if host and host not in shared_tenant_hosts and host in by_host:
        host_npis = by_host[host]
        if len(host_npis) == 1:
            (npi,) = host_npis.keys()
            return _format_match_from_npi(ep, npi, strategy="hostname_registered", confidence="high", orgs_by_npi=orgs_by_npi)
        elif len(host_npis) <= HOST_NPI_AMBIGUITY_CUTOFF:
            # Pick the NPI with the most endpoint registrations on this host
            top_npi = max(host_npis.items(), key=lambda kv: kv[1])[0]
            return _format_match_from_npi(ep, top_npi, strategy="hostname_registered", confidence="medium",
                                          orgs_by_npi=orgs_by_npi,
                                          extra={"host_npi_ambiguity": len(host_npis)})
        # else: too ambiguous, fall through to name

    # Name-based strategies: skip if name is sandbox/infrastructure jargon.
    name = (ep.get("managing_organization_name") or "").strip()
    if not name or _is_infrastructure_jargon(name):
        return None
    name_norm = normalize_org_name(name)
    if not name_norm:
        return None

    # 3. Org name + state — exact normalized match
    if state_hint:
        npis = by_name_state.get((name_norm, state_hint), [])
        if len(npis) == 1:
            return _format_match_from_npi(ep, npis[0], strategy="org_name_state", confidence="high",
                                          orgs_by_npi=orgs_by_npi)
        elif 1 < len(npis) <= 3:
            return _format_match_from_npi(ep, npis[0], strategy="org_name_state", confidence="medium",
                                          orgs_by_npi=orgs_by_npi,
                                          extra={"state_match_ambiguity": len(npis)})

    # 4. Org name unique nationally
    npis = by_name.get(name_norm, [])
    if len(npis) == 1:
        return _format_match_from_npi(ep, npis[0], strategy="org_name_unique", confidence="medium",
                                      orgs_by_npi=orgs_by_npi)

    # 5. Fuzzy match within state. Picks up name variants ("RWJ Barnabas Health"
    #    vs "Robert Wood Johnson Barnabas Health") that exact normalization misses.
    if state_hint and state_hint in by_state:
        candidates = by_state[state_hint]
        choices = [c[0] for c in candidates]  # name_norm list
        best = process.extractOne(
            name_norm, choices, scorer=fuzz.token_set_ratio,
            score_cutoff=FUZZY_NAME_THRESHOLD,
        )
        if best is not None:
            _matched_name, score, idx = best
            picked_npi = candidates[idx][1]
            return _format_match_from_npi(
                ep, picked_npi, strategy="org_name_fuzzy_state",
                confidence="medium" if score >= 92 else "low",
                orgs_by_npi=orgs_by_npi,
                extra={"fuzzy_score": int(score)},
            )

    return None


def _format_match(ep: dict, npp_ep: dict, *, strategy: str, confidence: str, orgs_by_npi: dict) -> dict:
    return _format_match_from_npi(ep, npp_ep["npi"], strategy=strategy, confidence=confidence,
                                  orgs_by_npi=orgs_by_npi,
                                  extra={"matched_nppes_url": npp_ep.get("url")})


def _format_match_from_npi(
    ep: dict, npi: str, *, strategy: str, confidence: str, orgs_by_npi: dict, extra: dict | None = None,
) -> dict:
    org = orgs_by_npi.get(npi) or {}
    out = {
        "endpoint_id": ep.get("endpoint_id"),
        "endpoint_address": ep.get("address"),
        "match_strategy": strategy,
        "confidence": confidence,
        "npi": npi,
        "org_name_nppes": org.get("name"),
        "state": org.get("state") or None,
        "city": org.get("city") or None,
        "postal_code": org.get("postal") or None,
        "address_line": org.get("addr") or None,
        "taxonomy": org.get("taxonomy") or None,
    }
    if extra:
        out.update(extra)
    return out


def resolve_vendor(
    vendor: str,
    by_url_norm: dict,
    by_host: dict,
    by_name_state: dict,
    by_name: dict,
    by_state: dict,
    orgs_by_npi: dict,
    nppes_release: str,
) -> dict:
    fleet_path = EHRS_DIR / vendor / "production_fleet.json"
    if not fleet_path.exists():
        sys.exit(f"ERROR: missing {fleet_path}.")
    fleet = json.loads(fleet_path.read_text())

    # Load existing overlays for state hints
    npd_overlay = _load_overlay_by_addr(OVERLAYS_DIR / f"{vendor}-npd.json")
    pos_overlay = _load_overlay_by_addr(POS_OVERLAYS_DIR / f"{vendor}-pos.json", address_key="endpoint_address")

    matches: list[dict] = []
    by_strategy: dict[str, int] = defaultdict(int)
    n_with_npi = 0
    eps = list(_iter_fleet(fleet))
    shared_tenant_hosts = _identify_shared_tenant_hosts(eps)
    if shared_tenant_hosts:
        print(f"  {vendor}: {len(shared_tenant_hosts)} shared-tenant hosts detected (hostname strategy disabled for these)")
    for ep in eps:
        state_hint = _state_hint_for_endpoint(ep, pos_overlay, npd_overlay)
        m = resolve_one(ep, state_hint, by_url_norm, by_host, by_name_state, by_name, by_state, orgs_by_npi, shared_tenant_hosts)
        if m is None:
            continue
        matches.append(m)
        by_strategy[m["match_strategy"]] += 1
        if m.get("npi"):
            n_with_npi += 1

    return {
        "vendor": vendor,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "source_nppes_release": nppes_release,
        "summary": {
            "fleet_total": len(eps),
            "matched": len(matches),
            "matched_with_npi": n_with_npi,
            "unmatched": len(eps) - len(matches),
            "by_strategy": dict(by_strategy),
        },
        "matches": matches,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=VENDORS)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--release", help="NPPES release date (e.g. 2026-02-08); default = latest")
    args = ap.parse_args()
    if not args.vendor and not args.all:
        ap.error("specify a vendor or --all")

    release, fhir_index, orgs_by_npi = load_nppes_indexes(args.release)
    by_url_norm, by_host, by_name_state, by_name, by_state = build_lookups(fhir_index, orgs_by_npi)

    OVERLAYS_DIR.mkdir(parents=True, exist_ok=True)
    vendors = list(VENDORS) if args.all else [args.vendor]
    for v in vendors:
        result = resolve_vendor(v, by_url_norm, by_host, by_name_state, by_name, by_state, orgs_by_npi, release)
        out_path = OVERLAYS_DIR / f"{v}-nppes.json"
        out_path.write_text(json.dumps(result, indent=2) + "\n")
        s = result["summary"]
        total = s["fleet_total"] or 1
        print(
            f"  {v}: matched {s['matched']}/{s['fleet_total']} ({100 * s['matched'] // total}%), "
            f"NPI {s['matched_with_npi']} ({100 * s['matched_with_npi'] // total}%), "
            f"strategies={s['by_strategy']}"
        )
        print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
