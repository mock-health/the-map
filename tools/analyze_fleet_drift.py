"""Analyze a harvested production-fleet snapshot, write per-vendor production_fleet.json.

Inputs (produced by tools.harvest_production_capstmts):
    tests/golden/production-fleet/{ehr}/{captured_date}/
      _summary.json
      _input-brands-bundle.json
      <slug>/capability-statement.json (or .fetch-error.json)
      <slug>/smart-configuration.json  (or .fetch-error.json)

Output:
    ehrs/{ehr}/production_fleet.json   (~50KB, conforms to schema/production_fleet.schema.json)

The headline finding produced: per US Core profile, what fraction of reachable
customers actually advertise it in their CapStmt.supportedProfile. Vendor sandbox
claims X profiles; production fleet says only Y% of customers expose each.

Usage:
    python -m tools.analyze_fleet_drift epic
    python -m tools.analyze_fleet_drift cerner
    python -m tools.analyze_fleet_drift meditech
    python -m tools.analyze_fleet_drift epic --captured-date=2026-04-27   # specific snapshot
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_FLEET = REPO_ROOT / "tests" / "golden" / "production-fleet"
US_CORE_BASELINE_PATH = REPO_ROOT / "us-core" / "us-core-6.1-baseline.json"

# Suffixes that mark a name as a legal-entity registration rather than a
# brand: "ACUMEN PHYSICIAN SOLUTIONS, LLC", "ARIZONA COMMUNITY PHYSICIANS PC".
# Anchored at end-of-string, after optional trailing punctuation.
LEGAL_ENTITY_SUFFIX_RE = re.compile(
    r"(?:\b|,\s*)(LLC|INC|LTD|CORP|PA|PC|PLLC|LLP|LP|PLC|MD|DDS|DO)\.?\s*$",
    re.IGNORECASE,
)
# Words that mark a name as a recognizable healthcare brand. Whole-word match
# avoids false-positives like "MEMORIAL HERMANN" — kept simple on purpose.
HEALTHCARE_KEYWORD_RE = re.compile(
    r"\b(HOSPITAL|MEDICAL\s+CENTER|HEALTHCARE|HEALTH\s+SYSTEM|CHILDREN|MEMORIAL|UNIVERSITY|REGIONAL|CLINIC)\b",
    re.IGNORECASE,
)


def _brand_quality(name: str) -> int:
    """Score a name's value as a 'hospital you'd recognize' label. Higher = better.

    Why this exists: NPD's NPPES-derived names are uppercase legal-entity strings
    ("ACUMEN PHYSICIAN SOLUTIONS, LLC"); vendor brands bundles use curated
    mixed-case ("Akron Children's Hospital"). When ranking singletons in a
    cluster's recognizable_members list, alphabetical-first would have made
    "Acumen Physician Solutions, LLC" the headline — exactly the anti-pattern
    the squishy-meerkat DX plan called out. This score pushes brand-quality
    names to the front of the tiebreaker so cluster pages headline what
    a reader would actually recognize.
    """
    if not name:
        return -10
    s = name.strip().rstrip(".,")
    score = 0
    if LEGAL_ENTITY_SUFFIX_RE.search(s):
        score -= 2
    if HEALTHCARE_KEYWORD_RE.search(s):
        score += 2
    if any(c.islower() for c in s) and any(c.isupper() for c in s):
        score += 1
    return score
NPD_OVERLAY_DIR = REPO_ROOT / "data" / "hospital-overlays"
NPPES_OVERLAY_DIR = REPO_ROOT / "data" / "hospital-overlays"
POS_OVERLAY_DIR = REPO_ROOT / "data" / "hospital-overrides"
PECOS_INDEX_DIR = REPO_ROOT / "data" / "cms-pecos"

VERIFIED_VIA = {
    "epic": "epic_production_fleet",
    "cerner": "cerner_production_fleet",
    "meditech": "meditech_production_fleet",
}

# Vendors that publish more than one host front (e.g., Oracle Health's
# fhir-ehr.cerner.com provider tier vs fhir-myrecord.cerner.com patient tier).
# The brands bundles are harvested separately under their own
# tests/golden/production-fleet/{harvest_slug}/ directories, then unioned at
# analysis time into a single ehrs/{ehr}/production_fleet.json. Each cluster is
# tagged with access_scope so consumers can distinguish provider vs patient
# clusters without parsing cluster IDs. The harvest_slug is internal — it does
# NOT appear as a public EHR identifier (no ehrs/{harvest_slug}/ directory).
MULTI_BUNDLE_VENDORS: dict[str, list[tuple[str, str]]] = {
    # ehr -> list of (harvest_slug, access_scope)
    "cerner": [
        ("cerner", "provider"),
        ("cerner-patient", "patient"),
    ],
}


def _profile_id_from_url(url: str) -> str:
    """Mirrors tools.synthesize._profile_id_from_url so cluster output uses the
    same profile_ids the rest of the pipeline does."""
    base = url.split("|")[0]
    return base.rsplit("/", 1)[-1] if "/" in base else base


# ─────────────────── shape hashing ───────────────────────────────────────────
def capstmt_shape(cap: dict) -> dict:
    """Canonical, hash-friendly shape of a CapStmt's REST surface. Two CapStmts
    with the same shape expose identical resource × profile × search × interaction
    × security signatures — i.e., the same FHIR API to a client. Different
    shapes = deployment-config drift."""
    rest = (cap.get("rest") or [{}])[0]
    resources = []
    for r in rest.get("resource", []) or []:
        resources.append({
            "type": r.get("type"),
            "supportedProfile": sorted(r.get("supportedProfile", [])),
            "interactions": sorted(i.get("code") for i in (r.get("interaction") or [])),
            "searchParams": sorted(p.get("name") for p in (r.get("searchParam") or [])),
            "searchInclude": sorted(r.get("searchInclude", [])),
            "searchRevInclude": sorted(r.get("searchRevInclude", [])),
        })
    resources.sort(key=lambda x: x.get("type") or "")
    security = rest.get("security", {}) or {}
    security_codes = sorted(
        c.get("code")
        for s in (security.get("service") or [])
        for c in (s.get("coding") or [])
        if c.get("code")
    )
    return {"resources": resources, "security_codes": security_codes}


def shape_hash(shape: dict) -> str:
    blob = json.dumps(shape, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(blob).hexdigest()[:16]


# ─────────────────── snapshot discovery ──────────────────────────────────────
def find_snapshot_dir(ehr: str, captured_date: str | None) -> Path:
    base = GOLDEN_FLEET / ehr
    if not base.exists():
        sys.exit(f"ERROR: no harvest directory at {base.relative_to(REPO_ROOT)}/. Run `python -m tools.harvest_production_capstmts {ehr}` first.")
    if captured_date:
        d = base / captured_date
        if not d.exists():
            sys.exit(f"ERROR: no snapshot for {captured_date} under {base.relative_to(REPO_ROOT)}/")
        return d
    candidates = sorted([p for p in base.iterdir() if p.is_dir()])
    if not candidates:
        sys.exit(f"ERROR: no dated subdirectories under {base.relative_to(REPO_ROOT)}/")
    return candidates[-1]


def iter_endpoints(snapshot_dir: Path):
    """Yield (slug, capstmt_dict_or_None, smart_config_dict_or_None) per endpoint."""
    for sub in sorted(snapshot_dir.iterdir()):
        if not sub.is_dir() or sub.name.startswith("_"):
            continue
        cap_path = sub / "capability-statement.json"
        sm_path = sub / "smart-configuration.json"
        cap = json.loads(cap_path.read_text()) if cap_path.exists() else None
        sm = json.loads(sm_path.read_text()) if sm_path.exists() else None
        yield sub.name, cap, sm


# ─────────────────── overlay loading (NPD + POS + NPPES) ───────────────────
def _load_overlays(ehr: str) -> dict[str, dict]:
    """Build a per-endpoint enrichment lookup keyed by normalized address.

    Three overlay sources, layered with this precedence:
      - NPD overlay        (data/hospital-overlays/{ehr}-npd.json):
                           authoritative for NPI when matched. Sets the floor
                           for NPI / state / city / parent_org_name.
      - POS overlay        (data/hospital-overrides/{ehr}-pos.json):
                           authoritative for CCN. Fills state/city/name when
                           NPD missed (MEDITECH on-prem fleets, mostly).
      - NPPES overlay      (data/hospital-overlays/{ehr}-nppes.json):
                           backfills NPI for the long tail NPD didn't index.
                           NPPES has 8M registered providers vs NPD's 100K
                           FHIR-publishing orgs. ALSO sets taxonomy (NPD's
                           NDH publication doesn't carry it).

    The three overlay families use different `endpoint_id` conventions, so we
    key by normalize_address(endpoint_address) — the universal join. Returns
    `{addr_norm: {npi, ccn, pac_id, org_name_canonical, state, city, taxonomy,
                  parent_org_name, parent_org_npi, identity_sources}}`.

    `identity_sources` is a tuple of the overlay names that contributed a
    field — useful for downstream UI: "NPI from NPD, CCN from POS, taxonomy
    from NPPES" gives a citable provenance trail.
    """
    from tools.luxera_endpoint_discovery import normalize_address  # local import; avoids cycle

    out: dict[str, dict] = {}

    def _record_source(slot: dict, source: str) -> None:
        srcs = slot.setdefault("identity_sources", [])
        if source not in srcs:
            srcs.append(source)

    npd_path = NPD_OVERLAY_DIR / f"{ehr}-npd.json"
    if npd_path.exists():
        npd = json.loads(npd_path.read_text())
        for m in npd.get("matches") or []:
            a = normalize_address(m.get("endpoint_address") or "")
            if not a:
                continue
            slot = out.setdefault(a, {})
            if m.get("npi"):
                slot["npi"] = m["npi"]
                slot["org_name_canonical"] = m.get("org_name_npd")
                slot["state"] = m.get("state")
                slot["city"] = m.get("city")
                if m.get("parent_org_name"):
                    slot["parent_org_name"] = m["parent_org_name"]
                if m.get("parent_org_npi"):
                    slot["parent_org_npi"] = m["parent_org_npi"]
                _record_source(slot, "npd")

    pos_path = POS_OVERLAY_DIR / f"{ehr}-pos.json"
    if pos_path.exists():
        pos = json.loads(pos_path.read_text())
        for e in pos.get("endpoints") or []:
            a = normalize_address(e.get("endpoint_address") or "")
            if not a:
                continue
            slot = out.setdefault(a, {})
            if e.get("ccn"):
                slot["ccn"] = e["ccn"]
                _record_source(slot, "pos")
            # POS supplies state/city for many MEDITECH endpoints NPD misses
            if not slot.get("state") and e.get("state"):
                slot["state"] = e["state"]
            if not slot.get("city") and e.get("city"):
                slot["city"] = e["city"]
            # POS name_pos is the catalog-canonical hospital name; only used
            # as a fallback when NPD didn't supply one.
            if not slot.get("org_name_canonical") and e.get("name_pos"):
                slot["org_name_canonical"] = e["name_pos"]

    # Second POS layer: NPPES-address-augmented matches. Tier-3 enrichment
    # for endpoints whose brands-bundle address was empty so the first POS
    # pass skipped them — these used NPPES's practice address as the join
    # key instead. Different verified_via, same CCN field.
    pos_aug_path = POS_OVERLAY_DIR / f"{ehr}-pos-nppes-augmented.json"
    if pos_aug_path.exists():
        pos_aug = json.loads(pos_aug_path.read_text())
        for e in pos_aug.get("endpoints") or []:
            a = normalize_address(e.get("endpoint_address") or "")
            if not a:
                continue
            slot = out.setdefault(a, {})
            # Don't overwrite an existing CCN from the first POS pass — that
            # one had brand-bundle address evidence which is stronger.
            if not slot.get("ccn") and e.get("ccn"):
                slot["ccn"] = e["ccn"]
                _record_source(slot, "pos_via_nppes")

    nppes_path = NPPES_OVERLAY_DIR / f"{ehr}-nppes.json"
    if nppes_path.exists():
        nppes = json.loads(nppes_path.read_text())
        for m in nppes.get("matches") or []:
            a = normalize_address(m.get("endpoint_address") or "")
            if not a:
                continue
            slot = out.setdefault(a, {})
            if not m.get("npi"):
                continue
            # Don't overwrite NPD's authoritative NPI — NPPES is a backfill
            # source for the long tail NPD doesn't index. NPD wins ties because
            # its endpoint→org join is the registered FHIR identity; NPPES
            # joins via name+state which is weaker.
            if not slot.get("npi"):
                slot["npi"] = m["npi"]
                _record_source(slot, "nppes")
                if not slot.get("org_name_canonical"):
                    slot["org_name_canonical"] = m.get("org_name_nppes")
            elif slot["npi"] == m["npi"]:
                # Confirmation — NPD and NPPES agree. Record but don't
                # downgrade confidence.
                _record_source(slot, "nppes")
            # NPPES uniquely supplies taxonomy. Always take it if not set.
            if not slot.get("taxonomy") and m.get("taxonomy"):
                slot["taxonomy"] = m["taxonomy"]
            # State/city: NPPES fills when not already supplied.
            if not slot.get("state") and m.get("state"):
                slot["state"] = m["state"]
            if not slot.get("city") and m.get("city"):
                slot["city"] = m["city"]

    # PECOS layer — enriches existing slots with PAC ID + provider type
    # description. PECOS is keyed by NPI and only adds value to endpoints
    # that already have an NPI from one of the prior layers. The PAC ID
    # is stable across NPI changes (longitudinal join key); the provider
    # type description ("HOSPITAL", "FQHC", "AMBULATORY SURGICAL CENTER")
    # is a CMS-canonical category complementing NPPES's NUCC taxonomy code.
    pecos_index = _load_pecos_index_if_present()
    if pecos_index:
        for slot in out.values():
            npi = slot.get("npi")
            if not npi:
                continue
            rec = pecos_index.get(npi)
            if not rec:
                continue
            if rec.get("pac"):
                slot["pac_id"] = rec["pac"]
                _record_source(slot, "pecos")
            if rec.get("type_desc") and not slot.get("medicare_provider_type"):
                slot["medicare_provider_type"] = rec["type_desc"]

    return out


_pecos_cache: dict | None = None


def _load_pecos_index_if_present() -> dict[str, dict]:
    """Lazy-load the latest PECOS enrollment index. Returns {} if no index
    is built — PECOS enrichment is then a no-op, not an error."""
    global _pecos_cache
    if _pecos_cache is not None:
        return _pecos_cache
    candidates = sorted(PECOS_INDEX_DIR.glob("enrollment-*.json")) if PECOS_INDEX_DIR.exists() else []
    if not candidates:
        _pecos_cache = {}
        return _pecos_cache
    payload = json.loads(candidates[-1].read_text())
    _pecos_cache = payload.get("npis") or {}
    return _pecos_cache


# ─────────────────── per-snapshot loading ────────────────────────────────────
def _load_snapshot(harvest_slug: str, captured_date: str | None) -> tuple[Path, dict, dict[str, dict], dict[str, dict], dict[str, dict]]:
    """Load one harvest snapshot. Returns (snapshot_dir, summary, slug_to_meta,
    capstmts, smart_configs)."""
    snapshot_dir = find_snapshot_dir(harvest_slug, captured_date)
    summary = json.loads((snapshot_dir / "_summary.json").read_text())
    slug_to_meta: dict[str, dict] = {p["slug"]: p for p in summary.get("per_endpoint", [])}
    capstmts: dict[str, dict] = {}
    smart_configs: dict[str, dict] = {}
    for slug, cap, sm in iter_endpoints(snapshot_dir):
        if cap:
            capstmts[slug] = cap
        if sm:
            smart_configs[slug] = sm
    return snapshot_dir, summary, slug_to_meta, capstmts, smart_configs


# ─────────────────── analysis ────────────────────────────────────────────────
def analyze(ehr: str, captured_date: str | None) -> dict:
    from tools.luxera_endpoint_discovery import normalize_address  # local; avoid cycle

    bundle_specs = MULTI_BUNDLE_VENDORS.get(ehr, [(ehr, None)])
    overlay_by_addr = _load_overlays(ehr)

    # Load every harvest snapshot that contributes to this ehr.
    snapshots: list[dict] = []
    for harvest_slug, access_scope in bundle_specs:
        snap_dir, summary, slug_to_meta, capstmts, smart_configs = _load_snapshot(
            harvest_slug, captured_date
        )
        snapshots.append({
            "harvest_slug": harvest_slug,
            "access_scope": access_scope,
            "snapshot_dir": snap_dir,
            "captured_date": snap_dir.name,
            "summary": summary,
            "slug_to_meta": slug_to_meta,
            "capstmts": capstmts,
            "smart_configs": smart_configs,
        })

    # The captured_date in the merged output picks the most recent across snapshots
    # (so quarterly staleness checks stay meaningful for multi-bundle vendors).
    captured_date = max(s["captured_date"] for s in snapshots)

    # Union the per-snapshot data so the rest of the pipeline can run as before.
    # Slugs are unique per host (different harvest dirs -> different slugs).
    slug_to_meta: dict[str, dict] = {}
    capstmts: dict[str, dict] = {}
    smart_configs: dict[str, dict] = {}
    slug_to_scope: dict[str, str | None] = {}
    for snap in snapshots:
        slug_to_meta.update(snap["slug_to_meta"])
        capstmts.update(snap["capstmts"])
        smart_configs.update(snap["smart_configs"])
        for s in snap["capstmts"]:
            slug_to_scope[s] = snap["access_scope"]

    customers_with_capstmt = len(capstmts)

    # Software / FHIR version distributions
    sw_dist = Counter()
    fv_dist = Counter()
    for cap in capstmts.values():
        sw = cap.get("software") or {}
        sw_label = " ".join(filter(None, [sw.get("name"), sw.get("version")])) or "unknown"
        sw_dist[sw_label] += 1
        fv_dist[cap.get("fhirVersion") or "unknown"] += 1

    # Cluster by shape hash. For multi-bundle vendors, cluster per-bundle so
    # cluster IDs stay scoped (cerner-cluster-A vs cerner-patient-cluster-A) and
    # each bundle has its own modal cluster. modal=True is a per-scope flag —
    # the modal patient cluster and the modal provider cluster are both modal
    # within their own access scope.
    slug_to_hash: dict[str, str] = {}
    capstmt_shape_clusters: list[dict] = []
    for snap in snapshots:
        snap_capstmts = snap["capstmts"]
        snap_slug_to_meta = snap["slug_to_meta"]
        access_scope = snap["access_scope"]
        cluster_id_prefix = snap["harvest_slug"]

        hash_to_slugs: dict[str, list[str]] = {}
        hash_to_shape: dict[str, dict] = {}
        for slug, cap in snap_capstmts.items():
            shape = capstmt_shape(cap)
            h = shape_hash(shape)
            slug_to_hash[slug] = h
            hash_to_slugs.setdefault(h, []).append(slug)
            hash_to_shape[h] = shape

        clusters_sorted = sorted(hash_to_slugs.items(), key=lambda kv: -len(kv[1]))
        for idx, (h, slugs) in enumerate(clusters_sorted):
            cluster_id = f"{cluster_id_prefix}-cluster-{chr(ord('A') + idx) if idx < 26 else f'AA{idx}'}"
            shape = hash_to_shape[h]
            resources_advertised = [r["type"] for r in shape["resources"] if r.get("type")]
            supported_profiles = sorted({sp for r in shape["resources"] for sp in r.get("supportedProfile", [])})
            example = slugs[0]
            ex_meta = snap_slug_to_meta.get(example, {})
            # Embed the full endpoint roster for this cluster so consumers can list
            # "hospitals in this cluster" without re-deriving from the harvest snapshot.
            cluster_eps: list[dict] = []
            for s in slugs:
                m = snap_slug_to_meta.get(s, {})
                addr = m.get("address")
                ovl = overlay_by_addr.get(normalize_address(addr or ""), {})
                ep_entry: dict = {
                    "endpoint_id": s,
                    "address": addr,
                    "managing_organization_name": m.get("name"),
                }
                # Layer NPD+POS+NPPES+PECOS enrichment, only emitting keys that
                # have values so the output stays tidy for endpoints with no
                # overlay hit.
                for key in ("npi", "ccn", "pac_id", "state", "city",
                            "taxonomy", "medicare_provider_type",
                            "parent_org_name", "parent_org_npi", "identity_sources"):
                    v = ovl.get(key)
                    if v:
                        ep_entry[key] = v
                cluster_eps.append(ep_entry)
            cluster_eps.sort(key=lambda e: (e.get("managing_organization_name") or "").lower())

            state_dist = Counter(ep.get("state") for ep in cluster_eps if ep.get("state"))
            # Recognizable members: surface up to 10 names per cluster, ranked
            # so the headline of any cluster page reads like "names a clinician
            # would recognize." Tier 1: parents catalogued in NPD (strongest
            # signal — explicit "X has facilities here"). Tier 2: mgmt names
            # repeating across endpoints (vendor bundles list the same brand
            # for sibling tenants). Tier 3: singletons, ranked by _brand_quality
            # so curated names beat legal-entity strings.
            parent_counts: Counter = Counter(
                ep.get("parent_org_name") for ep in cluster_eps if ep.get("parent_org_name")
            )
            mgmt_counts: Counter = Counter(
                ep.get("managing_organization_name") for ep in cluster_eps if ep.get("managing_organization_name")
            )
            # Distinct endpoint count per name: an endpoint contributes once
            # even if its parent_org_name and managing_organization_name are
            # the same string. Avoids double-counting in the displayed count.
            name_to_eps: dict[str, set[int]] = {}
            for i, ep in enumerate(cluster_eps):
                for name in {ep.get("parent_org_name"), ep.get("managing_organization_name")}:
                    if name:
                        name_to_eps.setdefault(name, set()).add(i)

            def _rank_key(n: str, pc: Counter = parent_counts, mc: Counter = mgmt_counts) -> tuple[int, int, int, str]:
                p = pc.get(n, 0)
                m = mc.get(n, 0)
                return (-p, -m, -_brand_quality(n), (n or "").lower())

            candidates = [n for n in (set(parent_counts) | set(mgmt_counts)) if n]
            recognizable_members = [
                {"name": n, "endpoint_count": len(name_to_eps.get(n, ()))}
                for n in sorted(candidates, key=_rank_key)[:10]
            ]

            cluster: dict = {
                "cluster_id": cluster_id,
                "modal": idx == 0,
                "endpoint_count": len(slugs),
                "shape_hash": h,
                "resources_advertised": resources_advertised,
                "supported_profiles_total": len(supported_profiles),
                "supported_profiles": [_profile_id_from_url(sp) for sp in supported_profiles],
                "example_endpoint_id": example,
                "example_endpoint_address": ex_meta.get("address"),
                "endpoints": cluster_eps,
            }
            if state_dist:
                cluster["state_distribution"] = dict(state_dist.most_common(10))
            if recognizable_members:
                cluster["recognizable_members"] = recognizable_members
            if access_scope is not None:
                cluster["access_scope"] = access_scope
            capstmt_shape_clusters.append(cluster)

    # Re-sort the unioned cluster list by endpoint_count desc so consumers that
    # don't filter by access_scope still get the most-impactful clusters first.
    capstmt_shape_clusters.sort(key=lambda c: -c["endpoint_count"])

    # Per-profile support rate (denominator = customers_with_capstmt)
    baseline = json.loads(US_CORE_BASELINE_PATH.read_text())
    baseline_profile_ids = sorted({p["profile_id"] for p in baseline.get("profiles", [])})
    # Anything seen anywhere in the fleet, even if not in baseline (interesting on its own)
    seen_profile_ids: Counter = Counter()
    per_customer_profile_ids: dict[str, set[str]] = {}
    for slug, cap in capstmts.items():
        ids: set[str] = set()
        for r in (cap.get("rest") or [{}])[0].get("resource", []) or []:
            for sp in r.get("supportedProfile", []) or []:
                ids.add(_profile_id_from_url(sp))
        per_customer_profile_ids[slug] = ids
        for pid in ids:
            seen_profile_ids[pid] += 1

    # Modal-cluster profile set used to compute "absent_in_modal_cluster". For
    # multi-bundle vendors the union of every modal cluster's supported_profiles
    # is the right set: a profile flagged absent_in_modal_cluster=True means no
    # access scope's modal advertises it.
    modal_cluster_supported: set = set()
    for c in capstmt_shape_clusters:
        if c.get("modal"):
            modal_cluster_supported.update(c["supported_profiles"])

    profile_support: list[dict] = []
    union_profile_ids = sorted(set(baseline_profile_ids) | set(seen_profile_ids))
    for pid in union_profile_ids:
        n = seen_profile_ids.get(pid, 0)
        profile_support.append({
            "profile_id": pid,
            "in_us_core_baseline": pid in baseline_profile_ids,
            "customers_advertising": n,
            "customers_with_capstmt": customers_with_capstmt,
            "fraction": round(n / customers_with_capstmt, 4) if customers_with_capstmt else 0.0,
            "absent_in_modal_cluster": pid not in modal_cluster_supported,
        })
    profile_support.sort(key=lambda x: (not x["in_us_core_baseline"], -x["fraction"], x["profile_id"]))

    # SMART config drift
    grant_types_dist = Counter()
    cap_dist = Counter()
    auth_methods_dist = Counter()
    pkce_dist = Counter()
    scope_buckets = Counter()
    capabilities_seen: dict[str, int] = Counter()
    for sm in smart_configs.values():
        grant_types_dist[json.dumps(sorted(sm.get("grant_types_supported") or []))] += 1
        cap_dist[json.dumps(sorted(sm.get("capabilities") or []))] += 1
        for c in (sm.get("capabilities") or []):
            capabilities_seen[c] += 1
        auth_methods_dist[json.dumps(sorted(sm.get("token_endpoint_auth_methods_supported") or []))] += 1
        pkce_dist[json.dumps(sorted(sm.get("code_challenge_methods_supported") or []))] += 1
        n_scopes = len(sm.get("scopes_supported") or [])
        bucket = "0" if n_scopes == 0 else "1-99" if n_scopes < 100 else "100-199" if n_scopes < 200 else "200-399" if n_scopes < 400 else "400-599" if n_scopes < 600 else "600+"
        scope_buckets[bucket] += 1

    modal_capabilities_set: list[str] = []
    if cap_dist:
        modal_caps_json, _ = cap_dist.most_common(1)[0]
        modal_capabilities_set = json.loads(modal_caps_json)

    # Outliers — customers NOT in their bundle's modal cluster. For multi-bundle
    # vendors a patient-tier endpoint is an outlier only if it diverges from the
    # patient modal, not from the provider modal (the two modals describe
    # legitimately different shapes — both are intentional).
    outlier_endpoints: list[dict] = []
    for snap in snapshots:
        snap_capstmts = snap["capstmts"]
        snap_meta = snap["slug_to_meta"]
        access_scope = snap["access_scope"]
        # Find this bundle's modal cluster (either tagged with this access_scope
        # or, for single-bundle vendors, the one without an access_scope tag).
        modal_cluster = next(
            (
                c for c in capstmt_shape_clusters
                if c.get("modal") and c.get("access_scope") == access_scope
            ),
            None,
        )
        if modal_cluster is None or len(snap_capstmts) <= modal_cluster["endpoint_count"]:
            continue
        modal_hash = modal_cluster["shape_hash"]
        modal_supp = set(modal_cluster["supported_profiles"])
        for slug, cap in snap_capstmts.items():
            h = slug_to_hash.get(slug)
            if h == modal_hash:
                continue
            cluster_id = next(
                (c["cluster_id"] for c in capstmt_shape_clusters if c["shape_hash"] == h and c.get("access_scope") == access_scope),
                "?",
            )
            this_supp = {
                _profile_id_from_url(sp)
                for r in (cap.get("rest") or [{}])[0].get("resource", []) or []
                for sp in r.get("supportedProfile", []) or []
            }
            missing = sorted(modal_supp - this_supp)
            extra = sorted(this_supp - modal_supp)
            parts = []
            if missing:
                parts.append(f"missing {len(missing)}: {', '.join(missing[:3])}{'…' if len(missing) > 3 else ''}")
            if extra:
                parts.append(f"extra {len(extra)}: {', '.join(extra[:3])}{'…' if len(extra) > 3 else ''}")
            outlier_endpoints.append({
                "endpoint_id": slug,
                "address": snap_meta.get(slug, {}).get("address"),
                "managing_organization_name": snap_meta.get(slug, {}).get("name"),
                "cluster_id": cluster_id,
                "deviation_summary": "; ".join(parts) or "shape differs but profile sets match",
            })
    outlier_endpoints.sort(key=lambda x: x["cluster_id"] or "")

    # Sum harvest stats across all bundles. brands_bundle_source_url is a
    # newline-delimited string when there are multiple bundles so the schema
    # (which types it as `string`) stays satisfied.
    def _sum(field: str) -> int:
        return sum(int(s["summary"].get(field, 0) or 0) for s in snapshots)

    failure_categories: Counter = Counter()
    for s in snapshots:
        for k, v in (s["summary"].get("failure_categories") or {}).items():
            failure_categories[k] += int(v or 0)

    bundle_paths = [s["summary"].get("brands_bundle_source_url_or_path", "") for s in snapshots]
    snapshot_dirs = [str(s["snapshot_dir"].relative_to(REPO_ROOT)) for s in snapshots]
    if len(snapshots) > 1:
        scope_breakdown = ", ".join(
            f"{s['access_scope']}={len(s['capstmts'])}" for s in snapshots
        )
        source_quote = (
            f"Aggregated from {customers_with_capstmt} CapabilityStatements across "
            f"{len(snapshots)} access scopes ({scope_breakdown}) harvested under "
            + ", ".join(snapshot_dirs)
            + "/"
        )
    else:
        source_quote = (
            f"Aggregated from {customers_with_capstmt} CapabilityStatements harvested "
            f"under {snapshot_dirs[0]}/"
        )

    return {
        "ehr": ehr,
        "captured_date": captured_date,
        "brands_bundle_source_url": "\n".join(p for p in bundle_paths if p),
        "brands_bundle_total_endpoints": _sum("brands_bundle_total_endpoints"),
        "harvest_summary": {
            "endpoints_attempted": _sum("endpoints_attempted"),
            "capstmt_fetched_ok": _sum("capstmt_fetched_ok"),
            "capstmt_fetch_failed": _sum("capstmt_fetch_failed"),
            "smart_config_fetched_ok": _sum("smart_config_fetched_ok"),
            "smart_config_fetch_failed": _sum("smart_config_fetch_failed"),
            "failure_categories": dict(failure_categories.most_common()),
            "wall_clock_seconds": sum(
                float(s["summary"].get("wall_clock_seconds") or 0) for s in snapshots
            ) or None,
            "concurrency": snapshots[0]["summary"].get("concurrency"),
        },
        "software_distribution": dict(sw_dist.most_common()),
        "fhir_version_distribution": dict(fv_dist),
        "capstmt_shape_clusters": capstmt_shape_clusters,
        "smart_config_drift": {
            "grant_types_supported_distribution": dict(grant_types_dist.most_common()),
            "capabilities_modal_set": modal_capabilities_set,
            "capabilities_distribution": dict(sorted(capabilities_seen.items(), key=lambda kv: -kv[1])),
            "scope_count_distribution": dict(scope_buckets),
            "token_endpoint_auth_methods_distribution": dict(auth_methods_dist.most_common()),
            "code_challenge_methods_distribution": dict(pkce_dist.most_common()),
        },
        "us_core_profile_support_rate": profile_support,
        "outlier_endpoints": outlier_endpoints,
        "verification": {
            "source_url": "\n".join(snapshot_dirs),
            "source_quote": source_quote,
            "verified_via": VERIFIED_VIA.get(ehr, f"{ehr}_production_fleet"),
            "verified_date": captured_date,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr")
    ap.add_argument("--captured-date", help="Specific snapshot date (default: most recent under tests/golden/production-fleet/{ehr}/)")
    ap.add_argument("--out", help="Override output path; default ehrs/{ehr}/production_fleet.json")
    ap.add_argument("--print", action="store_true", help="Print headline findings to stdout after writing")
    args = ap.parse_args()

    fleet = analyze(args.ehr, args.captured_date)

    out_path = Path(args.out) if args.out else (EHRS_DIR / args.ehr / "production_fleet.json")
    if not out_path.is_absolute():
        out_path = REPO_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(fleet, indent=2) + "\n")

    print(f"wrote {out_path.relative_to(REPO_ROOT)}")
    print(f"  endpoints harvested: {fleet['harvest_summary']['endpoints_attempted']}")
    print(f"  capstmt OK: {fleet['harvest_summary']['capstmt_fetched_ok']} ({100 * fleet['harvest_summary']['capstmt_fetched_ok'] // max(1, fleet['harvest_summary']['endpoints_attempted'])}%)")
    print(f"  shape clusters: {len(fleet['capstmt_shape_clusters'])}")
    if fleet['capstmt_shape_clusters']:
        modal = fleet['capstmt_shape_clusters'][0]
        print(f"    modal: {modal['cluster_id']} = {modal['endpoint_count']} endpoints, {modal['supported_profiles_total']} profiles")
    if args.print:
        print("\n  Top US Core profile support rates:")
        for p in fleet["us_core_profile_support_rate"][:15]:
            print(f"    {p['profile_id']:50}  {p['customers_advertising']:>5}/{p['customers_with_capstmt']:<5} = {p['fraction'] * 100:>5.1f}%  {'(in baseline)' if p['in_us_core_baseline'] else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
