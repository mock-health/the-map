"""Resolve FHIR brand-bundle Endpoints to CMS hospitals (CCN).

Joins each Endpoint in `tests/golden/cross-vendor/{stem}-{date}.json` against
the POS hospital catalog at `data/cms-pos/hospitals-{date}.json` and emits a
per-vendor resolution file at `data/hospital-overrides/{vendor}-pos.json`.

Per-vendor strategy (in order of confidence the brands bundle gives us):

  cerner    Organization.address.{state, postalCode, city} is universal (1451/1451).
            Filter POS to (state, zip5), pick best name-Jaccard match.
            ~9% of orgs also publish NPI; recorded for future NPPES upgrade.

  meditech  Organization.address present on ~96% of orgs. Same join as cerner.
            ~4% with no address fall to name-only or unmatched.

  epic      Endpoint-only bundle. Endpoint.name is the only signal.
            We try to extract a state-or-city hint from the name, then output
            candidates[] rather than auto-resolving. A hand-curated
            data/hospital-overrides/epic-pos.manual.json wins over auto.

Output schema: schema/hospital_resolution.schema.json. Re-runnable: a fresh
harvest never overwrites entries present in the *.manual.json override file.

Usage:
    python -m tools.resolve_endpoints_to_pos cerner
    python -m tools.resolve_endpoints_to_pos meditech
    python -m tools.resolve_endpoints_to_pos epic
    python -m tools.resolve_endpoints_to_pos --all
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_CROSS_VENDOR = REPO_ROOT / "tests" / "golden" / "cross-vendor"
POS_DIR = REPO_ROOT / "data" / "cms-pos"
OUT_DIR = REPO_ROOT / "data" / "hospital-overrides"

NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"

# Brands-bundle source per vendor → (cross-vendor stem, optional second stem for
# vendors with multiple bundles). Stems match tools/fetch_brands.py.
VENDOR_BUNDLES: dict[str, list[str]] = {
    "cerner": ["oracle-health-provider-r4-endpoints"],
    "meditech": ["meditech-brands"],
    "epic": ["epic-r4-endpoints"],
}

# Auto-acceptance thresholds. Below these the row goes to candidates[] only.
THRESH_STATE_ZIP5 = 0.55
THRESH_STATE_ONLY = 0.75
# Min score for an entry to even appear in candidates[]. Lower scores produce
# too many "Treasure Coast Podiatry → Encompass Health Rehab of Treasure Coast"
# style noise where a clinic shares one location word with an unrelated hospital.
CANDIDATE_FLOOR = 0.4


# --- name normalization --------------------------------------------------

# Tokens that appear in nearly every hospital name and dilute Jaccard signal
# rather than improving it. Removing them raises recall for the genuinely
# distinguishing tokens (city/saint name/system name) without hurting precision
# because we still require a state+zip filter to even get to the comparison.
STOP_TOKENS = frozenset({
    "hospital", "hospitals", "medical", "center", "centers", "centre",
    "health", "healthcare", "system", "systems", "the", "of", "and", "at",
    "a", "an", "for", "to", "inc", "llc", "corp", "corporation", "co",
    "regional", "community", "general", "memorial", "campus",
})


def name_tokens(s: str) -> set[str]:
    if not s:
        return set()
    cleaned = re.sub(r"[^a-z0-9\s]", " ", s.lower())
    toks = {t for t in cleaned.split() if t and t not in STOP_TOKENS}
    return toks


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# --- POS catalog loader --------------------------------------------------

def load_latest_catalog() -> tuple[Path, dict]:
    """Prefer broader providers-*.json catalog (hospitals + FQHC + ASC + RHC + …)
    when present; fall back to legacy hospitals-*.json for hospital-only."""
    providers = sorted(POS_DIR.glob("providers-*.json"))
    if providers:
        p = providers[-1]
        return p, json.loads(p.read_text())
    candidates = sorted(POS_DIR.glob("hospitals-*.json"))
    if not candidates:
        sys.exit(
            "ERROR: no POS catalog found under data/cms-pos/. Run "
            "`python -m tools.build_pos_hospital_index --categories all` first."
        )
    p = candidates[-1]
    return p, json.loads(p.read_text())


def index_catalog(catalog: dict) -> dict:
    """Return a multi-key index: (state, zip5), (state,), and an iterable for fallback."""
    by_state_zip: dict[tuple[str, str], list[dict]] = defaultdict(list)
    by_state: dict[str, list[dict]] = defaultdict(list)
    by_npi: dict[str, dict] = {}  # POS doesn't have NPI; reserved for NPPES upgrade.
    by_ccn: dict[str, dict] = {}
    for h in catalog["hospitals"]:
        st = h["state"].upper()
        z = h["zip"]
        by_state_zip[(st, z)].append(h)
        by_state[st].append(h)
        by_ccn[h["ccn"]] = h
    return {
        "by_state_zip": by_state_zip,
        "by_state": by_state,
        "by_npi": by_npi,
        "by_ccn": by_ccn,
        "all": catalog["hospitals"],
    }


# --- match scoring -------------------------------------------------------

def score_candidates(name: str, pool: list[dict]) -> list[tuple[float, dict]]:
    """Return [(score, hospital)] sorted descending. Score is name-token Jaccard."""
    qt = name_tokens(name)
    if not qt:
        return []
    scored = [(jaccard(qt, name_tokens(h["name"])), h) for h in pool]
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored


def to_candidate(score: float, h: dict) -> dict:
    out = {
        "ccn": h["ccn"],
        "name": h["name"],
        "city": h["city"],
        "state": h["state"],
        "zip5": h["zip"],
        "score": round(score, 4),
    }
    # Carry POS category through to consumers (LLM disambiguation uses these
    # to filter by NPPES-derived taxonomy: a hospital endpoint shouldn't be
    # disambiguated against FQHC candidates).
    if h.get("category_code"):
        out["category_code"] = h["category_code"]
    if h.get("category_label"):
        out["category_label"] = h["category_label"]
    return out


# --- per-vendor extractors ----------------------------------------------

def iter_endpoint_records(bundle: dict, vendor: str) -> list[dict]:
    """Walk a brands bundle and emit one dict per Endpoint with whatever metadata
    we can recover. Returns:
      [{endpoint_id, endpoint_address, name, npi, address: {city, state, zip5}}]
    """
    entries = bundle.get("entry") or []

    # Index Organizations by id; we resolve managingOrganization references
    # cross-resource. Cerner and Meditech use top-level Organizations; Epic
    # uses Endpoint.contained.
    orgs_by_id: dict[str, dict] = {}
    org_by_endpoint_id: dict[str, dict] = {}
    for e in entries:
        r = e.get("resource") or {}
        if r.get("resourceType") == "Organization":
            if r.get("id"):
                orgs_by_id[r["id"]] = r
            for ep_ref in r.get("endpoint") or []:
                ref = (ep_ref or {}).get("reference", "") if isinstance(ep_ref, dict) else ""
                # FHIR refs come in many flavors:
                #   "Endpoint/<id>"  /  "urn:uuid:<id>"  /  "<id>"
                if ref.startswith("Endpoint/"):
                    org_by_endpoint_id[ref.split("/", 1)[1]] = r
                elif ref.startswith("urn:uuid:"):
                    org_by_endpoint_id[ref.split(":", 2)[2]] = r
                elif ref:
                    org_by_endpoint_id[ref] = r

    out: list[dict] = []
    for e in entries:
        r = e.get("resource") or {}
        if r.get("resourceType") != "Endpoint":
            continue
        eid = r.get("id") or ""
        eaddr = r.get("address")

        org = None
        # 1. Endpoint→Organization via managingOrganization.reference
        ref = ((r.get("managingOrganization") or {}).get("reference") or "").strip()
        if ref:
            if ref in orgs_by_id:
                org = orgs_by_id[ref]
            elif ref.startswith("Organization/") and ref.split("/", 1)[1] in orgs_by_id:
                org = orgs_by_id[ref.split("/", 1)[1]]
        # 2. Endpoint.contained (Epic style)
        if org is None:
            for c in r.get("contained") or []:
                if c.get("resourceType") == "Organization":
                    org = c
                    break
        # 3. Reverse lookup: Organization.endpoint[] → this endpoint id
        if org is None and eid in org_by_endpoint_id:
            org = org_by_endpoint_id[eid]

        npi = None
        for ident in (org or {}).get("identifier") or []:
            if ident.get("system") == NPI_SYSTEM and ident.get("value"):
                npi = ident["value"].strip()
                break

        addr = ((org or {}).get("address") or [None])[0] or {}
        city = (addr.get("city") or "").strip()
        state = (addr.get("state") or "").strip().upper()
        zip5 = re.sub(r"\D", "", (addr.get("postalCode") or ""))[:5]

        # Endpoint-level name (Epic) or Organization.name (Cerner/Meditech)
        name = (r.get("name") or (org or {}).get("name") or "").strip()

        out.append({
            "endpoint_id": eid,
            "endpoint_address": eaddr,
            "name": name,
            "npi": npi,
            "city": city,
            "state": state,
            "zip5": zip5,
        })
    return out


# --- core resolver -------------------------------------------------------

def resolve_one(record: dict, idx: dict) -> dict:
    """Pick the best CCN match for one endpoint record.

    Returns a dict with the schema-compliant fields.
    """
    out: dict = {
        "endpoint_id": record["endpoint_id"],
        "endpoint_address": record["endpoint_address"],
        "name_observed": record["name"],
        "npi": record["npi"],
        "city": record["city"] or None,
        "state": record["state"] or None,
        "zip5": record["zip5"] or None,
    }

    state, zip5, name = record["state"], record["zip5"], record["name"]

    # Strategy A: state + zip5 narrow candidate pool
    best_strategy = "unmatched"
    best_score = 0.0
    best: dict | None = None
    candidates: list[dict] = []

    if state and zip5:
        pool = idx["by_state_zip"].get((state, zip5), [])
        scored = score_candidates(name, pool)
        if scored:
            top_score, top_h = scored[0]
            if top_score >= THRESH_STATE_ZIP5:
                best, best_score, best_strategy = top_h, top_score, "address_state_zip5_name"
            candidates = [to_candidate(s, h) for s, h in scored if s >= CANDIDATE_FLOOR][:5]

    # Strategy B: state-only fallback
    if best is None and state:
        pool = idx["by_state"].get(state, [])
        scored = score_candidates(name, pool)
        if scored:
            top_score, top_h = scored[0]
            if top_score >= THRESH_STATE_ONLY:
                # Tie-break: if the top score is very close to the second, refuse
                second = scored[1][0] if len(scored) > 1 else 0.0
                if top_score - second >= 0.05:
                    best, best_score, best_strategy = top_h, top_score, "address_state_name"
            if not candidates:
                candidates = [to_candidate(s, h) for s, h in scored if s >= CANDIDATE_FLOOR][:5]

    # Strategy C: name-only (Epic) — only used when state is unknown.
    # No auto-resolve; surface candidates[] only. Use a stricter floor here
    # because without state we have no narrowing signal at all.
    if best is None and not state and name:
        pool = idx["all"]
        scored = score_candidates(name, pool)
        candidates = [to_candidate(s, h) for s, h in scored if s >= 0.5][:8]
        best_strategy = "name_only" if candidates else "unmatched"

    if best is not None:
        out.update({
            "ccn": best["ccn"],
            "name_pos": best["name"],
            "fac_subtype_code": best["fac_subtype_code"],
            "bed_count": best["bed_count"],
            # also normalize observed addr from the POS row
            "city": best["city"],
            "state": best["state"],
            "zip5": best["zip"],
        })
    else:
        out.update({"ccn": None, "name_pos": None, "fac_subtype_code": None, "bed_count": None})

    out["match_strategy"] = best_strategy
    out["match_confidence"] = round(best_score, 4)
    if candidates:
        out["candidates"] = candidates
    return out


def apply_overrides(
    records: list[dict],
    override_path: Path,
    idx: dict,
    today: str,
    *,
    match_strategy: str,
    verified_via: str,
) -> int:
    """Apply an `{endpoint_id: ccn}` override file to in-place records.

    Used twice in the pipeline:
      1. `*-pos.llm.json` (match_strategy='llm_assisted', verified_via='llm_assisted')
      2. `*-pos.manual.json` (match_strategy='manual_override', verified_via='manual_override')

    Order matters: manual is applied AFTER LLM so a hand-curated override
    always wins over an LLM judgment, which always wins over the auto match.
    """
    if not override_path.exists():
        return 0
    overrides = json.loads(override_path.read_text())
    if not isinstance(overrides, dict):
        sys.exit(f"ERROR: {override_path} must be a JSON object {{endpoint_id: ccn, ...}}")
    applied = 0
    by_id = {r["endpoint_id"]: r for r in records}
    for eid, ccn in overrides.items():
        rec = by_id.get(eid)
        if rec is None:
            continue
        h = idx["by_ccn"].get(ccn)
        if h is None:
            sys.stderr.write(
                f"WARN: override {eid} → {ccn} (from {override_path.name}) "
                f"not found in POS catalog\n"
            )
            continue
        rec.update({
            "ccn": ccn,
            "name_pos": h["name"],
            "city": h["city"],
            "state": h["state"],
            "zip5": h["zip"],
            "fac_subtype_code": h["fac_subtype_code"],
            "bed_count": h["bed_count"],
            "match_strategy": match_strategy,
            "match_confidence": 1.0,
            "verified_via": verified_via,
            "verified_date": today,
        })
        applied += 1
    return applied


# --- driver --------------------------------------------------------------

def resolve_vendor(vendor: str, catalog_path: Path, idx: dict, today: str) -> Path:
    if vendor not in VENDOR_BUNDLES:
        sys.exit(f"ERROR: unknown vendor {vendor!r}. Known: {sorted(VENDOR_BUNDLES)}")

    # Pick the newest archived bundle for each stem.
    bundles: list[dict] = []
    for stem in VENDOR_BUNDLES[vendor]:
        matches = sorted(GOLDEN_CROSS_VENDOR.glob(f"{stem}-*.json"))
        if not matches:
            sys.exit(
                f"ERROR: no archive matching {stem}-*.json under tests/golden/cross-vendor/.\n"
                f"Run `python -m tools.fetch_brands {vendor}` first."
            )
        bundles.append(json.loads(matches[-1].read_text()))

    # Walk every bundle's endpoints. Endpoint ids are vendor-globally unique
    # within a single vendor's bundle so a flat list is fine.
    raw: list[dict] = []
    for bundle in bundles:
        raw.extend(iter_endpoint_records(bundle, vendor))

    print(f"\n[{vendor}] {len(raw)} endpoints to resolve")
    print(f"[{vendor}] address coverage: "
          f"state={sum(1 for r in raw if r['state'])}, "
          f"zip5={sum(1 for r in raw if r['zip5'])}, "
          f"npi={sum(1 for r in raw if r['npi'])}")

    resolved = [resolve_one(r, idx) for r in raw]

    # Stamp iron-rule fields per row before applying overrides.
    for row in resolved:
        row.setdefault("vendor", vendor)
        row.setdefault("verified_via", "cms_pos_csv")
        row.setdefault("verified_date", today)

    # Apply LLM judgments first (lower priority), then manual overrides
    # (higher priority — they overwrite LLM picks for the same endpoint_id).
    llm_path = OUT_DIR / f"{vendor}-pos.llm.json"
    manual_path = OUT_DIR / f"{vendor}-pos.manual.json"
    llm_applied = apply_overrides(
        resolved, llm_path, idx, today,
        match_strategy="llm_assisted", verified_via="llm_assisted",
    )
    manual_applied = apply_overrides(
        resolved, manual_path, idx, today,
        match_strategy="manual_override", verified_via="manual_override",
    )

    # Match-rate summary
    n = len(resolved)
    matched_unique = sum(1 for r in resolved if r["ccn"])
    matched_multiple = sum(1 for r in resolved if not r["ccn"] and r.get("candidates"))
    unmatched = sum(1 for r in resolved if not r["ccn"] and not r.get("candidates"))
    pct = (100.0 * matched_unique / n) if n else 0.0
    print(f"[{vendor}] matched unique: {matched_unique}/{n} ({pct:.1f}%)"
          f"  multi-candidate: {matched_multiple}  unmatched: {unmatched}"
          f"  llm overrides: {llm_applied}  manual overrides: {manual_applied}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{vendor}-pos.json"
    payload = {
        "vendor": vendor,
        "captured_date": today,
        "pos_catalog": str(catalog_path.relative_to(REPO_ROOT)),
        "match_summary": {
            "endpoints_total": n,
            "matched_unique": matched_unique,
            "matched_multiple": matched_multiple,
            "unmatched": unmatched,
        },
        "endpoints": resolved,
        "verification": {
            "source_url": str(catalog_path.relative_to(REPO_ROOT)),
            "source_quote": (
                "CMS Provider-of-Services file (hospitals only, "
                "PRVDR_CTGRY_CD=01, active termination code) "
                "matched on (state, zip5, name-token Jaccard)."
            ),
            "verified_via": "cms_pos_csv",
            "verified_date": today,
        },
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"[{vendor}] wrote {out_path.relative_to(REPO_ROOT)}")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=list(VENDOR_BUNDLES))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--captured-date", help="ISO date stamp (default: today)")
    args = ap.parse_args()

    if not args.vendor and not args.all:
        ap.error("pass a vendor identifier or --all")

    catalog_path, catalog = load_latest_catalog()
    catalog_count = catalog.get("facility_count") or catalog.get("hospital_count") or len(catalog.get("hospitals", []))
    print(f"POS catalog: {catalog_path.relative_to(REPO_ROOT)} ({catalog_count} facilities)")
    idx = index_catalog(catalog)

    today = args.captured_date or datetime.date.today().isoformat()

    targets = list(VENDOR_BUNDLES) if args.all else [args.vendor]
    for v in targets:
        resolve_vendor(v, catalog_path, idx, today)
    return 0


if __name__ == "__main__":
    sys.exit(main())
