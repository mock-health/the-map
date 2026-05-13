"""Rebuild `{vendor}-pos.llm.json` from the local LLM cache.

A subtle workflow hazard: re-running `tools.llm_disambiguate` overwrites
`{vendor}-pos.llm.json` with only the picks the LLM made on THE CURRENT
candidate set. If a prior run wrote 65 overrides and those overrides got
applied to `pos.json` (gaining CCNs), the next LLM run sees fewer
unmatched rows to disambiguate — and writes a smaller override file. The
prior 65 picks aren't gone from the *applied* state (they're already
baked into pos.json's CCN values), but the override file becomes a
misleading snapshot that doesn't reflect the cumulative LLM work.

This recovery tool walks the local cache at
`data/hospital-overrides/.cache/llm-disambiguate/` and emits an override
file containing every high+medium-confidence pick the LLM has ever made
for the named vendor. Run it after any `llm_disambiguate` run if you
want the override file to reflect the FULL cumulative LLM judgment, not
just the most recent run's increment.

Cache filename schema (from tools.llm_disambiguate.cache_path):
  {endpoint_id_safe}__{candidates_hash}__{model}__{prompt_version}.json
We treat each cache file as authoritative for its (endpoint, candidates)
combo. When multiple cache files exist for the same endpoint (different
candidate sets across runs), we keep the most recently mtime-touched.

Usage:
    python -m tools.recover_llm_overrides cerner
    python -m tools.recover_llm_overrides --all
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "data" / "hospital-overrides"
CACHE_DIR = OUT_DIR / ".cache" / "llm-disambiguate"

VENDORS = ("epic", "cerner", "meditech")


def _endpoint_id_from_filename(name: str) -> str | None:
    """Cache file: `<id_safe>__<hash>__<model>__<version>.json`."""
    if not name.endswith(".json"):
        return None
    return name.split("__", 1)[0]


def vendor_endpoint_ids(vendor: str) -> set[str]:
    """The endpoint_ids the vendor's pos.json knows about (resolver-emitted
    rows). Cache files for foreign vendors are ignored — same cache dir,
    distinct namespaces.
    """
    pos_path = OUT_DIR / f"{vendor}-pos.json"
    if not pos_path.exists():
        sys.exit(f"ERROR: {pos_path.relative_to(REPO_ROOT)} not found")
    pos = json.loads(pos_path.read_text())
    return {r["endpoint_id"] for r in pos.get("endpoints", []) if r.get("endpoint_id")}


def collect_overrides(vendor: str) -> dict[str, str]:
    """Walk cache; collect endpoint_id → ccn for high+medium picks. When the
    same endpoint has multiple cache files (different candidate sets),
    keep the most recently modified one — it's the freshest LLM judgment.
    """
    if not CACHE_DIR.exists():
        sys.exit(f"ERROR: no cache dir at {CACHE_DIR}")
    eligible_ids = vendor_endpoint_ids(vendor)
    print(f"[{vendor}] cache dir: {len(list(CACHE_DIR.glob('*.json')))} files | "
          f"vendor pos.json knows {len(eligible_ids)} endpoint_ids")

    by_endpoint: dict[str, tuple[float, str]] = {}  # endpoint_id → (mtime, ccn)
    skipped_foreign = 0
    for cache_file in CACHE_DIR.glob("*.json"):
        ep_id = _endpoint_id_from_filename(cache_file.name)
        if not ep_id:
            continue
        # Cache filenames truncate endpoint_id to 80 chars + replace
        # non-alnum with `_`. To match against vendor's actual IDs we'd
        # need to reproduce that transform. For now, accept any cache file
        # whose stem PREFIX matches the safe-form of any vendor id.
        if not any(
            "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in v_id)[:80] == ep_id
            for v_id in eligible_ids
        ):
            skipped_foreign += 1
            continue
        try:
            payload = json.loads(cache_file.read_text())
        except json.JSONDecodeError:
            continue
        if payload.get("confidence") not in ("high", "medium"):
            continue
        ccn = (payload.get("ccn") or "").strip()
        if not ccn:
            continue
        mt = cache_file.stat().st_mtime
        # Map the cache-file safe-id back to the canonical endpoint_id.
        for v_id in eligible_ids:
            if "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in v_id)[:80] == ep_id:
                prior = by_endpoint.get(v_id)
                if prior is None or mt > prior[0]:
                    by_endpoint[v_id] = (mt, ccn)
                break

    overrides = {ep_id: ccn for ep_id, (_mt, ccn) in by_endpoint.items()}
    print(f"[{vendor}] recovered {len(overrides)} high+medium overrides "
          f"(skipped {skipped_foreign} cache files matching other vendors)")
    return overrides


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", nargs="?", choices=VENDORS)
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if not args.vendor and not args.all:
        ap.error("specify a vendor or --all")

    vendors = list(VENDORS) if args.all else [args.vendor]
    for v in vendors:
        overrides = collect_overrides(v)
        out_path = OUT_DIR / f"{v}-pos.llm.json"
        out_path.write_text(json.dumps(overrides, indent=2, sort_keys=True) + "\n")
        print(f"  wrote {out_path.relative_to(REPO_ROOT)} ({len(overrides)} entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
