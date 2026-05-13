"""LLM-assisted disambiguation for ambiguous FHIR endpoint → CCN resolutions.

Reads `data/hospital-overrides/{vendor}-pos.json` (output of
`tools.resolve_endpoints_to_pos`) and asks Claude Haiku 4.5 to pick the best
CCN from each row's `candidates[]` list — or refuse if no candidate fits.

Targets only the rows where the deterministic resolver gave up:
  - `match_strategy == "name_only"` (Epic — no address signal)
  - `ccn is None` AND `candidates` non-empty (Cerner/Meditech ambiguous)

Already-matched rows and unmatched-with-no-candidates rows are skipped: those
are either cleanly resolved or cleanly not-a-hospital, no LLM judgment needed.

Cost guardrails:
  - Haiku 4.5: ~$1/M input, ~$5/M output. ~700 endpoints across all vendors.
  - Per call: ~1800 input + ~80 output tokens → ~$0.0022/endpoint → ~$1.50 total.
  - Per-endpoint local cache keyed by (endpoint_id, candidates_hash, model_id):
    re-runs are free; only new endpoints or newly-changed candidate lists hit
    the API. Cache lives at data/hospital-overrides/.cache/llm-disambiguate/.

Outputs (alongside the deterministic resolver's output):
  - `data/hospital-overrides/{vendor}-pos.llm.json` — `{endpoint_id: ccn}` for
    high+medium-confidence picks. Same shape as `*.manual.json`; the resolver
    picks it up automatically (manual_override still wins over llm_assisted).
  - `data/hospital-overrides/{vendor}-pos.llm.detail.json` — full audit trail
    with confidence + reasoning + token usage for every row processed.

Iron rule: every llm_assisted row carries `verified_via=llm_assisted`,
`verified_date`, AND a stored prompt/model footprint in the detail file.

Requires THE_MAP_ANTHROPIC_API_KEY (or ANTHROPIC_API_KEY) in env / .env.
Install dependencies: `pip install -e .[llm]`

Usage:
    python -m tools.llm_disambiguate cerner
    python -m tools.llm_disambiguate epic --limit 20
    python -m tools.llm_disambiguate meditech --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import sys
import time
from pathlib import Path

from tools._env import REPO_ROOT, load_env
from tools.nucc_to_pos import filter_candidates_by_taxonomy

OUT_DIR = REPO_ROOT / "data" / "hospital-overrides"
EHRS_DIR = REPO_ROOT / "ehrs"
CACHE_DIR = OUT_DIR / ".cache" / "llm-disambiguate"

MODEL = "claude-haiku-4-5"
# Bump when SYSTEM_PROMPT changes; cache key invalidates. The candidates_hash
# already invalidates rows whose candidate set changes (e.g., after a NUCC
# taxonomy filter narrows the candidates), so we don't need to bump on
# pipeline-side changes — only on prompt edits.
SYSTEM_PROMPT_VERSION = "v1"

# Anthropic SDK guidance:
#   - Strict tool use forces the model to return well-typed structured output.
#     The tool's input_schema with additionalProperties:false validates server-side.
#   - cache_control on the system prompt + tools caches the stable prefix; user
#     content (per-endpoint) is the only varying part. Note: Haiku 4.5's minimum
#     cacheable prefix is 4096 tokens — our ~1800-token system prompt is below
#     that, so caching silently no-ops. Code is correct for if/when we move to
#     Sonnet 4.6 (2048-token minimum) or grow the prompt.

SYSTEM_PROMPT = """\
You are disambiguating FHIR healthcare endpoints to specific Medicare-certified hospitals.

CONTEXT
EHR vendors (Epic, Oracle Health/Cerner, MEDITECH) publish public FHIR endpoint registries listing their hospital and provider customers. The published metadata is often insufficient to identify the specific hospital — same names appear across states, hospital systems share names with member facilities, and many endpoints are physician practices or clinics rather than hospitals at all.

Your job: for one endpoint at a time, choose the best matching CMS-certified hospital from a provided candidate list, or refuse if no candidate plausibly fits.

INPUT FIELDS
- vendor: epic, cerner, or meditech
- endpoint_name: the FHIR Endpoint.name or managing Organization.name
- endpoint_address: the FHIR base URL (sometimes contains city/system hints, e.g. "haiku.wacofhc.org")
- observed_city, observed_state, observed_zip5: from the FHIR Organization.address (may be empty for Epic)
- candidates: short list of CMS hospital rows (CCN, name, city, state, zip5) that token-overlap with the endpoint name

DECISION RULES
- HIGH: one candidate is a clear match. Name overlaps strongly AND (observed location agrees OR the candidate's city/state/system clearly appears in the endpoint name) AND no other candidate is competitive.
- MEDIUM: one candidate is the best fit but secondary signals are limited (name matches but you can't independently verify location, or observed_state agrees but you don't know which of two same-state candidates).
- LOW: a candidate is plausible but ambiguous — better to leave for human review than to auto-resolve.
- NONE: no candidate is plausible. The endpoint is likely a physician practice, single-specialty clinic, ambulatory surgery center, urgent care, or other non-hospital provider that simply token-overlaps with a hospital name.

CALIBRATION SIGNALS FOR "NONE"
- Endpoint name contains "Dr.", "MD", "DO", "DPM", "PC", "LLC", "PA", "PLLC" — usually a physician practice.
- Endpoint name says "Foot", "Podiatry", "Dental", "Family Practice", "Primary Care", "Pediatrics" alone — usually a clinic.
- Endpoint name is a non-healthcare entity ("Construction Co.", a person's name, a generic group name).
- All candidates have a different organization type than the endpoint suggests (e.g., endpoint is a clinic, candidates are all hospitals).

OUTPUT RULES
- Return `ccn` as the EXACT string from one of the candidates. NEVER invent a CCN. NEVER return a CCN that does not appear in the candidates list.
- If confidence is "none", return ccn="" (empty string).
- `reason`: one or two sentences citing the specific signals (which candidate field, which endpoint field) that drove the decision. Be concrete; do not include caveats or apologies.

EXAMPLES

Endpoint name: "Boston Children's Hospital", state: MA, zip5: 02115
Candidates: [BOSTON CHILDREN'S HOSPITAL (BOSTON, MA, 02115)]
→ {"ccn": "<that ccn>", "confidence": "high", "reason": "Endpoint name and observed city/state/zip5 exactly match the only candidate."}

Endpoint name: "Children's Hospital Colorado", state: "", zip5: ""
Candidates: [CHILDREN'S HOSPITAL COLORADO (AURORA, CO), CHILDREN'S HOSPITAL COLORADO - COLORADO SPRINGS (COLORADO SPRINGS, CO)]
→ {"ccn": "<aurora ccn>", "confidence": "medium", "reason": "Endpoint name lacks 'Colorado Springs' suffix, suggesting the main Aurora campus over the Springs branch; no observed location to break the tie further."}

Endpoint name: "Family Practice Clinic of Dothan, PA", state: AL, zip5: 36301
Candidates: [JACKSON HOSPITAL & CLINIC INC (MONTGOMERY, AL), NOLAND HOSPITAL DOTHAN II, LLC (DOTHAN, AL)]
→ {"ccn": "", "confidence": "none", "reason": "Endpoint is a physician practice ('Family Practice Clinic ... PA'), not a Medicare-certified hospital; the Dothan zip overlap is incidental."}

Endpoint name: "JE Dunn Construction Co.", state: GA
Candidates: [a few unrelated GA hospitals]
→ {"ccn": "", "confidence": "none", "reason": "Endpoint is a construction company, not a healthcare provider."}
"""


DISAMBIGUATE_TOOL = {
    "name": "record_hospital_match",
    "description": (
        "Record the best CCN match for the FHIR endpoint, or 'none' if no candidate is plausible. "
        "The ccn field MUST be either an empty string (when confidence is 'none') or one of the "
        "exact ccn strings from the candidates list."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "ccn": {
                "type": "string",
                "description": (
                    "Exact CCN from candidates[].ccn, or empty string if confidence is 'none'. "
                    "Do NOT invent a CCN."
                ),
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low", "none"],
                "description": "high/medium → auto-resolve; low → keep for human review; none → not a hospital.",
            },
            "reason": {
                "type": "string",
                "description": "1-2 sentences citing the specific signals that drove the decision.",
            },
        },
        "required": ["ccn", "confidence", "reason"],
        "additionalProperties": False,
    },
}


def candidates_hash(candidates: list[dict]) -> str:
    """Stable hash of the candidate set so a changed POS catalog forces re-query."""
    payload = json.dumps(
        [(c["ccn"], c["name"], c["city"], c["state"], c.get("zip5", "")) for c in candidates],
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def cache_path(endpoint_id: str, c_hash: str, model: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in endpoint_id)[:80]
    # Include SYSTEM_PROMPT_VERSION in the key so prompt edits invalidate the cache.
    return CACHE_DIR / f"{safe}__{c_hash}__{model}__{SYSTEM_PROMPT_VERSION}.json"


def render_user_prompt(row: dict) -> str:
    cand_lines = []
    for i, c in enumerate(row.get("candidates") or [], 1):
        cat_suffix = ""
        if c.get("category_label"):
            cat_suffix = f"  facility_type={c['category_label']!r}"
        cand_lines.append(
            f"  [{i}] ccn={c['ccn']}  name={c['name']!r}  city={c['city']!r}  "
            f"state={c['state']!r}  zip5={c.get('zip5') or ''!r}  score={c.get('score', 0):.2f}"
            f"{cat_suffix}"
        )
    cands_text = "\n".join(cand_lines) if cand_lines else "  (none)"
    taxonomy_line = ""
    if row.get("_taxonomy"):
        taxonomy_line = f"endpoint_nucc_taxonomy: {row['_taxonomy']!r}  (NPPES-derived; candidates have been pre-filtered to compatible facility types)\n"
    return (
        f"vendor: {row.get('vendor', '?')}\n"
        f"endpoint_id: {row['endpoint_id']}\n"
        f"endpoint_name: {row.get('name_observed', '')!r}\n"
        f"endpoint_address: {row.get('endpoint_address', '') or ''!r}\n"
        f"observed_city: {row.get('city') or ''!r}\n"
        f"observed_state: {row.get('state') or ''!r}\n"
        f"observed_zip5: {row.get('zip5') or ''!r}\n"
        f"{taxonomy_line}"
        f"candidates ({len(row.get('candidates') or [])}):\n"
        f"{cands_text}\n\n"
        "Pick the best ccn from candidates, or refuse with confidence=none."
    )


def call_anthropic(client, row: dict) -> dict:
    """Single API call for one endpoint. Returns the tool input dict + usage metadata."""
    user_text = render_user_prompt(row)
    msg = client.messages.create(
        model=MODEL,
        max_tokens=512,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tools=[DISAMBIGUATE_TOOL],
        tool_choice={"type": "tool", "name": "record_hospital_match"},
        messages=[{"role": "user", "content": user_text}],
    )
    tool_use = next((b for b in msg.content if getattr(b, "type", None) == "tool_use"), None)
    if tool_use is None:
        raise RuntimeError(
            f"model returned no tool_use block (stop_reason={msg.stop_reason}); "
            f"content types: {[getattr(b, 'type', '?') for b in msg.content]}"
        )
    return {
        "ccn": tool_use.input.get("ccn", "") or "",
        "confidence": tool_use.input.get("confidence", "none"),
        "reason": tool_use.input.get("reason", ""),
        "model": MODEL,
        "usage": {
            "input_tokens": msg.usage.input_tokens,
            "output_tokens": msg.usage.output_tokens,
            "cache_read_input_tokens": getattr(msg.usage, "cache_read_input_tokens", 0) or 0,
            "cache_creation_input_tokens": getattr(msg.usage, "cache_creation_input_tokens", 0) or 0,
        },
    }


def validate_against_candidates(result: dict, candidates: list[dict]) -> dict:
    """Defense-in-depth: even with strict tool use, never trust a CCN that
    isn't in the candidate set. If the model invented one, downgrade to none."""
    valid_ccns = {c["ccn"] for c in candidates}
    if result["ccn"] and result["ccn"] not in valid_ccns:
        original_reason = result.get("reason", "")
        return {
            **result,
            "ccn": "",
            "confidence": "none",
            "reason": f"INVALIDATED (model returned ccn {result['ccn']!r} not in candidates). Original: {original_reason}",
            "validation_failed": True,
        }
    return result


def disambiguate_one(client, row: dict, *, force_refresh: bool = False) -> tuple[dict, bool]:
    """Returns (result, was_cached)."""
    candidates = row.get("candidates") or []
    if not candidates:
        return {
            "ccn": "", "confidence": "none",
            "reason": "no candidates to choose from",
            "model": "(no-call)",
        }, False

    c_hash = candidates_hash(candidates)
    cp = cache_path(row["endpoint_id"], c_hash, MODEL)

    if cp.exists() and not force_refresh:
        return json.loads(cp.read_text()), True

    result = call_anthropic(client, row)
    result = validate_against_candidates(result, candidates)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps(result, indent=2) + "\n")
    return result, False


def select_rows_for_disambiguation(payload: dict) -> list[dict]:
    """Pick rows the deterministic resolver couldn't auto-resolve but where
    candidates exist. Skip already-matched rows and clean misses."""
    rows: list[dict] = []
    for row in payload.get("endpoints", []):
        if row.get("ccn"):
            continue  # already matched
        if not (row.get("candidates") or []):
            continue  # no candidates → almost certainly not a hospital
        rows.append(row)
    return rows


def load_fleet_taxonomy_map(vendor: str) -> dict[str, str]:
    """Build endpoint_address → NUCC taxonomy code from the published
    production fleet. The taxonomy comes from the NPPES overlay layered in
    by analyze_fleet_drift; missing means we don't have NPPES org identity
    for that endpoint and the LLM filter falls back to "kept_all".

    Keyed by URL because pos.json uses brand-bundle resource IDs while
    production_fleet uses harvest slugs — different schemes. URL is the
    universal join key.
    """
    fleet_path = EHRS_DIR / vendor / "production_fleet.json"
    if not fleet_path.exists():
        return {}
    fleet = json.loads(fleet_path.read_text())
    out: dict[str, str] = {}
    for cluster in fleet.get("capstmt_shape_clusters") or []:
        for ep in cluster.get("endpoints") or []:
            t = ep.get("taxonomy")
            addr = ep.get("address")
            if t and addr:
                out[addr.rstrip("/")] = t
    return out


def apply_nucc_filter(
    rows: list[dict],
    taxonomy_by_address: dict[str, str],
) -> tuple[list[dict], dict[str, int]]:
    """Narrow each row's `candidates[]` to those compatible with the
    endpoint's NPPES taxonomy. Returns (filtered_rows, stats) where stats
    counts the filter outcomes: kept_all / filtered / filtered_empty.

    Rows whose filter empties candidates are dropped from disambiguation —
    a hospital endpoint with no hospital-class candidates is correctly
    classified as "no candidate is plausible," and skipping saves an API
    call where the answer would be `none` anyway.
    """
    stats = {"kept_all": 0, "filtered": 0, "filtered_empty": 0, "no_taxonomy": 0}
    out: list[dict] = []
    for row in rows:
        addr = (row.get("endpoint_address") or "").rstrip("/")
        taxonomy = taxonomy_by_address.get(addr)
        if not taxonomy:
            stats["no_taxonomy"] += 1
            out.append(row)
            continue
        filtered, status = filter_candidates_by_taxonomy(row.get("candidates") or [], taxonomy)
        stats[status] += 1
        if status == "filtered_empty":
            # Don't send to LLM — no compatible candidate exists. Leave the
            # row out (resolver keeps it as unmatched, which is correct).
            continue
        row = {**row, "candidates": filtered, "_taxonomy": taxonomy}
        out.append(row)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("vendor", choices=["cerner", "meditech", "epic"])
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap the number of endpoints sent to the API (cache hits don't count toward this).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Walk the input and print what would be sent; no API calls.")
    ap.add_argument("--force-refresh", action="store_true",
                    help="Ignore the local cache and re-query every endpoint.")
    ap.add_argument("--captured-date", help="ISO date stamp (default: today)")
    args = ap.parse_args()

    load_env(strict=False)
    api_key = os.environ.get("THE_MAP_ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key and not args.dry_run:
        sys.exit(
            "ERROR: set THE_MAP_ANTHROPIC_API_KEY (or ANTHROPIC_API_KEY) in env or .env.\n"
            "       --dry-run skips the API and is safe without a key."
        )

    in_path = OUT_DIR / f"{args.vendor}-pos.json"
    if not in_path.exists():
        sys.exit(
            f"ERROR: {in_path.relative_to(REPO_ROOT)} not found. "
            f"Run `python -m tools.resolve_endpoints_to_pos {args.vendor}` first."
        )
    payload = json.loads(in_path.read_text())

    todo = select_rows_for_disambiguation(payload)
    print(f"[{args.vendor}] {len(todo)} endpoints with candidates to disambiguate "
          f"(of {len(payload.get('endpoints', []))} total)")

    # NUCC taxonomy filter: narrow each row's candidates to the POS facility
    # categories compatible with the endpoint's NPPES taxonomy. Drops rows
    # where no compatible candidate exists — they're correctly unmatchable
    # (asking LLM would just yield "none").
    taxonomy_map = load_fleet_taxonomy_map(args.vendor)
    todo, filter_stats = apply_nucc_filter(todo, taxonomy_map)
    print(
        f"[{args.vendor}] NUCC filter: "
        f"kept_all={filter_stats['kept_all']} (no taxonomy hit or unmapped) | "
        f"filtered={filter_stats['filtered']} (narrowed candidates) | "
        f"filtered_empty={filter_stats['filtered_empty']} (dropped — no compatible candidate) | "
        f"no_taxonomy={filter_stats['no_taxonomy']} (endpoint has no NPPES taxonomy)"
    )
    print(f"[{args.vendor}] {len(todo)} endpoints remain for LLM")

    if args.dry_run:
        for r in todo[:5]:
            print(f"\n--- DRY RUN sample ---\n{render_user_prompt(r)}")
        if len(todo) > 5:
            print(f"\n  ... and {len(todo) - 5} more")
        print(f"\nWould call Anthropic for up to {args.limit or len(todo)} of these.")
        return 0

    client = None
    try:
        import anthropic  # type: ignore
    except ImportError:
        sys.exit("ERROR: pip install -e .[llm]   (the anthropic SDK is needed for this step)")
    client = anthropic.Anthropic(api_key=api_key)

    overrides: dict[str, str] = {}
    detail: list[dict] = []
    cache_hits = 0
    api_calls = 0
    invalidations = 0
    api_calls_remaining = args.limit if args.limit is not None else None

    t0 = time.monotonic()
    for i, row in enumerate(todo, 1):
        c_hash = candidates_hash(row["candidates"])
        cp = cache_path(row["endpoint_id"], c_hash, MODEL)
        cached = cp.exists() and not args.force_refresh

        if not cached and api_calls_remaining is not None and api_calls_remaining <= 0:
            # Reached the --limit on uncached calls; skip the rest.
            continue

        try:
            result, was_cached = disambiguate_one(client, row, force_refresh=args.force_refresh)
        except Exception as e:
            print(f"  ERROR on {row['endpoint_id']}: {e}")
            continue

        if was_cached:
            cache_hits += 1
        else:
            api_calls += 1
            if api_calls_remaining is not None:
                api_calls_remaining -= 1
        if result.get("validation_failed"):
            invalidations += 1

        if result["confidence"] in ("high", "medium") and result["ccn"]:
            overrides[row["endpoint_id"]] = result["ccn"]

        detail.append({
            "endpoint_id": row["endpoint_id"],
            "vendor": args.vendor,
            "name_observed": row.get("name_observed"),
            "candidates": row.get("candidates"),
            **{k: result[k] for k in ("ccn", "confidence", "reason", "model") if k in result},
            "usage": result.get("usage"),
            "validation_failed": result.get("validation_failed", False),
        })

        if i % 25 == 0:
            elapsed = int(time.monotonic() - t0)
            print(f"  [{i:>4}/{len(todo)}]  cache_hits={cache_hits}  api_calls={api_calls}  "
                  f"invalidations={invalidations}  elapsed={elapsed}s")

    today = args.captured_date or datetime.date.today().isoformat()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Override file (same shape as *.manual.json — picked up by resolve_endpoints_to_pos)
    overrides_path = OUT_DIR / f"{args.vendor}-pos.llm.json"
    overrides_path.write_text(json.dumps(overrides, indent=2, sort_keys=True) + "\n")

    # Detail audit trail
    detail_path = OUT_DIR / f"{args.vendor}-pos.llm.detail.json"
    detail_payload = {
        "vendor": args.vendor,
        "captured_date": today,
        "model": MODEL,
        "system_prompt_version": SYSTEM_PROMPT_VERSION,
        "summary": {
            "endpoints_considered": len(todo),
            "endpoints_processed": len(detail),
            "cache_hits": cache_hits,
            "api_calls": api_calls,
            "invalidations": invalidations,
            "high_confidence": sum(1 for d in detail if d["confidence"] == "high"),
            "medium_confidence": sum(1 for d in detail if d["confidence"] == "medium"),
            "low_confidence": sum(1 for d in detail if d["confidence"] == "low"),
            "none_confidence": sum(1 for d in detail if d["confidence"] == "none"),
            "auto_resolved": len(overrides),
        },
        "endpoints": detail,
    }
    detail_path.write_text(json.dumps(detail_payload, indent=2) + "\n")

    summary = detail_payload["summary"]
    print(
        f"\n[{args.vendor}] done.\n"
        f"  high  : {summary['high_confidence']}\n"
        f"  medium: {summary['medium_confidence']}\n"
        f"  low   : {summary['low_confidence']}\n"
        f"  none  : {summary['none_confidence']}\n"
        f"  auto-resolved (high+medium): {summary['auto_resolved']}\n"
        f"  cache hits: {cache_hits}  api calls: {api_calls}  invalidations: {invalidations}\n"
        f"  wrote {overrides_path.relative_to(REPO_ROOT)}\n"
        f"  wrote {detail_path.relative_to(REPO_ROOT)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
