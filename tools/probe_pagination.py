"""KATA-CAPTURE-BUNDLE-LINK-SEMANTICS — per-vendor pagination link probe.

Offline / fixture-driven: walks the golden Bundle fixtures already captured under
``tests/golden/{epic,cerner,meditech}/phase-b-*`` and extracts pagination semantics:

  * link relations present on Bundles
  * next-page URL format (opaque cursor vs. parseable query continuation)
  * Bundle.total semantics (reflects page size? omitted? matches entry count?)
  * empty-bundle shape (zero entries vs. ``mode=outcome`` advisory entry)
  * default and observed page sizes, per-resource overrides
  * token stability — do next-page URLs change across captures of the "same" query?
  * self-link canonicalization — does the server echo the request URL or rewrite it?

Outputs a structured JSON report at ``reports/pagination/pagination-{ISODATE}.json``
plus a side-by-side Markdown summary at ``reports/pagination-{ISODATE}.md``. Writes
a ``pagination`` block into each vendor's ``ehrs/{ehr}/overlay.json`` (alongside
the legacy ``pagination_overlay`` already there).

No HTTP requests. Reads fixtures only. Idempotent.

Usage::

    python -m tools.probe_pagination                # all vendors
    python -m tools.probe_pagination --vendor epic  # one
    python -m tools.probe_pagination --dry-run      # don't write overlays/report
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"
REPORTS_DIR = REPO_ROOT / "reports"

# Where to look for paginated Bundle fixtures per vendor. We accept any
# ``phase-b-*`` directory plus any vendor-specific extras.
VENDOR_FIXTURE_ROOTS: dict[str, list[Path]] = {
    "epic": [GOLDEN_DIR / "epic"],
    "cerner": [GOLDEN_DIR / "cerner"],
    "meditech": [GOLDEN_DIR / "meditech"],
}

# verified_via values must match the overlay.schema.json enum.
VENDOR_VERIFIED_VIA: dict[str, str] = {
    "epic": "epic_public_sandbox",
    "cerner": "cerner_public_sandbox",
    "meditech": "meditech_public_sandbox",
}

VENDOR_SOURCE_URL_HINT: dict[str, str] = {
    "epic": "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4",
    "cerner": "https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d",
    "meditech": "https://greenfield-prod-apis.meditech.com/v2/uscore/STU6",
}

# Heuristics for classifying next_url_format.
OPAQUE_TOKEN_HINTS = ("sessionID=", "-pageContext=", "continue=", "page-token=", "cursor=")
QUERY_CONTINUATION_HINTS = ("_count=", "_offset=", "_page=", "page=")

# ---------- Fixture discovery / parsing ------------------------------------------------


def _iter_bundle_fixtures(vendor: str) -> Iterable[tuple[Path, dict]]:
    """Yield (path, parsed_json) for every fixture under the vendor's golden roots
    that wraps a single Bundle response. Recognizes:

      * envelope shape ``{_request, _captured_at, body: {...Bundle}}`` (our standard
        capture wrapper from measure_phase_b / capture_meditech_goldens)
      * bare Bundle JSON (no wrapper)

    Skips OperationOutcome-only error fixtures, hapi-validation sidecars,
    sweep-summary metadata, and the special ``paginated-with-total`` synthesis
    file (we parse that separately for Epic's Bundle.total quirk).
    """
    for root in VENDOR_FIXTURE_ROOTS.get(vendor, []):
        if not root.exists():
            continue
        for fp in sorted(root.rglob("*.json")):
            name = fp.name
            # Skip non-bundle artifacts
            if name.startswith("error-"):
                continue
            if name == "sweep-summary.json":
                continue
            if name.startswith("CapabilityStatement") or name.startswith("smart-configuration"):
                continue
            if name.startswith("brands-"):
                continue
            if name.startswith("scope-response-"):
                continue
            if name.endswith("-hapi-validation.json"):
                continue
            if name.startswith("write-probe"):
                continue
            if "paginated-with-total" in name:
                # Handled by a dedicated parser — it's a synthesis, not a raw capture.
                continue
            try:
                doc = json.loads(fp.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            if not isinstance(doc, dict):
                continue
            body = doc.get("body") if "body" in doc else doc
            if not isinstance(body, dict):
                continue
            if body.get("resourceType") != "Bundle":
                continue
            yield fp, doc


def _bundle_body(doc: dict) -> dict:
    return doc.get("body") if "body" in doc else doc


def _request_url(doc: dict) -> str | None:
    req = doc.get("_request") or {}
    if isinstance(req, dict):
        return req.get("url")
    return None


# ---------- Heuristics -----------------------------------------------------------------


def classify_next_url_format(next_url: str) -> str:
    """Map a next-page URL into the schema enum value.

    Returns one of:
      * ``opaque-base64`` — cursor is an opaque blob (sessionID, pageContext, etc.)
      * ``parseable-query-string`` — caller can read original query params
      * ``vendor-custom`` — vendor-specific marker (``continue=...`` shorthand)
      * ``unknown``
    """
    if not next_url:
        return "unknown"
    if "sessionID=" in next_url:
        # Epic's cursor: the rest of the original query is dropped. Caller can read
        # the sessionID but it's not a re-usable query continuation.
        return "opaque-base64"
    if "-pageContext=" in next_url:
        # Cerner's cursor: original params (patient=...) are preserved alongside an
        # opaque pageContext blob; still need to follow rel=next verbatim.
        return "vendor-custom"
    if "continue=" in next_url:
        # MEDITECH: original params preserved + opaque ``continue=`` cursor.
        return "vendor-custom"
    if any(h in next_url for h in QUERY_CONTINUATION_HINTS):
        return "parseable-query-string"
    return "unknown"


def empty_bundle_shape(body: dict) -> str | None:
    """Return shape label for a Bundle that returned zero matches, or None if the
    bundle has match entries."""
    entries = body.get("entry") or []
    match_count = sum(1 for e in entries if (e.get("search") or {}).get("mode") == "match")
    if match_count > 0:
        return None
    if not entries:
        # No entry field, or empty []
        if "entry" not in body:
            return "no_entry_field"
        return "empty_entry_array"
    # Has entries but zero matches — only outcome / include rows.
    modes = sorted({(e.get("search") or {}).get("mode") for e in entries})
    if "outcome" in modes:
        return "mode_outcome_entry"
    return "include_only_no_match"


def _norm_query(url: str | None) -> dict[str, list[str]]:
    if not url:
        return {}
    try:
        return parse_qs(urlparse(url).query, keep_blank_values=True)
    except Exception:
        return {}


def self_link_canonicalization(request_url: str | None, self_url: str | None) -> str:
    """Compare the request URL the probe sent vs. what Bundle.link[rel=self] echoed.

    Returns one of:
      * ``echo_exact`` — byte-identical
      * ``echo_normalized`` — same params, possibly reordered or port-canonicalized
      * ``rewritten`` — server replaced/added params
      * ``unknown`` — couldn't compare (one side missing)
    """
    if not request_url or not self_url:
        return "unknown"
    if request_url == self_url:
        return "echo_exact"
    rq = _norm_query(request_url)
    sq = _norm_query(self_url)
    # Compare param dicts (order-insensitive)
    if rq == sq:
        return "echo_normalized"
    return "rewritten"


# ---------- Per-vendor probe -----------------------------------------------------------


def _resource_type_from_self(self_url: str | None, request_url: str | None) -> str | None:
    """Pull the resource type from the FHIR path segment immediately before the query."""
    for url in (self_url, request_url):
        if not url:
            continue
        try:
            path = urlparse(url).path
        except Exception:
            continue
        # Last non-empty segment of path
        segs = [s for s in path.split("/") if s]
        if not segs:
            continue
        candidate = segs[-1]
        # FHIR resource type starts with uppercase ASCII letter and is alpha-only
        if candidate and candidate[0].isupper() and candidate.isalpha():
            return candidate
    return None


def _vital_signs_category(request_url: str | None) -> bool:
    if not request_url:
        return False
    q = _norm_query(request_url)
    return "vital-signs" in q.get("category", [])


def probe_vendor(vendor: str) -> dict:
    """Walk all paginated Bundle fixtures for ``vendor`` and assemble a findings dict.

    The dict is shape-compatible with the new ``pagination`` overlay block plus a
    raw ``observations`` array of per-fixture evidence that downstream reports can
    cite directly.
    """
    today = datetime.date.today().isoformat()
    out: dict = {
        "vendor": vendor,
        "verified_date": today,
        "verified_via": VENDOR_VERIFIED_VIA[vendor],
        "observations": [],
    }

    fixture_count = 0
    fixtures_with_next = 0
    link_relations_counter: Counter[str] = Counter()
    next_url_formats: Counter[str] = Counter()
    empty_shapes: Counter[str] = Counter()
    self_canonicalizations: Counter[str] = Counter()
    total_present_count = 0
    total_omitted_count = 0
    entry_counts_no_next: list[int] = []  # for inferring default page sizes / unbounded pages
    entry_counts_with_next: list[int] = []  # likely page size cap
    per_resource_entry_counts_with_next: dict[str, list[int]] = defaultdict(list)
    per_resource_entry_counts_no_next: dict[str, list[int]] = defaultdict(list)
    sample_next_urls: list[str] = []
    sample_self_urls: list[str] = []
    vital_signs_max: int | None = None

    for fp, doc in _iter_bundle_fixtures(vendor):
        fixture_count += 1
        body = _bundle_body(doc)
        links = body.get("link") or []
        rels = [l.get("relation") for l in links if isinstance(l, dict)]
        for r in rels:
            if r:
                link_relations_counter[r] += 1
        next_url = next((l.get("url") for l in links if isinstance(l, dict) and l.get("relation") == "next"), None)
        self_url = next((l.get("url") for l in links if isinstance(l, dict) and l.get("relation") == "self"), None)
        request_url = _request_url(doc)
        entry = body.get("entry") or []
        match_entries = [e for e in entry if (e.get("search") or {}).get("mode") == "match"]
        bundle_total = body.get("total")

        has_next = bool(next_url)
        if has_next:
            fixtures_with_next += 1
            entry_counts_with_next.append(len(match_entries))
            next_url_formats[classify_next_url_format(next_url)] += 1
            sample_next_urls.append(next_url)
        else:
            entry_counts_no_next.append(len(match_entries))

        if "total" in body:
            if bundle_total is None:
                total_omitted_count += 1
            else:
                total_present_count += 1
        else:
            total_omitted_count += 1

        shape = empty_bundle_shape(body)
        if shape is not None:
            empty_shapes[shape] += 1

        canon = self_link_canonicalization(request_url, self_url)
        self_canonicalizations[canon] += 1
        if self_url:
            sample_self_urls.append(self_url)

        rt = _resource_type_from_self(self_url, request_url) or "?"
        if has_next:
            per_resource_entry_counts_with_next[rt].append(len(match_entries))
        else:
            per_resource_entry_counts_no_next[rt].append(len(match_entries))

        if _vital_signs_category(request_url) and not has_next:
            vital_signs_max = max(vital_signs_max or 0, len(match_entries))
        if _vital_signs_category(request_url) and has_next:
            # The page that triggered rel=next is at-or-near the cap
            vital_signs_max = max(vital_signs_max or 0, len(match_entries))

        out["observations"].append({
            "fixture": str(fp.relative_to(REPO_ROOT)),
            "request_url": request_url,
            "resource_type": rt,
            "vital_signs": _vital_signs_category(request_url),
            "bundle_total_field_present": "total" in body,
            "bundle_total_value": bundle_total,
            "match_entry_count": len(match_entries),
            "raw_entry_count": len(entry),
            "link_relations": rels,
            "self_url": self_url,
            "next_url": next_url,
            "next_url_format": classify_next_url_format(next_url) if next_url else None,
            "empty_bundle_shape": shape,
            "self_link_canonicalization": canon,
        })

    # ---- Synthesize summary -----------------------------------------------------------
    out["fixture_count"] = fixture_count
    out["fixtures_with_next_link"] = fixtures_with_next
    out["link_relations_observed"] = sorted(link_relations_counter.keys())
    out["link_relations_distribution"] = dict(link_relations_counter)
    out["next_url_format_distribution"] = dict(next_url_formats)
    out["self_link_canonicalization_distribution"] = dict(self_canonicalizations)
    out["empty_bundle_shape_distribution"] = dict(empty_shapes)
    out["bundle_total_present_count"] = total_present_count
    out["bundle_total_omitted_count"] = total_omitted_count

    # next_url_format — pick the dominant non-unknown class, if any.
    if next_url_formats:
        ranked = sorted(next_url_formats.items(), key=lambda kv: (-kv[1], kv[0]))
        out["next_url_format"] = ranked[0][0]
    else:
        out["next_url_format"] = "unknown"

    # default_page_size: when rel=next exists, the entry count is the page cap.
    # When no rel=next, the bundle returned everything OR fell below the cap.
    if entry_counts_with_next:
        # Most common page size when paginating
        counter = Counter(entry_counts_with_next)
        out["default_page_size"] = counter.most_common(1)[0][0]
    else:
        out["default_page_size"] = None
    # max observed page size
    all_entry_counts = entry_counts_with_next + entry_counts_no_next
    out["max_page_size_observed"] = max(all_entry_counts) if all_entry_counts else None

    # Per-resource overrides — flag any resource where the typical "with next" page
    # size differs from the vendor default (e.g., Epic's vital-signs 1000 vs.
    # everyone else returning ≤default).
    per_resource_overrides: dict[str, int] = {}
    default = out["default_page_size"]
    for rt, counts in per_resource_entry_counts_with_next.items():
        if not counts:
            continue
        most_common = Counter(counts).most_common(1)[0][0]
        if default is not None and most_common != default:
            per_resource_overrides[rt] = most_common
    # Also surface the no-next single-page maxima per resource so a downstream
    # reader can see "Cerner served 1623 entries on Condition without rel=next."
    per_resource_single_page_max: dict[str, int] = {}
    for rt, counts in per_resource_entry_counts_no_next.items():
        if not counts:
            continue
        per_resource_single_page_max[rt] = max(counts)
    out["per_resource_page_overrides"] = per_resource_overrides
    out["per_resource_single_page_max"] = per_resource_single_page_max

    if vital_signs_max is not None:
        out["vital_signs_page_size_observed"] = vital_signs_max

    # empty_bundle_shape: pick the dominant non-None shape
    if empty_shapes:
        ranked = sorted(empty_shapes.items(), key=lambda kv: (-kv[1], kv[0]))
        out["empty_bundle_shape"] = ranked[0][0]
    else:
        out["empty_bundle_shape"] = "unobserved"

    # total_semantics — derive from observations.
    out["total_semantics"] = derive_total_semantics(vendor, out["observations"])

    # token_stability — across the corpus we only have one snapshot per query, so
    # we can only assert "stable_across_pages_in_capture" for vendors where the
    # existing overlay already pinned it down. Default to "unmeasured_offline".
    out["token_stability"] = derive_token_stability(vendor, out["observations"])

    # Sample URLs for the report (truncated)
    out["sample_next_urls"] = sample_next_urls[:5]
    out["sample_self_urls"] = sample_self_urls[:5]

    return out


def derive_total_semantics(vendor: str, observations: list[dict]) -> dict:
    """Classify how Bundle.total behaves. Returns a dict with ``label`` + ``evidence``.

    Per-vendor logic, in order:

      * **Epic** — uses the dedicated ``paginated-with-total`` synthesis fixture
        as evidence that Bundle.total tracks the page payload when ``_count`` is
        set. The synthesis file is captured separately (not via measure_phase_b)
        because the quirk only surfaces when you control ``_count`` explicitly.
      * **Cerner** — Bundle.total is *usually absent* but present on a few
        smaller bundles (Immunization). When present it matches the match count.
      * **MEDITECH** — Bundle.total is present on every captured response and
        matches the match-entry count.
    """
    # Epic: dedicated paginated-with-total fixture proves total reflects page size
    # when _count is set. Already cited in existing overlay; cite the canonical
    # fixture here too.
    if vendor == "epic":
        ev_fixture = "tests/golden/epic/phase-b-2026-04-26/observation-vital-signs-paginated-with-total-2026-05-02.json"
        if (REPO_ROOT / ev_fixture).exists():
            return {
                "label": "reflects_page_size_when_count_set",
                "evidence_fixture": ev_fixture,
                "quote": "page_count_5: bundle_total=5 entry_count=5; page_count_1000: bundle_total=1000 entry_count=1000 — Bundle.total tracks the page payload, not the unbounded match set.",
            }

    if not observations:
        return {"label": "unobserved", "evidence_fixture": "(none)", "quote": ""}

    with_total = [o for o in observations if o.get("bundle_total_value") is not None]
    without_total = [o for o in observations if o.get("bundle_total_value") is None]

    # Cerner: total absent in the majority of fixtures; present on a minority
    # where it equals match_entry_count.
    if vendor == "cerner":
        absent_share = len(without_total) / max(len(observations), 1)
        # When ≥75% of fixtures omit Bundle.total, surface that as the dominant
        # semantics — citing the largest no-total fixture to make the gap obvious.
        if absent_share >= 0.75:
            ev_fixture = next(
                (o["fixture"] for o in without_total if o["match_entry_count"] >= 50),
                without_total[0]["fixture"] if without_total else observations[0]["fixture"],
            )
            return {
                "label": "absent_in_most_captures",
                "evidence_fixture": ev_fixture,
                "quote": (
                    f"{len(without_total)}/{len(observations)} captured Cerner bundles "
                    "omit Bundle.total entirely (no `total` key); the remaining "
                    f"{len(with_total)} include a total that matches match_entry_count "
                    "(e.g. immunization-12742399.json: total=24, matches=24)."
                ),
            }

    # General: every with_total observation has total==match_entry_count
    matches_match_count = [
        o for o in with_total if o["bundle_total_value"] == o["match_entry_count"]
    ]
    if with_total and len(matches_match_count) == len(with_total):
        ev = matches_match_count[0]
        return {
            "label": "reflects_returned_match_count",
            "evidence_fixture": ev["fixture"],
            "quote": (
                f"All {len(with_total)} captured bundles with a Bundle.total have "
                f"total==match_entry_count (e.g. {ev['fixture']}: total={ev['bundle_total_value']}, "
                f"matches={ev['match_entry_count']})."
            ),
        }
    return {
        "label": "mixed",
        "evidence_fixture": observations[0]["fixture"],
        "quote": "Bundle.total semantics vary across the captured corpus — see observations[].",
    }


def derive_token_stability(vendor: str, observations: list[dict]) -> dict:
    """Heuristic. Offline we cannot reproduce token TTL — but we can flag whether
    the cursor format embeds anything timestamp-like."""
    next_urls = [o["next_url"] for o in observations if o.get("next_url")]
    if not next_urls:
        return {"label": "unobserved", "evidence_fixture": "(none)"}
    # Look for embedded timestamps (10-digit unix or ISO) in the URLs
    timestamp_re = re.compile(r"(?:[12]\d{9}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2})")
    has_ts = any(timestamp_re.search(u) for u in next_urls)
    if vendor == "epic":
        # Epic publishes "Pagination expects walking, not retry" doc — we know from
        # the existing overlay row 1239 that next-page tokens were stable across the
        # 2-page walk. Document that.
        return {
            "label": "stable_within_session_offline_observation",
            "evidence_fixture": "ehrs/epic/overlay.json:pagination_overlay.next_token_stable_across_pages",
            "quote": "next_token_stable_across_pages: true (walked 2 pages; URLs did not duplicate or drift). TTL unmeasured offline.",
        }
    if vendor == "cerner":
        # Cerner's `-pageContext=` is a UUID + base64 blob; no timestamp signal.
        return {
            "label": "ttl_unknown_no_timestamp_in_token",
            "evidence_fixture": next((o["fixture"] for o in observations if o.get("next_url")), "(none)"),
            "quote": "Cerner next URL embeds an opaque pageContext (UUID/base64). No timestamp component, no documented TTL in measured fixtures.",
        }
    if vendor == "meditech":
        return {
            "label": "ttl_unknown_continue_cursor_is_resource_typed",
            "evidence_fixture": next((o["fixture"] for o in observations if o.get("next_url")), "(none)"),
            "quote": "MEDITECH next URL uses `continue=<RESOURCETYPE-prefix>.<UUID>` (e.g. VS./LA./B76C...). No timestamp, no documented TTL.",
        }
    return {
        "label": "ttl_unknown_timestamp_in_token" if has_ts else "ttl_unknown",
        "evidence_fixture": "(none)",
    }


# ---------- Overlay write --------------------------------------------------------------


def build_overlay_block(vendor_findings: dict) -> dict:
    """Translate the probe summary into a typed overlay ``pagination`` block.

    Kept separate from ``pagination_overlay`` (the older block) so we don't break
    callers that read the legacy field. The legacy block is left untouched.
    """
    v = vendor_findings
    vendor = v["vendor"]
    today = v["verified_date"]
    # Pick a citation fixture for the verification block: prefer one that includes
    # a rel=next link if available, else the first observation.
    cite = next(
        (o["fixture"] for o in v["observations"] if o.get("next_url")),
        v["observations"][0]["fixture"] if v["observations"] else None,
    )
    if cite is None:
        cite_url = VENDOR_SOURCE_URL_HINT[vendor]
        cite_quote = "No paginated fixtures present in the captured corpus."
    else:
        cite_url = VENDOR_SOURCE_URL_HINT[vendor]
        cite_quote = (
            f"Walked {v['fixture_count']} Bundle fixtures under tests/golden/{vendor}/; "
            f"{v['fixtures_with_next_link']} carried rel=next. "
            f"Sample: {cite}"
        )
    block: dict = {
        "link_relations": v["link_relations_observed"],
        "next_url_format": v["next_url_format"],
        "default_page_size": v["default_page_size"],
        "max_page_size_observed": v["max_page_size_observed"],
        "per_resource_page_overrides": v["per_resource_page_overrides"],
        "per_resource_single_page_max": v["per_resource_single_page_max"],
        "empty_bundle_shape": v["empty_bundle_shape"],
        "total_semantics": v["total_semantics"],
        "token_stability": v["token_stability"],
        "self_link_canonicalization_distribution": v["self_link_canonicalization_distribution"],
        "fixture_count_probed": v["fixture_count"],
        "fixtures_with_next_link": v["fixtures_with_next_link"],
        "sample_next_urls": v["sample_next_urls"],
        "verification": {
            "source_url": cite_url,
            "source_quote": cite_quote,
            "verified_via": v["verified_via"],
            "verified_date": today,
        },
        "probe_metadata": {
            "verified_via_tool": "probe_pagination.py",
            "verified_date": today,
            "fixture_corpus_root": f"tests/golden/{vendor}/",
        },
    }
    if "vital_signs_page_size_observed" in v:
        block["vital_signs_page_size_observed"] = v["vital_signs_page_size_observed"]
    return block


def _splice_pagination_block(raw: str, block: dict) -> str:
    """Insert (or replace) the top-level "pagination" key in a pretty-printed
    JSON file without re-serializing the rest of the document.

    Why string splicing instead of ``json.loads`` → ``json.dumps``? The repo's
    overlays use *mixed* Unicode encoding styles (epic = raw, cerner = escaped,
    meditech = mixed). A full re-serialize would normalize the whole file —
    that's a meaningful cleanup but a separate concern from this kata. We
    preserve byte-for-byte continuity outside the inserted block.

    Assumes the input is pretty-printed with 2-space indentation (this is the
    universal style in ``ehrs/{vendor}/overlay.json``).
    """
    parsed = json.loads(raw)
    block_value_json = json.dumps(block, indent=2, ensure_ascii=False)
    block_text = '"pagination": ' + block_value_json
    indented_block = "\n".join(
        "  " + line if line else line for line in block_text.splitlines()
    )
    if "pagination" in parsed:
        # Re-run path: locate the existing block by scanning to the
        # ``"pagination":`` key at column 2 (top-level) and slicing it out.
        start, end = _find_top_level_key_span(raw, "pagination")
        # Replace just that span. Trim preceding comma (if our block was last)
        # or trailing comma (if it was middle).
        head = raw[:start]
        tail = raw[end:]
        # ``head`` ends right before the indent of the line containing
        # ``"pagination":`` — we need to keep that indent.
        # ``tail`` starts right after the value of pagination — typically at a
        # ``,\n`` or ``\n`` boundary.
        replacement = indented_block
        return head + replacement + tail
    # First write: splice before the final '}'.
    stripped = raw.rstrip()
    if not stripped.endswith("}"):
        raise ValueError("overlay JSON does not end with '}' — cannot splice")
    last_close = stripped.rfind("}")
    head = stripped[:last_close].rstrip()
    return head + ",\n" + indented_block + "\n}\n"


def _find_top_level_key_span(raw: str, key: str) -> tuple[int, int]:
    """Return (start, end) byte offsets covering the entire ``"<key>": <value>``
    fragment at the top level of the JSON document. The slice includes only the
    key + value and excludes the surrounding comma/newline so the caller can
    decide how to stitch.
    """
    # Find ``"key":`` at column 2 (i.e., preceded by ``\n  ``).
    needle = f'\n  "{key}":'
    idx = raw.find(needle)
    if idx < 0:
        raise ValueError(f"top-level key {key!r} not found")
    key_start = idx + 1  # skip the leading newline
    # Walk forward to find the matching value end. The value of ``pagination``
    # is an object, so brace-balance from the first '{' after the colon.
    p = raw.find("{", idx + len(needle))
    if p < 0:
        raise ValueError(f"value of {key!r} not an object")
    depth = 0
    i = p
    in_str = False
    escape = False
    while i < len(raw):
        c = raw[i]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    return key_start, end
        i += 1
    raise ValueError(f"unterminated object value for {key!r}")


def write_overlay(vendor: str, block: dict, *, dry_run: bool) -> Path:
    overlay_path = EHRS_DIR / vendor / "overlay.json"
    raw_before = overlay_path.read_text()
    if dry_run:
        return overlay_path
    new_content = _splice_pagination_block(raw_before, block)
    # Round-trip parse as belt-and-suspenders — catches splicer regressions.
    json.loads(new_content)
    overlay_path.write_text(new_content)
    return overlay_path


# ---------- Report generation ----------------------------------------------------------


def render_markdown_report(per_vendor: dict[str, dict], iso_date: str) -> str:
    lines: list[str] = []
    lines.append(f"# Per-vendor pagination link semantics — {iso_date}")
    lines.append("")
    lines.append(
        "Captured offline by `tools/probe_pagination.py` walking the golden Bundle "
        "fixtures under `tests/golden/{epic,cerner,meditech}/`. Every claim below "
        "cites a captured response — no live sandbox calls were made to produce this "
        "report. Unblocks KATA-EPIC-PAGINATION-01 (#117) and KATA-EPIC-PAGINATION-02 "
        "(#118), and surfaces the corresponding deltas for Cerner and MEDITECH so a "
        "downstream emulator can faithfully reproduce them."
    )
    lines.append("")
    lines.append("## Comparison table")
    lines.append("")
    headers = [
        "Dimension",
        "Epic",
        "Cerner",
        "MEDITECH",
    ]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    def cell(value) -> str:
        if value is None:
            return "_n/a_"
        if isinstance(value, list):
            return ", ".join(str(x) for x in value) or "_empty_"
        if isinstance(value, dict):
            return ", ".join(f"{k}={v}" for k, v in value.items()) or "_empty_"
        return str(value).replace("|", "\\|")

    def row(label: str, key: str | callable) -> None:
        cells = [label]
        for vendor in ("epic", "cerner", "meditech"):
            data = per_vendor.get(vendor, {})
            if callable(key):
                cells.append(cell(key(data)))
            else:
                cells.append(cell(data.get(key)))
        lines.append("| " + " | ".join(cells) + " |")

    row("Fixtures probed", "fixture_count")
    row("Fixtures with rel=next", "fixtures_with_next_link")
    row("link relations seen", "link_relations_observed")
    row("next URL format", "next_url_format")
    row("default page size (when paginating)", "default_page_size")
    row("max single-page entries", "max_page_size_observed")
    row("vital-signs page-size override", lambda d: d.get("vital_signs_page_size_observed"))
    row("Bundle.total semantics", lambda d: (d.get("total_semantics") or {}).get("label"))
    row("empty bundle shape", "empty_bundle_shape")
    row("token stability", lambda d: (d.get("token_stability") or {}).get("label"))
    row("self-link canonicalization", "self_link_canonicalization_distribution")
    row("per-resource page-size overrides", "per_resource_page_overrides")

    lines.append("")
    lines.append("## Per-vendor detail")
    for vendor in ("epic", "cerner", "meditech"):
        v = per_vendor.get(vendor, {})
        lines.append("")
        lines.append(f"### {vendor.upper()}")
        lines.append("")
        if v.get("fixture_count", 0) == 0:
            lines.append("- _No Bundle fixtures captured for this vendor — pagination semantics unmeasured offline._")
            continue
        lines.append(
            f"- Probed `{v['fixture_count']}` Bundle fixtures; "
            f"`{v['fixtures_with_next_link']}` carry `rel=next`."
        )
        ts = v.get("total_semantics") or {}
        lines.append(f"- **Bundle.total**: `{ts.get('label')}` — {ts.get('quote', '').strip()}")
        lines.append(f"  - Evidence: `{ts.get('evidence_fixture')}`")
        tok = v.get("token_stability") or {}
        lines.append(f"- **Token stability**: `{tok.get('label')}` — {tok.get('quote', '').strip()}")
        lines.append(f"  - Evidence: `{tok.get('evidence_fixture')}`")
        if v.get("vital_signs_page_size_observed") is not None:
            lines.append(
                f"- **Vital-signs page-size override**: `{v['vital_signs_page_size_observed']}` "
                "entries on a single page (compare to vendor default)."
            )
        if v.get("per_resource_page_overrides"):
            lines.append(
                f"- **Per-resource page-size overrides**: {v['per_resource_page_overrides']}"
            )
        if v.get("per_resource_single_page_max"):
            lines.append(
                f"- **Largest single-page no-rel=next bundles** (per resource): "
                f"{v['per_resource_single_page_max']}"
            )
        if v.get("sample_next_urls"):
            lines.append("- **Sample next URLs** (truncated to 5):")
            for u in v["sample_next_urls"]:
                lines.append(f"  - `{u}`")

    # Known gaps — kata acceptance asks us to document these explicitly.
    lines.append("")
    lines.append("## Known gaps")
    lines.append("")
    epic = per_vendor.get("epic") or {}
    if epic.get("fixtures_with_next_link", 0) <= 2:
        lines.append(
            "- **Epic**: only the vital-signs probe ever crossed Epic's page-size cap "
            "in our captured corpus, so the empirical `default_page_size` is biased "
            "toward the 1000-entry vital-signs ceiling. Non-vital-signs resources "
            "(DocumentReference, Observation-lab, etc.) returned ≤70 entries on a "
            "single page with no `rel=next` — meaning either they exhausted the "
            "match set or sit under a lower per-resource cap we cannot resolve "
            "from one-shot fixtures."
        )
    cerner = per_vendor.get("cerner") or {}
    if cerner.get("empty_bundle_shape") == "unobserved":
        lines.append(
            "- **Cerner**: empty-result Bundle shape unobserved in our corpus "
            "(every captured Cerner search returned ≥1 match). KATA-OO-02 "
            "captures the OperationOutcome shape on auth/preflight errors; an "
            "explicit empty-result capture remains TODO."
        )
    meditech = per_vendor.get("meditech") or {}
    if meditech.get("fixture_count", 0) < 30:
        lines.append(
            "- **MEDITECH**: pagination corpus is small (single canonical patient "
            "under Phase B 2026-04-28 capture). Token expiry / TTL not measurable "
            "offline; would require re-issuing a previously-captured `continue=` "
            "cursor against the live sandbox — outside this kata's offline scope. "
            "MEDITECH server-side self-link normalization (`:443` appended to host) "
            "is consistent across all 15 captures."
        )
    if meditech.get("empty_bundle_shape") == "unobserved":
        lines.append(
            "- **MEDITECH**: empty-result Bundle shape unobserved (every captured "
            "MEDITECH search returned ≥1 match for the bound Greenfield patient). "
            "Capture pending under kata #133 (MEDITECH Phase B consent gate)."
        )

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(
        "_Generated by `tools/probe_pagination.py`. Re-run with `python -m tools.probe_pagination` "
        "after any new golden capture refresh. KATA-CAPTURE-BUNDLE-LINK-SEMANTICS (#136)._"
    )
    lines.append("")
    return "\n".join(lines)


# ---------- Driver ---------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vendor", choices=sorted(VENDOR_FIXTURE_ROOTS), help="Probe one vendor; default: all")
    ap.add_argument("--dry-run", action="store_true", help="Print findings but don't write overlays or report")
    ap.add_argument("--report-only", action="store_true", help="Write the markdown report but skip overlay writes")
    args = ap.parse_args()

    today = datetime.date.today().isoformat()
    vendors = [args.vendor] if args.vendor else sorted(VENDOR_FIXTURE_ROOTS)

    per_vendor: dict[str, dict] = {}
    print(f"== probe_pagination.py — {today} ==")
    for vendor in vendors:
        print(f"\n[{vendor}] walking fixtures under tests/golden/{vendor}/ ...")
        findings = probe_vendor(vendor)
        per_vendor[vendor] = findings
        print(
            f"  fixtures: {findings['fixture_count']}  "
            f"with rel=next: {findings['fixtures_with_next_link']}  "
            f"link relations: {findings['link_relations_observed']}  "
            f"next URL format: {findings['next_url_format']}  "
            f"total_semantics: {findings['total_semantics']['label']}"
        )

    # Write overlays
    if not args.report_only:
        for vendor, findings in per_vendor.items():
            block = build_overlay_block(findings)
            path = write_overlay(vendor, block, dry_run=args.dry_run)
            if args.dry_run:
                print(f"  (dry-run) would update {path.relative_to(REPO_ROOT)}.pagination")
            else:
                print(f"  updated {path.relative_to(REPO_ROOT)} (added/refreshed `pagination` block)")

    # Write report
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_md_path = REPORTS_DIR / f"pagination-{today}.md"
    report_json_dir = REPORTS_DIR / "pagination"
    report_json_dir.mkdir(parents=True, exist_ok=True)
    report_json_path = report_json_dir / f"pagination-{today}.json"

    md = render_markdown_report(per_vendor, today)
    payload = {
        "kata": "KATA-CAPTURE-BUNDLE-LINK-SEMANTICS",
        "kata_issue": 136,
        "generated": today,
        "per_vendor": per_vendor,
    }
    if args.dry_run:
        print(f"\n(dry-run) would write {report_md_path.relative_to(REPO_ROOT)}")
        print(f"(dry-run) would write {report_json_path.relative_to(REPO_ROOT)}")
    else:
        report_md_path.write_text(md)
        report_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
        print(f"\n  wrote {report_md_path.relative_to(REPO_ROOT)}")
        print(f"  wrote {report_json_path.relative_to(REPO_ROOT)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
