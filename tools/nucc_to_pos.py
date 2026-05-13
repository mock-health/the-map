"""NUCC Healthcare Provider Taxonomy → CMS POS facility category mapping.

NPPES carries the NUCC taxonomy code (e.g., `282N00000X` = General Acute
Care Hospital). POS catalogs facilities by `PRVDR_CTGRY_CD` (e.g., `01` =
Hospital). These code-systems describe related concepts but use different
identifiers, so we maintain a curated bridge.

Used by `tools.llm_disambiguate` to pre-filter candidate POS facilities
before sending them to the LLM. If an endpoint's NPPES org is an Acute
Care Hospital (282N) and the candidate list has 4 hospitals + 8 FQHCs,
the FQHCs are correctly suppressed — we KNOW the endpoint isn't an FQHC.
Narrowing the candidate set reduces LLM confusion and restores its
ability to confidently pick from the relevant subset.

The mapping is intentionally CONSERVATIVE — only codes where the
NUCC→POS mapping is unambiguous are listed. NUCC has 800+ codes; many
have no POS equivalent (private physician practices, dental clinics,
many specialty care providers don't appear in POS at all). When a code
is unmapped we return None and the caller should not filter.

Prefix matching: NUCC codes are hierarchical (Section / Classification /
Specialization). We try longest-prefix match first so a specific
specialization wins over a broad classification.

Refs:
  - NUCC taxonomy code set: https://taxonomy.nucc.org/
  - POS Data Dictionary PRVDR_CTGRY_CD: see data dictionary in
    `~/back/data/pos/` next to the source ZIPs.
"""
from __future__ import annotations

# Curated NUCC prefix → POS category set. When a prefix maps, ONLY POS
# rows with one of these category codes are valid candidates for the
# endpoint's underlying org.
#
# Each key is a NUCC prefix; lookup tries the longest prefix first so
# narrow specializations override broad classifications.
#
# Mapping notes per row:
NUCC_PREFIX_TO_POS_CATEGORIES: dict[str, set[str]] = {
    # ─── Hospitals (282 + 283 + 273) ─────────────────────────────────
    # General Acute Care Hospital — the modal hospital category.
    "282N":    {"01"},
    # Long Term Care Hospital, Long Term Care Hospital General.
    "282E":    {"01"},
    # Hospital - Pediatric, Adolescent
    "282NC2000X": {"01"},
    # Religious Non-medical Health Care Institution — POS code 23.
    "282J":    {"23"},
    # Hospital Units (273) — psych units, rehab units, etc. inside hospitals
    "273":     {"01"},
    # Specialty hospitals (283): Psychiatric, Children's, Rehabilitation
    "283Q":    {"01"},  # Psychiatric Hospital
    "283X":    {"01"},  # Children's Hospital
    "2865":    {"01"},  # Specialty Hospital
    # ─── Skilled Nursing Facility (314) ──────────────────────────────
    "314":     {"02"},
    # ─── Hospice (251G) ──────────────────────────────────────────────
    "251G":    {"05"},
    # ─── Federally Qualified Health Center ───────────────────────────
    # FQHC (organization) — appears as 261QF0400X
    "261QF":   {"11", "15"},
    # ─── Rural Health Clinic (261QR1300X) ────────────────────────────
    "261QR":   {"08"},
    # ─── Ambulatory Surgical Center (261QA1903X) ─────────────────────
    "261QA":   {"12", "21"},
    # ─── End-Stage Renal Disease Treatment Facility (261QE0700X) ─────
    "261QE":   {"06"},
    # ─── Comprehensive Outpatient Rehab Facility (261QR0405X etc.) ───
    # Note: 261QR overlaps with RHC above — the more-specific
    # specialization wins via longest-prefix match below.
    # Outpatient Physical Therapy = 261QR0205X (5-char prefix matches RHC).
    # We're conservative: only map the explicit FQHC/RHC/ASC/ESRD codes
    # and leave PT/OT/Speech as unknown.
}

# Maximum prefix length we'll try. Computed once for efficiency. Lookups
# walk down from this length to 3 (the minimum meaningful prefix in NUCC).
_MAX_PREFIX_LEN = max((len(k) for k in NUCC_PREFIX_TO_POS_CATEGORIES), default=10)


def pos_categories_for_taxonomy(taxonomy: str | None) -> set[str] | None:
    """Return the set of POS category codes valid for a NUCC taxonomy.

    Returns None when:
      - taxonomy is empty/None, OR
      - the taxonomy doesn't match any mapped prefix (caller: don't filter).

    Returns a set of one or more PRVDR_CTGRY_CD strings when:
      - a mapped prefix matches. Caller filters candidates to rows whose
        category_code ∈ this set.

    Longest-prefix wins so a specific specialization overrides a broader
    classification (e.g., 261QF0400X picks "261QF" → FQHC, not whatever
    broader 261Q* might map to in the future).
    """
    if not taxonomy:
        return None
    t = taxonomy.strip().upper()
    # Try longest prefix first.
    for n in range(_MAX_PREFIX_LEN, 2, -1):
        if len(t) < n:
            continue
        prefix = t[:n]
        if prefix in NUCC_PREFIX_TO_POS_CATEGORIES:
            return NUCC_PREFIX_TO_POS_CATEGORIES[prefix]
    return None


def filter_candidates_by_taxonomy(
    candidates: list[dict],
    taxonomy: str | None,
) -> tuple[list[dict], str]:
    """Filter `candidates` to those whose category_code matches the taxonomy's
    POS mapping. Returns (filtered_candidates, status).

    status is one of:
      - "kept_all"        : no taxonomy filter applied (taxonomy unknown
                            or unmapped, or candidates lack category info)
      - "filtered"        : filter narrowed the candidate set
      - "filtered_empty"  : filter removed every candidate — strong signal
                            that the endpoint's org isn't any of the
                            candidates. Caller should NOT send to LLM.
    """
    allowed = pos_categories_for_taxonomy(taxonomy)
    if allowed is None:
        return candidates, "kept_all"
    # If no candidate carries category_code, we can't safely filter.
    if not any(c.get("category_code") for c in candidates):
        return candidates, "kept_all"
    filtered = [c for c in candidates if c.get("category_code") in allowed]
    if not filtered:
        return [], "filtered_empty"
    if len(filtered) == len(candidates):
        return candidates, "kept_all"
    return filtered, "filtered"
