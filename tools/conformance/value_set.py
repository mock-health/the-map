"""Axis 3: value-set conformance.

Strategy:
  - When `binding.strength == "required"` we enforce closed membership: every observed
    code at the path must come from the bound ValueSet's enumerated codes (or a
    declared `compose.include[].system` for system-only includes).
  - When strength is `extensible` / `preferred` / `example`, we don't enforce —
    we record the observed codes as a `value-set-narrowed` finding so editorial can
    decide whether the EHR is using a vendor-specific subset.

ValueSets we can fully expand locally:
  - Those with `compose.include[].concept` (concept lists)
  - Those with `compose.include[].system` only (we treat them as "any code from system X")

ValueSets we *cannot* expand locally without external data:
  - Those that filter against an external code system (e.g. SNOMED subsets, LOINC
    subsets defined by `compose.include[].filter` constraints).

For the un-expandable case we mark the finding `value-set-unverified-locally` and
defer to HAPI's $validate-code (Phase 4). That keeps the analyzer offline-deterministic
and surfaces the boundary explicitly rather than silently passing.
"""
from __future__ import annotations

import json
from pathlib import Path

from .presence import collect_at_path


class ValueSetIndex:
    """Loads every ValueSet-*.json from the IG package and builds a lookup-by-canonical-url.

    Lazy expansion: each VS is parsed but not expanded until first lookup, then cached.
    `member?(value_set_url, code, system)` is the single API.
    """

    def __init__(self, ig_package_dir: Path):
        self._ig_dir = ig_package_dir
        self._vs_by_url: dict[str, dict] = {}
        self._cs_by_url: dict[str, dict] = {}  # CodeSystems for fallback
        self._expansion_cache: dict[str, set[tuple[str, str]] | None] = {}
        # `None` in the cache means "we couldn't expand this VS locally; defer to HAPI"

        for p in sorted(ig_package_dir.glob("ValueSet-*.json")):
            try:
                vs = json.loads(p.read_text())
            except json.JSONDecodeError:
                continue
            url = vs.get("url")
            if url:
                self._vs_by_url[url] = vs
                # Some IGs reference VS by `url|version`; we strip versions at lookup time.
        for p in sorted(ig_package_dir.glob("CodeSystem-*.json")):
            try:
                cs = json.loads(p.read_text())
            except json.JSONDecodeError:
                continue
            url = cs.get("url")
            if url:
                self._cs_by_url[url] = cs

    def _expand_locally(self, vs_url: str) -> set[tuple[str, str]] | None:
        """Return a set of (system, code) tuples for the VS, or None if we can't expand."""
        if vs_url in self._expansion_cache:
            return self._expansion_cache[vs_url]

        vs = self._vs_by_url.get(vs_url)
        if vs is None:
            self._expansion_cache[vs_url] = None
            return None

        codes: set[tuple[str, str]] = set()
        compose = vs.get("compose") or {}
        deferrable = False
        for inc in compose.get("include", []) or []:
            system = inc.get("system", "")
            if inc.get("concept"):
                for c in inc["concept"]:
                    codes.add((system, c.get("code", "")))
            elif inc.get("filter"):
                # Can't expand filters locally — we'd need the CodeSystem itself
                deferrable = True
            elif inc.get("valueSet"):
                # Recursive include; we'll attempt to follow
                for nested_url in inc["valueSet"]:
                    sub = self._expand_locally(nested_url.split("|")[0])
                    if sub is None:
                        deferrable = True
                    else:
                        codes |= sub
            else:
                # `include[].system` only — accept any code from that system
                # We encode that as a wildcard tuple `(system, "*")`
                codes.add((system, "*"))
        # Some VS use `expansion` directly (post-expansion form)
        for c in (vs.get("expansion") or {}).get("contains", []) or []:
            codes.add((c.get("system", ""), c.get("code", "")))

        if not codes and deferrable:
            self._expansion_cache[vs_url] = None
            return None
        self._expansion_cache[vs_url] = codes
        return codes

    def member(self, vs_url: str, code: str, system: str) -> str:
        """Return one of: 'in', 'out', 'unverified-locally', 'unknown-vs'.

        - `in` / `out`: definitive membership decision based on local expansion.
        - `unverified-locally`: VS uses filter or external CS we can't expand.
        - `unknown-vs`: the requested URL isn't in the IG package.
        """
        # Strip version pin
        clean = vs_url.split("|", 1)[0]
        if clean not in self._vs_by_url:
            return "unknown-vs"
        expanded = self._expand_locally(clean)
        if expanded is None:
            return "unverified-locally"
        if (system, code) in expanded:
            return "in"
        if (system, "*") in expanded:
            return "in"
        # `code`-typed primitives (e.g. Observation.status) carry no system in the response;
        # the binding implies the system. If our query lacks a system, accept any system.
        if not system:
            for s, c in expanded:
                if c == code:
                    return "in"
                if c == "*":
                    return "in"
        return "out"


def _extract_codings(value: object) -> list[tuple[str, str]]:
    """Walk a value (which may be a CodeableConcept, code string, Coding, etc.) and
    return every (system, code) pair it carries."""
    out: list[tuple[str, str]] = []
    if value is None:
        return out
    if isinstance(value, str):
        # Plain `code` primitive — system is unknown at this level
        out.append(("", value))
        return out
    if isinstance(value, list):
        for v in value:
            out.extend(_extract_codings(v))
        return out
    if isinstance(value, dict):
        if "coding" in value:
            for c in value.get("coding") or []:
                out.append((c.get("system", ""), c.get("code", "")))
        if "code" in value and "system" in value:
            # A Coding directly
            out.append((value.get("system", ""), value.get("code", "")))
        # CodeableConcept with text-only also lands here; no codings to extract
    return out


def value_set_finding(*, profile_id: str, must_support: dict, resource: dict, ehr: str, today: str, vs_index: ValueSetIndex) -> dict | None:
    binding = must_support.get("binding") or {}
    strength = binding.get("strength")
    vs_url = binding.get("valueSet")
    if not vs_url or strength not in ("required", "extensible", "preferred"):
        return None

    values = collect_at_path(resource, must_support["path"])
    if not values:
        return None

    observed: set[tuple[str, str]] = set()
    for v in values:
        for sc in _extract_codings(v):
            observed.add(sc)

    if not observed:
        return None  # No codes in the response at this path; nothing to compare

    deferred = []
    out_of_set = []
    in_set = []
    for system, code in observed:
        result = vs_index.member(vs_url, code, system)
        if result == "in":
            in_set.append((system, code))
        elif result == "out":
            out_of_set.append((system, code))
        else:
            deferred.append((system, code, result))

    # Required + at least one out-of-set code → conformance violation
    if strength == "required" and out_of_set:
        return {
            "profile_id": profile_id,
            "path": must_support["path"],
            "deviation_category": "value-set-mismatch",
            "expected_per_us_core": f"binding required → {binding.get('valueSet_id', '?')}",
            "observed_in_ehr": f"{len(out_of_set)} of {len(observed)} codings out-of-set; sample: {list(out_of_set)[:3]}",
            "deviation": (
                f"Required binding to {binding.get('valueSet_id', vs_url)} but {len(out_of_set)}/{len(observed)} "
                f"observed code(s) are not members. Vendor likely emits codes from a wider/different VS."
            ),
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"Out-of-set codes: {list(out_of_set)[:5]}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }

    # Required + locally-undefined VS → defer note
    if strength == "required" and deferred and not out_of_set and not in_set:
        return {
            "profile_id": profile_id,
            "path": must_support["path"],
            "deviation_category": "value-set-unverified-locally",
            "expected_per_us_core": f"binding required → {binding.get('valueSet_id', '?')}",
            "observed_in_ehr": f"{len(observed)} coding(s) observed; VS uses external code system filters not expandable locally",
            "deviation": (
                "Bound VS references an external code system (likely LOINC/SNOMED subset) "
                "and cannot be locally expanded. Membership is deferred to HAPI's $validate-code (Phase 4)."
            ),
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"sample observed codes: {list(observed)[:5]}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }

    # Extensible/preferred — only record when vendor actually emits a code outside the bound VS.
    # If every observed code is in-set OR the VS can't be locally expanded, no row needed.
    if strength in ("extensible", "preferred") and out_of_set:
        return {
            "profile_id": profile_id,
            "path": must_support["path"],
            "deviation_category": "value-set-narrowed",
            "expected_per_us_core": f"binding {strength} → {binding.get('valueSet_id', '?')}",
            "observed_in_ehr": f"{len(out_of_set)} of {len(observed)} codings outside the bound VS; sample: {list(out_of_set)[:3]}",
            "deviation": (
                f"With {strength} binding, vendor MAY emit codes outside the bound VS, but doing so "
                f"signals vendor-specific value-set narrowing/extension. Out-of-set codes recorded "
                "for editorial review."
            ),
            "verification": {
                "source_url": "(see paired golden fixture)",
                "source_quote": f"out-of-set: {list(out_of_set)[:5]}; in-set: {list(in_set)[:3]}",
                "verified_via": f"{ehr}_public_sandbox",
                "verified_date": today,
            },
        }

    return None  # required + all in-set is the success case; no row needed
