"""Axis 4: primitive format checks.

For each FHIR primitive type that has a regex/format invariant per the spec, verify
that values at the path satisfy the invariant. This is narrow on purpose: complex
types (HumanName, CodeableConcept, etc.) are validated via cardinality + value-set;
this axis catches the "EHR returned a malformed date" / "EHR returned uppercase
where lowercase code expected" class of bugs.

Patterns sourced from FHIR R4 datatypes (https://hl7.org/fhir/R4/datatypes.html).
"""
from __future__ import annotations

import re

from .presence import collect_at_path

# Compiled regexes for FHIR primitives. None of these are perfect (FHIR primitives
# have edge cases); these are the practical patterns most vendors get wrong.
PRIMITIVE_PATTERNS: dict[str, re.Pattern] = {
    "date": re.compile(r"^[0-9]{4}(-(0[1-9]|1[0-2])(-(0[1-9]|[12][0-9]|3[01]))?)?$"),
    "dateTime": re.compile(
        r"^[0-9]{4}"
        r"(-(0[1-9]|1[0-2])"
        r"(-(0[1-9]|[12][0-9]|3[01])"
        r"(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?"
        r"(Z|[+\-]([01][0-9]|2[0-3]):[0-5][0-9]))?)?)?$"
    ),
    "instant": re.compile(
        r"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
        r"T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?"
        r"(Z|[+\-]([01][0-9]|2[0-3]):[0-5][0-9])$"
    ),
    "time": re.compile(r"^([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?$"),
    "code": re.compile(r"^[^\s]+(\s[^\s]+)*$"),  # non-empty, no leading/trailing whitespace
    "id": re.compile(r"^[A-Za-z0-9\-\.]{1,64}$"),
    "oid": re.compile(r"^urn:oid:[0-2](\.(0|[1-9][0-9]*))+$"),
    "uri": re.compile(r"^\S+$"),  # no whitespace; we don't enforce scheme
    "url": re.compile(r"^\S+$"),
    "uuid": re.compile(r"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
    "decimal": re.compile(r"^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+\-]?[0-9]+)?$"),
    "integer": re.compile(r"^-?(0|[1-9][0-9]*)$"),
    "positiveInt": re.compile(r"^[1-9][0-9]*$"),
    "unsignedInt": re.compile(r"^(0|[1-9][0-9]*)$"),
}


def _primitive_value(v: object) -> str | None:
    """Extract a primitive string value if `v` is or wraps one."""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    return None


def format_finding(*, profile_id: str, must_support: dict, resource: dict, ehr: str, today: str) -> dict | None:
    types = must_support.get("type") or []
    # Only run format axis when at least one type is a primitive we have a regex for.
    primitive_types = [t for t in types if t in PRIMITIVE_PATTERNS]
    if not primitive_types:
        return None
    # If the declared type list mixes primitive-with-regex types and primitive-without-regex
    # (e.g. `Patient.deceased[x]` is `boolean | dateTime`), skip — we'd flag the boolean
    # as malformed dateTime. We can only enforce format when EVERY declared type has a regex.
    untyped = [t for t in types if t not in PRIMITIVE_PATTERNS]
    if untyped:
        return None

    values = collect_at_path(resource, must_support["path"])
    if not values:
        return None

    # Collect all primitive string values from the path
    primitives: list[tuple[str, str]] = []  # (value, matching_type_or_empty)
    for v in values:
        s = _primitive_value(v)
        if s is None:
            continue
        # Try every declared type's pattern; record the first success or empty if none match
        matched_type = ""
        for t in primitive_types:
            if PRIMITIVE_PATTERNS[t].match(s):
                matched_type = t
                break
        primitives.append((s, matched_type))

    if not primitives:
        return None

    bad = [s for s, mt in primitives if not mt]
    if not bad:
        return None

    return {
        "profile_id": profile_id,
        "path": must_support["path"],
        "deviation_category": "format-violation",
        "expected_per_us_core": f"primitive {' | '.join(primitive_types)}",
        "observed_in_ehr": f"{len(bad)} of {len(primitives)} value(s) failed primitive regex; sample: {bad[:3]}",
        "deviation": (
            f"Element {must_support['path']} is declared as primitive type {primitive_types}, "
            f"but {len(bad)} value(s) don't match the FHIR R4 primitive pattern."
        ),
        "verification": {
            "source_url": "(see paired golden fixture)",
            "source_quote": f"failing samples: {bad[:5]}",
            "verified_via": f"{ehr}_public_sandbox",
            "verified_date": today,
        },
    }
