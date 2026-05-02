"""Capture MEDITECH Greenfield goldens for the derisk read-observe loop.

Writes one fixture per (resource-type, search-style) into
`tests/golden/meditech/phase-b-2026-04-28/{prefix}-{id}.json` using the same
envelope shape `derisk.py` already consumes for Epic:

    {
      "_request": {"method": "GET", "url": "...", "status": <int>},
      "_captured_at": "<iso8601>",
      "body": <resource-or-Bundle>
    }

The prefixes mirror Epic's naming so the same `GOLDEN_TO_RTYPE` mapping
(extended for MEDITECH-only types) picks them up.

Source patient: launch_patient from the cached SMART token (Paula Bond).
Authentication: reads the cached SMART-launch token at .tokens/meditech.json,
no fresh consent.
"""
from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from tools.auth_flows import get_access_token_with_meta

THE_MAP = Path(__file__).resolve().parent.parent
OUT_DIR = THE_MAP / "tests" / "golden" / "meditech" / "phase-b-2026-04-28"

# Per-resource-type query plan. Tuples of (prefix, resource_type, params).
# Prefixes match Epic naming conventions where applicable so derisk's
# GOLDEN_TO_RTYPE mapping carries over without per-vendor branching.
QUERIES: list[tuple[str, str, dict]] = [
    # Direct patient read (no params, ID injected per-call)
    ("patient", "Patient", {}),
    # Search-style by ?patient=
    ("allergyintolerance", "AllergyIntolerance", {"patient": "{pid}"}),
    ("condition", "Condition", {"patient": "{pid}"}),
    ("careplan", "CarePlan", {"patient": "{pid}"}),
    ("careteam", "CareTeam", {"_id": "{pid}"}),
    ("diagnosticreport", "DiagnosticReport", {"patient": "{pid}"}),
    ("documentreference", "DocumentReference", {"patient.identifier": "{pid}"}),
    ("encounter", "Encounter", {"patient": "{pid}"}),
    ("goal", "Goal", {"patient": "{pid}"}),
    ("immunization", "Immunization", {"patient": "{pid}"}),
    ("medicationrequest", "MedicationRequest", {"patient": "{pid}"}),
    ("observation-lab", "Observation", {"patient": "{pid}", "category": "laboratory"}),
    ("observation-vital-signs", "Observation", {"patient": "{pid}", "category": "vital-signs"}),
    ("observation-social-history", "Observation", {"patient": "{pid}", "category": "social-history"}),
    ("procedure", "Procedure", {"patient": "{pid}"}),
    ("servicerequest", "ServiceRequest", {"patient": "{pid}"}),
]


def _get_with_retry(client: httpx.Client, url: str, params: dict, *, tries: int = 3) -> httpx.Response:
    """Greenfield's gateway hiccups intermittently with 502; retry."""
    last: httpx.Response | None = None
    for i in range(tries):
        last = client.get(url, params=params)
        if last.status_code != 502:
            return last
        time.sleep(1.0)
    assert last is not None
    return last


def _save(prefix: str, ident: str, request_url: str, status: int, body) -> Path:
    path = OUT_DIR / f"{prefix}-{ident}.json"
    envelope = {
        "_request": {"method": "GET", "url": request_url, "status": status},
        "_captured_at": datetime.now(UTC).isoformat(),
        "body": body,
    }
    path.write_text(json.dumps(envelope, indent=2) + "\n")
    return path


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meta = get_access_token_with_meta("meditech")
    tok = meta["access_token"]
    base = meta["fhir_base"]
    pid = meta.get("patient")
    if not pid:
        print("no launch_patient in cached token; run begin_consent / paste_callback first")
        return 1

    print(f"capturing goldens for patient {pid} from {base}")
    print(f"output: {OUT_DIR}")

    client = httpx.Client(
        headers={"Authorization": f"Bearer {tok}", "Accept": "application/fhir+json"},
        timeout=60,
    )

    written: list[Path] = []

    # Direct Patient read
    url = f"{base}/Patient/{pid}"
    r = _get_with_retry(client, url, {})
    if r.status_code == 200:
        body = r.json()
        written.append(_save("patient", pid, url, r.status_code, body))
        print(f"  ok: patient/{pid}")
    else:
        print(f"  FAIL Patient/{pid}: HTTP {r.status_code} {r.text[:120]}")

    # Search-style queries — one Bundle per resource type
    for prefix, rtype, params_template in QUERIES:
        if prefix == "patient":
            continue  # already done
        params = {k: (v.replace("{pid}", pid) if isinstance(v, str) else v) for k, v in params_template.items()}
        url = f"{base}/{rtype}"
        r = _get_with_retry(client, url, params)
        if r.status_code != 200:
            print(f"  FAIL {prefix} ({rtype}): HTTP {r.status_code}")
            continue
        bundle = r.json()
        # Save the bundle (search response) under {prefix}-bundle.json
        full_url = f"{url}?{httpx.QueryParams(params)}"
        written.append(_save(prefix, "bundle", full_url, r.status_code, bundle))
        # Plus save the first entry as a standalone fixture so the read-only diff
        # has a single-resource target to compare against (matches Epic's golden
        # naming convention `{prefix}-{id}.json`).
        entries = bundle.get("entry") or []
        if entries:
            first = entries[0].get("resource") or {}
            rid = first.get("id") or "first"
            written.append(_save(prefix, rid, f"{url}/{rid}", 200, first))
        print(f"  ok: {prefix:30} total={bundle.get('total','?')} entries={len(entries)}")

    print(f"\nwrote {len(written)} fixtures to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
