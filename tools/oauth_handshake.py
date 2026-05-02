"""Walk an EHR's OAuth handshake and print a token + optional probe.

This module is a backward-compatible shim over `tools.auth_flows`. The flow
specifics live in `tools/auth_flows/{client_credentials,auth_code}.py`; this
file is the CLI surface and the import target for legacy callers that do
`from tools.oauth_handshake import EHR_CONFIG, build_client_assertion, request_token`.

Usage:
    python -m tools.oauth_handshake epic
    python -m tools.oauth_handshake epic --probe
    python -m tools.oauth_handshake meditech              # walks browser consent on first run
    python -m tools.oauth_handshake meditech --force-refresh  # re-walk consent (ignore cache)
"""
from __future__ import annotations

import argparse
import sys

import requests

from tools.auth_flows import (
    EHR_CONFIG,
    build_client_assertion,  # re-export for legacy callers
    get_access_token,
    get_access_token_with_meta,
    request_token,  # re-export for legacy callers
)

__all__ = [
    "EHR_CONFIG",
    "build_client_assertion",
    "get_access_token",
    "get_access_token_with_meta",
    "request_token",
]


def probe(fhir_base: str, access_token: str, patient_id: str | None) -> None:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/fhir+json"}
    print()
    print(f"probe: GET {fhir_base}/metadata")
    r = requests.get(f"{fhir_base}/metadata", headers=headers, timeout=30)
    print(f"  HTTP {r.status_code}  {len(r.content)} bytes")
    if patient_id:
        print(f"probe: GET {fhir_base}/Patient/{patient_id}")
        r = requests.get(f"{fhir_base}/Patient/{patient_id}", headers=headers, timeout=30)
        print(f"  HTTP {r.status_code}  {len(r.content)} bytes")
        if r.ok:
            body = r.json()
            name = body.get("name", [{}])[0]
            family = name.get("family")
            given = " ".join(name.get("given", []))
            print(f"  patient: {given} {family} (gender={body.get('gender')}, birthDate={body.get('birthDate')})")
        else:
            print(f"  body: {r.text[:400]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--force-refresh", action="store_true",
                    help="auth_code only: ignore cached token, re-walk consent")
    args = ap.parse_args()

    cfg = EHR_CONFIG[args.ehr]
    print(f"ehr:   {args.ehr}")
    print(f"flow:  {cfg.get('flow', 'client_credentials')}")
    print()

    meta = get_access_token_with_meta(args.ehr, force_refresh=args.force_refresh)
    access_token = meta["access_token"]
    fhir_base = meta["fhir_base"]
    print(f"  access_token: {access_token[:24]}…{access_token[-8:]} ({len(access_token)} chars)")
    if meta.get("scope"):
        print(f"  scope:        {meta['scope']}")
    if meta.get("patient"):
        print(f"  patient:      {meta['patient']}")
    if meta.get("fhir_user"):
        print(f"  fhir_user:    {meta['fhir_user']}")

    if args.probe and access_token:
        probe(fhir_base, access_token, meta.get("patient") or cfg.get("canonical_patient_id"))

    return 0


if __name__ == "__main__":
    sys.exit(main())
