"""Phase 6: token-lifecycle and transport-edge probes.

Captures behavior the CapabilityStatement can't tell you: how Epic responds to
expired tokens, parallel token requests, content-type negotiation, and (cautiously)
rate limiting. Output is `transport_findings` in the overlay.

Usage:
    python -m tools.probe_transport epic
    python -m tools.probe_transport epic --skip-rate-limit       # don't fire 100 RPS
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from tools._env import load_env
from tools.auth_flows.client_credentials import build_client_assertion, request_token
from tools.oauth_handshake import EHR_CONFIG, get_access_token, get_access_token_with_meta

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) transport-probe"


def get_token_response(ehr: str) -> requests.Response:
    """Return the raw HTTP response from the token endpoint. Only meaningful for
    client_credentials flows — auth_code flows can't issue tokens unattended."""
    cfg = EHR_CONFIG[ehr]
    if cfg.get("flow", "client_credentials") != "client_credentials":
        raise NotImplementedError("get_token_response only supported for client_credentials flows")
    pem = open(os.environ[cfg["private_key_var"]], "rb").read()
    assertion = build_client_assertion(
        client_id=os.environ[cfg["client_id_var"]],
        token_url=os.environ[cfg["token_url_var"]],
        kid=os.environ[cfg["kid_var"]],
        private_key_pem=pem,
    )
    return request_token(token_url=os.environ[cfg["token_url_var"]], client_assertion=assertion, scope=cfg["default_scope"])


def probe_token_expiry(ehr: str) -> dict:
    """Test what the EHR returns when a request is sent with an obviously-invalid bearer token."""
    cfg = EHR_CONFIG[ehr]
    base = os.environ[cfg["fhir_base_var"]]
    pid = cfg.get("canonical_patient_id")
    if not pid:
        # auth_code flows discover patient from token cache
        meta = get_access_token_with_meta(ehr)
        pid = meta.get("patient")

    # Use a deliberately-malformed token
    fake_token = "this-is-not-a-real-jwt-eyJhbGciOiJSUzM4NCJ9.fake.signature"
    r = requests.get(
        f"{base.rstrip('/')}/Patient/{pid}",
        headers={
            "Authorization": f"Bearer {fake_token}",
            "Accept": "application/fhir+json",
            "User-Agent": USER_AGENT,
        },
        timeout=30,
    )
    return {
        "trigger": f"GET {base}/Patient/{pid} with malformed bearer token",
        "http_status": r.status_code,
        "www_authenticate_header": r.headers.get("WWW-Authenticate"),
        "content_type": r.headers.get("Content-Type"),
        "first_300": r.text[:300],
    }


def probe_concurrent_tokens(ehr: str, n: int = 5) -> dict:
    """Request N tokens in parallel and verify they're distinct."""
    with ThreadPoolExecutor(max_workers=n) as ex:
        futures = [ex.submit(get_token_response, ehr) for _ in range(n)]
        responses = [f.result() for f in futures]
    tokens: list[str] = []
    statuses: list[int] = []
    for r in responses:
        statuses.append(r.status_code)
        if r.ok:
            try:
                tokens.append(r.json().get("access_token", ""))
            except ValueError:
                tokens.append("")
    return {
        "n_requested": n,
        "n_succeeded": sum(1 for s in statuses if 200 <= s < 300),
        "all_distinct": len(set(t for t in tokens if t)) == len([t for t in tokens if t]),
        "statuses": statuses,
        "token_lengths": [len(t) for t in tokens],
    }


def probe_content_types(ehr: str, token: str) -> list[dict]:
    cfg = EHR_CONFIG[ehr]
    base = os.environ[cfg["fhir_base_var"]]
    pid = cfg.get("canonical_patient_id")
    if not pid:
        meta = get_access_token_with_meta(ehr)
        pid = meta.get("patient")
    out = []
    for accept in [
        "application/fhir+json",
        "application/json",
        "application/xml",
        "application/fhir+xml",
        "*/*",
        "this/is-not-a-real-mime",
        "",
    ]:
        headers = {"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT}
        if accept:
            headers["Accept"] = accept
        r = requests.get(f"{base.rstrip('/')}/Patient/{pid}", headers=headers, timeout=30)
        out.append({
            "accept_header": accept or "(omitted)",
            "http_status": r.status_code,
            "response_content_type": r.headers.get("Content-Type"),
            "first_120": r.text[:120].replace("\n", " "),
        })
    return out


def probe_rate_limit(ehr: str, token: str, *, n: int = 30, concurrency: int = 10) -> dict:
    """Cautiously probe rate-limit behavior. Defaults are deliberately moderate
    (30 reqs at 10 concurrent, ~3 RPS) to surface limits without becoming abusive."""
    cfg = EHR_CONFIG[ehr]
    base = os.environ[cfg["fhir_base_var"]]
    pid = cfg.get("canonical_patient_id")
    if not pid:
        meta = get_access_token_with_meta(ehr)
        pid = meta.get("patient")

    statuses: list[tuple[int, str]] = []

    def fire():
        r = requests.get(
            f"{base.rstrip('/')}/Patient/{pid}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/fhir+json", "User-Agent": USER_AGENT},
            timeout=30,
        )
        return r.status_code, r.headers.get("Retry-After", "")

    started = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        results = list(ex.map(lambda _: fire(), range(n)))
    elapsed = time.time() - started
    statuses = results
    statuses_only = [s for s, _ in statuses]
    return {
        "n_fired": n,
        "concurrency": concurrency,
        "wall_seconds": round(elapsed, 2),
        "approx_rps": round(n / max(elapsed, 0.001), 1),
        "status_distribution": dict(zip(*[iter(sorted(set(statuses_only)))] * 1, strict=False)) if False else {s: statuses_only.count(s) for s in sorted(set(statuses_only))},
        "any_429": 429 in statuses_only,
        "first_retry_after": next((ra for s, ra in statuses if s == 429 and ra), None),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--skip-rate-limit", action="store_true")
    ap.add_argument("--rate-limit-n", type=int, default=30)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_env(strict=True)

    today = datetime.date.today().isoformat()
    cfg = EHR_CONFIG[args.ehr]
    flow = cfg.get("flow", "client_credentials")
    print(f"== Phase 6 transport probes against {args.ehr} (flow={flow}) ==")

    print()
    print("step 1/4 — invalid-bearer probe")
    invalid = probe_token_expiry(args.ehr)
    print(f"  HTTP {invalid['http_status']}  WWW-Authenticate: {invalid['www_authenticate_header']!r}")

    print()
    print("step 2/4 — concurrent token issuance")
    if flow == "client_credentials":
        concurrent = probe_concurrent_tokens(args.ehr, n=5)
        print(f"  succeeded {concurrent['n_succeeded']}/{concurrent['n_requested']}  all_distinct={concurrent['all_distinct']}  statuses={concurrent['statuses']}")
    else:
        concurrent = {
            "skipped": True,
            "reason": f"flow={flow} cannot issue tokens unattended; concurrent-token probe is meaningful only for client_credentials.",
        }
        print(f"  SKIPPED ({concurrent['reason']})")

    # Need a token for content-type and rate-limit probes
    print()
    print("step 3/4 — content-type negotiation")
    token, _ = get_access_token(args.ehr)
    content_types = probe_content_types(args.ehr, token)
    for ct in content_types:
        print(f"  Accept={ct['accept_header']!r:<40} HTTP {ct['http_status']}  → {ct['response_content_type']!r}")

    rl: dict = {"skipped": True}
    if not args.skip_rate_limit:
        print()
        print(f"step 4/4 — rate-limit probe ({args.rate_limit_n} reqs)")
        rl = probe_rate_limit(args.ehr, token, n=args.rate_limit_n, concurrency=10)
        print(f"  fired {rl['n_fired']} reqs in {rl['wall_seconds']}s (~{rl['approx_rps']} RPS); status_distribution={rl['status_distribution']}; any_429={rl['any_429']}")

    findings = {
        "captured_date": today,
        "invalid_bearer": invalid,
        "concurrent_token_issuance": concurrent,
        "content_type_negotiation": content_types,
        "rate_limit_probe": rl,
        "verification": {
            "source_url": "(see overlay sub-fields for individual triggers)",
            "source_quote": f"Probed by tools.probe_transport on {today}",
            "verified_via": f"{args.ehr}_public_sandbox",
            "verified_date": today,
        },
    }

    if args.dry_run:
        print()
        print(json.dumps(findings, indent=2)[:1500])
        return 0

    overlay_path = EHRS_DIR / args.ehr / "overlay.json"
    overlay = json.loads(overlay_path.read_text())
    overlay["transport_findings"] = findings
    overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    print(f"\n  updated {overlay_path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
