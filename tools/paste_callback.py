"""Complete an auth_code consent by pasting the redirected URL back to the CLI.

Companion to `tools.begin_consent {ehr}` for non-localhost redirect_uri flows
where the consent server can't auto-capture. After you complete consent in your
browser and land on the registered redirect_uri, copy the URL (or just the
`?code=...&state=...` portion) and run:

    python -m tools.paste_callback <ehr> --state=<state> --code=<code>

Or, more conveniently, paste the full URL via --url:

    python -m tools.paste_callback <ehr> --url='https://.../callback?code=...&state=...'

The state value MUST match what the prior `tools.begin_consent <ehr>` printed
and persisted to `.tokens/{ehr}.pending-state`, which this helper reads to
verify automatically.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.parse
from pathlib import Path

from tools._env import load_env
from tools.auth_flows import EHR_CONFIG, TOKEN_CACHE_DIR
from tools.auth_flows.auth_code import _exchange_code_for_token, _normalize_token_response, _save_cache

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--url", help="full redirected URL or just `?code=...&state=...`")
    ap.add_argument("--code", help="auth code (alternative to --url)")
    ap.add_argument("--state", help="state value to verify against pending-state")
    args = ap.parse_args()

    load_env(strict=False)

    cfg = EHR_CONFIG[args.ehr]
    if cfg.get("flow") != "auth_code":
        sys.exit(f"{args.ehr} flow is {cfg.get('flow')}; paste_callback only applies to auth_code")

    code = args.code
    state = args.state
    if args.url:
        qs = args.url.split("?", 1)[1] if "?" in args.url else args.url
        params = dict(urllib.parse.parse_qsl(qs.lstrip("&").lstrip("?")))
        if "error" in params:
            sys.exit(f"redirect URL carries error: {params.get('error')}: {params.get('error_description', '')}")
        code = code or params.get("code")
        state = state or params.get("state")

    if not code:
        sys.exit("no code provided (use --url=... or --code=...)")

    pending_path = TOKEN_CACHE_DIR / f"{args.ehr}.pending-state"
    if pending_path.exists():
        expected = pending_path.read_text().strip()
        if state and state != expected:
            sys.exit(f"state mismatch: pending={expected!r} pasted={state!r}")
        if not state:
            print(f"  using pending state from {pending_path.relative_to(REPO_ROOT)}")
        # Don't unlink yet — only after successful exchange

    from tools.auth_flows import resolve_url
    client_id = os.environ[cfg["client_id_var"]]
    client_secret = os.environ[cfg["client_secret_var"]]
    token_url = resolve_url(cfg, "token_url")
    redirect_uri = cfg["redirect_uri"]
    fhir_base = resolve_url(cfg, "fhir_base")

    code_verifier = None
    pending_verifier_path = TOKEN_CACHE_DIR / f"{args.ehr}.pending-verifier"
    if pending_verifier_path.exists():
        code_verifier = pending_verifier_path.read_text().strip()

    print(f"  exchanging code for token at {token_url}")
    tok = _exchange_code_for_token(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        code=code,
        redirect_uri=redirect_uri,
        code_verifier=code_verifier,
        auth_method=cfg.get("token_endpoint_auth_method", "client_secret_basic"),
    )
    normalized = _normalize_token_response(args.ehr, cfg, tok, fhir_base=fhir_base)
    _save_cache(args.ehr, normalized)
    if pending_path.exists():
        pending_path.unlink()
    if pending_verifier_path.exists():
        pending_verifier_path.unlink()

    print(f"  cached token to .tokens/{args.ehr}.json")
    print(f"  patient:   {normalized.get('patient')}")
    print(f"  scope:     {normalized.get('scope')}")
    print(f"  expires_at: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(normalized['expires_at']))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
