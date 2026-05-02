"""Print an auth_code consent URL + persist the OAuth `state` for later pasteback.

Companion to `tools.paste_callback`. Use when the registered redirect_uri is not
localhost (e.g. a hosted https://...callback URL) and the CLI cannot capture the
code automatically.

Usage:
    python -m tools.begin_consent <ehr>
    # ... open the printed URL, complete consent, copy the redirect URL ...
    python -m tools.paste_callback <ehr> --url='https://.../callback?code=...&state=...'
"""
from __future__ import annotations

import argparse
import os
import secrets
import sys
import urllib.parse
from pathlib import Path

from tools._env import load_env
from tools.auth_flows import EHR_CONFIG, TOKEN_CACHE_DIR
from tools.auth_flows.auth_code import _aud_for, _pkce_pair

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", choices=sorted(EHR_CONFIG))
    ap.add_argument("--no-aud", action="store_true",
                    help="Omit `aud` from the authorize request. Some servers "
                         "(MEDITECH Greenfield) appear to treat unrecognized "
                         "aud values as a permission failure (access_denied).")
    args = ap.parse_args()

    load_env(strict=False)

    cfg = EHR_CONFIG[args.ehr]
    if cfg.get("flow") != "auth_code":
        sys.exit(f"{args.ehr} flow is {cfg.get('flow')}; consent URL only applies to auth_code")

    state = secrets.token_urlsafe(16)
    TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (TOKEN_CACHE_DIR / f"{args.ehr}.pending-state").write_text(state + "\n")

    from tools.auth_flows import resolve_url
    client_id = os.environ[cfg["client_id_var"]]
    authorize_url = resolve_url(cfg, "authorize_url")
    redirect_uri = cfg["redirect_uri"]
    fhir_base = resolve_url(cfg, "fhir_base")
    aud = _aud_for(cfg, fhir_base)

    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": cfg["default_scope"],
        "state": state,
    }
    if not args.no_aud and not cfg.get("omit_aud"):
        auth_params["aud"] = aud
    if cfg.get("use_pkce"):
        verifier, challenge = _pkce_pair()
        auth_params["code_challenge"] = challenge
        auth_params["code_challenge_method"] = "S256"
        (TOKEN_CACHE_DIR / f"{args.ehr}.pending-verifier").write_text(verifier + "\n")

    consent_url = f"{authorize_url}?{urllib.parse.urlencode(auth_params)}"

    print(f"== {args.ehr} OAuth consent ==")
    print(f"redirect_uri: {redirect_uri}")
    print(f"aud:          {aud}")
    print(f"state:        {state} (saved to .tokens/{args.ehr}.pending-state)")
    if cfg.get("use_pkce"):
        print(f"pkce:         S256 (verifier saved to .tokens/{args.ehr}.pending-verifier)")
    print()
    print("1) Open this URL in your browser:")
    print()
    print(f"   {consent_url}")
    print()
    if cfg.get("sandbox_test_logins"):
        print("   Sign-in screen credentials (published test accounts):")
        for login in cfg["sandbox_test_logins"]:
            print(f"     {login['role']:<32}  username={login['username']}  password={login['password']}")
        print()
    print("2) After consenting, copy the redirected URL (or its query string) and run:")
    print(f"   python -m tools.paste_callback {args.ehr} --url='<paste here>'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
