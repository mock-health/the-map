"""SMART authorization_code flow with localhost callback + token caching.

Used for EHRs that don't expose system/* scopes. Steps:

  1) Build the authorize URL with client_id + redirect_uri + scope + state
  2) Open the URL in the user's default browser
  3) Run a single-request HTTP server on `localhost:{callback_port}` that captures
     `?code=...&state=...` and returns a small "you can close this tab" page
  4) Exchange the code for an access_token + refresh_token via POST to the token
     endpoint with HTTP Basic auth (client_secret_basic)
  5) Cache the token to .tokens/{ehr}.json

On subsequent runs, the cache is opened. If access_token is unexpired, return it.
If expired but refresh_token is present, try refresh. If refresh fails, prompt
consent again.

The token cache schema:
    {
      "access_token": "...",
      "refresh_token": "...",   # optional
      "expires_at": 1714150523, # unix epoch
      "scope": "...",
      "patient": "<id>",        # FHIR patient id from the token response, when present
      "fhir_base": "...",
      "flow": "auth_code",
      "obtained_at": 1714146923
    }
"""
from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import secrets
import socketserver
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path

import requests


def _pkce_pair() -> tuple[str, str]:
    """Generate (verifier, challenge) per RFC 7636 S256 method."""
    verifier = secrets.token_urlsafe(64)[:128]  # 43-128 chars
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _aud_for(cfg: dict, fhir_base: str) -> str:
    """Compute the OAuth `aud` parameter. Some EHRs require the API server root,
    not the FHIR base — controlled via cfg.aud_strip_suffix."""
    suffix = cfg.get("aud_strip_suffix")
    if suffix and fhir_base.endswith(suffix):
        return fhir_base[: -len(suffix)]
    return fhir_base

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TOKEN_CACHE_DIR = REPO_ROOT / ".tokens"


def _resolve_url(cfg: dict, key: str) -> str:
    """Local copy of tools.auth_flows.resolve_url to avoid circular import."""
    override = cfg.get(f"{key}_override")
    if override:
        return override
    return os.environ[cfg[f"{key}_var"]]


def _cache_path(ehr: str) -> Path:
    return TOKEN_CACHE_DIR / f"{ehr}.json"


def _load_cache(ehr: str) -> dict | None:
    p = _cache_path(ehr)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return None


def _save_cache(ehr: str, data: dict) -> None:
    TOKEN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(ehr).write_text(json.dumps(data, indent=2) + "\n")
    try:
        os.chmod(_cache_path(ehr), 0o600)
    except OSError:
        pass


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode()
    return "Basic " + base64.b64encode(raw).decode()


def _token_request_kwargs(*, body: dict, client_id: str, client_secret: str, auth_method: str) -> dict:
    """Build POST kwargs for the token endpoint, picking the auth shape per RFC 6749.

    client_secret_basic: HTTP Basic header, secret NOT in body.
    client_secret_post:  client_id + client_secret in form body, no Basic header. (MEDITECH.)
    """
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    body = dict(body)
    body["client_id"] = client_id
    if auth_method == "client_secret_post":
        body["client_secret"] = client_secret
    elif auth_method == "client_secret_basic":
        headers["Authorization"] = _basic_auth_header(client_id, client_secret)
    else:
        sys.exit(f"unknown token_endpoint_auth_method {auth_method!r}")
    return {"data": body, "headers": headers, "timeout": 30}


def _exchange_code_for_token(*, token_url: str, client_id: str, client_secret: str, code: str, redirect_uri: str, code_verifier: str | None = None, auth_method: str = "client_secret_basic") -> dict:
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    if code_verifier:
        body["code_verifier"] = code_verifier
    r = requests.post(token_url, **_token_request_kwargs(
        body=body, client_id=client_id, client_secret=client_secret, auth_method=auth_method,
    ))
    if not r.ok:
        sys.exit(f"token exchange failed HTTP {r.status_code}: {r.text[:400]}")
    return r.json()


def _refresh_access_token(*, token_url: str, client_id: str, client_secret: str, refresh_token: str, auth_method: str = "client_secret_basic") -> dict | None:
    r = requests.post(token_url, **_token_request_kwargs(
        body={"grant_type": "refresh_token", "refresh_token": refresh_token},
        client_id=client_id, client_secret=client_secret, auth_method=auth_method,
    ))
    if not r.ok:
        return None
    return r.json()


def _run_callback_server(port: int, expected_state: str) -> tuple[str | None, str | None]:
    """Block until the OAuth callback hits localhost:{port}, return (code, error)."""
    captured: dict[str, str] = {}

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # silence default logger
            pass

        def do_GET(self):
            qs = urllib.parse.urlparse(self.path).query
            params = dict(urllib.parse.parse_qsl(qs))
            captured.update(params)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            if "error" in params:
                msg = f"<h1>Authorization error</h1><p>{params.get('error')}: {params.get('error_description', '')}</p>"
            elif "code" in params and params.get("state") == expected_state:
                msg = "<h1>OK — close this tab.</h1><p>The CLI captured the code; you can return to your terminal.</p>"
            else:
                msg = "<h1>Unexpected callback</h1><pre>" + json.dumps(params) + "</pre>"
            self.wfile.write(msg.encode())
            # Schedule shutdown after sending response
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    httpd = socketserver.TCPServer(("127.0.0.1", port), CallbackHandler)
    httpd.timeout = 600  # 10 minutes for human consent
    httpd.serve_forever()
    httpd.server_close()
    if "error" in captured:
        return None, f"{captured.get('error')}: {captured.get('error_description', '')}"
    if captured.get("state") != expected_state:
        return None, f"state mismatch — expected {expected_state}, got {captured.get('state')}"
    return captured.get("code"), None


def _is_localhost_redirect(uri: str) -> bool:
    """Local listener mode is only safe for http://localhost or 127.0.0.1 redirects."""
    parsed = urllib.parse.urlparse(uri)
    return parsed.hostname in {"localhost", "127.0.0.1", "::1"}


def _prompt_paste_callback(expected_state: str) -> tuple[str | None, str | None]:
    """Hosted-redirect-uri mode: user pastes the full callback URL (or just the
    query string) after their browser lands on the registered redirect target."""
    print()
    print("Paste here the URL your browser was redirected to (or just the `?code=...&state=...` portion):")
    print("  (Tip: copy the address bar after consent completes. End with a blank line.)")
    print("  > ", end="", flush=True)
    line = sys.stdin.readline().strip()
    if not line:
        return None, "no input"
    # Tolerate full URLs, just the query, or `code=...&state=...`
    if "?" in line:
        qs = line.split("?", 1)[1]
    else:
        qs = line
    qs = qs.lstrip("&").lstrip("?")
    params = dict(urllib.parse.parse_qsl(qs))
    if "error" in params:
        return None, f"{params.get('error')}: {params.get('error_description', '')}"
    if params.get("state") != expected_state:
        return None, f"state mismatch — expected {expected_state}, got {params.get('state')!r}"
    code = params.get("code")
    if not code:
        return None, f"no code in pasted input; parsed params={list(params)}"
    return code, None


def _walk_consent(*, ehr: str, cfg: dict) -> dict:
    """Run the browser consent flow end-to-end. Returns the token-response dict."""
    client_id = os.environ[cfg["client_id_var"]]
    client_secret = os.environ[cfg["client_secret_var"]]
    token_url = _resolve_url(cfg, "token_url")
    authorize_url = _resolve_url(cfg, "authorize_url")
    redirect_uri = cfg["redirect_uri"]
    fhir_base = _resolve_url(cfg, "fhir_base")
    state = secrets.token_urlsafe(16)
    aud = _aud_for(cfg, fhir_base)

    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": cfg["default_scope"],
        "state": state,
    }
    # Some servers (MEDITECH Greenfield) don't accept `aud` on authorize and
    # the bundled Postman collection omits it — opt-out per cfg.
    if not cfg.get("omit_aud"):
        auth_params["aud"] = aud
    code_verifier = None
    if cfg.get("use_pkce"):
        code_verifier, challenge = _pkce_pair()
        auth_params["code_challenge"] = challenge
        auth_params["code_challenge_method"] = "S256"

    consent_url = f"{authorize_url}?{urllib.parse.urlencode(auth_params)}"

    print(f"\n{ehr} consent required. Open this URL in your browser:\n  {consent_url}\n")
    if cfg.get("sandbox_test_logins"):
        print("  Published sandbox sign-in creds:")
        for login in cfg["sandbox_test_logins"]:
            print(f"    {login['role']}: {login['username']} / {login['password']}")
        print()

    if _is_localhost_redirect(redirect_uri):
        port = cfg["callback_port"]
        print(f"Waiting for callback on http://127.0.0.1:{port}/callback (10 min timeout)…")
        try:
            webbrowser.open(consent_url, new=2)
        except Exception:
            pass
        code, err = _run_callback_server(port, state)
    else:
        # Non-localhost redirect_uri (e.g. https://mock.health/auth/<ehr>/callback).
        # We can't run a listener there — the user pastes the redirected URL back.
        try:
            webbrowser.open(consent_url, new=2)
        except Exception:
            pass
        code, err = _prompt_paste_callback(state)

    if err:
        sys.exit(f"consent failed: {err}")
    if not code:
        sys.exit("consent returned no code")

    print("  consent ok, exchanging code for token…")
    return _exchange_code_for_token(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        code=code,
        redirect_uri=redirect_uri,
        code_verifier=code_verifier,
        auth_method=cfg.get("token_endpoint_auth_method", "client_secret_basic"),
    )


def _normalize_token_response(ehr: str, cfg: dict, tok: dict, *, fhir_base: str) -> dict:
    now = int(time.time())
    expires_in = int(tok.get("expires_in", 3600))
    return {
        "access_token": tok["access_token"],
        "refresh_token": tok.get("refresh_token"),
        "expires_at": now + expires_in - 60,  # 60s safety margin
        "scope": tok.get("scope") or cfg["default_scope"],
        "patient": tok.get("patient"),
        "fhir_user": tok.get("fhirUser") or tok.get("fhir_user"),
        "fhir_base": fhir_base,
        "flow": "auth_code",
        "obtained_at": now,
        "id_token_present": bool(tok.get("id_token")),
    }


def get_token_auth_code_meta(ehr: str, cfg: dict, *, force_refresh: bool = False) -> dict:
    """Return the full cached token-meta dict, refreshing or re-consenting as needed."""
    fhir_base = _resolve_url(cfg, "fhir_base")
    cached = _load_cache(ehr) if not force_refresh else None

    # Hit: cache present and not expired
    if cached and cached.get("access_token") and cached.get("expires_at", 0) > int(time.time()):
        return cached

    # Cache present but expired and refreshable: try refresh
    if cached and cached.get("refresh_token") and not force_refresh:
        client_id = os.environ[cfg["client_id_var"]]
        client_secret = os.environ[cfg["client_secret_var"]]
        token_url = _resolve_url(cfg, "token_url")
        refreshed = _refresh_access_token(
            token_url=token_url,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=cached["refresh_token"],
            auth_method=cfg.get("token_endpoint_auth_method", "client_secret_basic"),
        )
        if refreshed and refreshed.get("access_token"):
            # Some servers issue a new refresh_token; preserve the old if not
            if not refreshed.get("refresh_token"):
                refreshed["refresh_token"] = cached["refresh_token"]
            # Some servers don't echo the patient id on refresh; preserve it
            if not refreshed.get("patient") and cached.get("patient"):
                refreshed["patient"] = cached["patient"]
            normalized = _normalize_token_response(ehr, cfg, refreshed, fhir_base=fhir_base)
            _save_cache(ehr, normalized)
            return normalized

    # No cache or refresh failed — walk consent
    tok = _walk_consent(ehr=ehr, cfg=cfg)
    normalized = _normalize_token_response(ehr, cfg, tok, fhir_base=fhir_base)
    _save_cache(ehr, normalized)
    return normalized


def get_token_auth_code(ehr: str, cfg: dict, *, force_refresh: bool = False) -> tuple[str, str]:
    meta = get_token_auth_code_meta(ehr, cfg, force_refresh=force_refresh)
    return meta["access_token"], meta["fhir_base"]
