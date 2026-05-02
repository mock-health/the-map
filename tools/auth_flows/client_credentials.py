"""SMART Backend Services client_credentials flow.

Two auth methods:
  - private_key_jwt (default): RS384 JWT-bearer assertion. Used by Epic.
  - client_secret_basic: HTTP Basic with shared secret. Used by Cerner's
    sandbox confidential clients (despite the SMART spec preferring JWT).

Each EHR's cfg picks via `auth_method` ("private_key_jwt" | "client_secret_basic").
Defaults to private_key_jwt for back-compat.
"""
from __future__ import annotations

import base64
import os
import sys
import time
import uuid

import jwt
import requests


def build_client_assertion(*, client_id: str, token_url: str, kid: str, private_key_pem: bytes) -> str:
    now = int(time.time())
    payload = {
        "iss": client_id,
        "sub": client_id,
        "aud": token_url,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + 240,  # ≤300s per spec; 240s leaves headroom for clock skew
    }
    return jwt.encode(
        payload,
        private_key_pem,
        algorithm="RS384",
        headers={"kid": kid, "typ": "JWT"},
    )


def request_token(*, token_url: str, client_assertion: str, scope: str) -> requests.Response:
    return requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": client_assertion,
            "scope": scope,
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )


def request_token_basic(*, token_url: str, client_id: str, client_secret: str, scope: str) -> requests.Response:
    """client_secret_basic — HTTP Basic auth at the token endpoint."""
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    return requests.post(
        token_url,
        data={"grant_type": "client_credentials", "scope": scope},
        headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
        timeout=30,
    )


def get_token_client_credentials(cfg: dict) -> tuple[str, str]:
    """Return (access_token, fhir_base) for a client_credentials EHR config."""
    auth_method = cfg.get("auth_method", "private_key_jwt")
    token_url = os.environ[cfg["token_url_var"]]
    fhir_base = os.environ[cfg["fhir_base_var"]]

    if auth_method == "private_key_jwt":
        pem = open(os.environ[cfg["private_key_var"]], "rb").read()
        assertion = build_client_assertion(
            client_id=os.environ[cfg["client_id_var"]],
            token_url=token_url,
            kid=os.environ[cfg["kid_var"]],
            private_key_pem=pem,
        )
        r = request_token(token_url=token_url, client_assertion=assertion, scope=cfg["default_scope"])
    elif auth_method == "client_secret_basic":
        r = request_token_basic(
            token_url=token_url,
            client_id=os.environ[cfg["client_id_var"]],
            client_secret=os.environ[cfg["client_secret_var"]],
            scope=cfg["default_scope"],
        )
    else:
        sys.exit(f"unknown auth_method {auth_method!r}")

    if not r.ok:
        sys.exit(f"token endpoint returned {r.status_code}: {r.text[:300]}")
    return r.json()["access_token"], fhir_base
