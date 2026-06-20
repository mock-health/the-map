"""Public-client (PKCE, no secret) support in tools.auth_flows.auth_code.

Pins the T9 contract: a public client carries no secret, but every secret-based
method still REQUIRES its env var and fails fast + locally when it's missing —
instead of silently sending no secret and getting an opaque 401 back.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.auth_flows.auth_code import _resolve_client_secret, _token_request_kwargs


def test_public_client_needs_no_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANY_SECRET", raising=False)
    cfg = {"token_endpoint_auth_method": "none"}
    assert _resolve_client_secret(cfg) is None


def test_secret_post_returns_env_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MED_SECRET", "s3cr3t")
    cfg = {"token_endpoint_auth_method": "client_secret_post", "client_secret_var": "MED_SECRET"}
    assert _resolve_client_secret(cfg) == "s3cr3t"


def test_secret_method_missing_env_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MED_SECRET", raising=False)
    cfg = {"token_endpoint_auth_method": "client_secret_post", "client_secret_var": "MED_SECRET"}
    with pytest.raises(SystemExit) as e:
        _resolve_client_secret(cfg)
    assert "MED_SECRET" in str(e.value)  # the error names the missing var


def test_default_auth_method_is_secret_based(monkeypatch: pytest.MonkeyPatch) -> None:
    """A config without token_endpoint_auth_method defaults to client_secret_basic,
    so it must still require its secret (regression guard for the .get() change)."""
    monkeypatch.delenv("MED_SECRET", raising=False)
    cfg = {"client_secret_var": "MED_SECRET"}  # no token_endpoint_auth_method
    with pytest.raises(SystemExit):
        _resolve_client_secret(cfg)


def test_secret_method_without_secret_var_is_config_error() -> None:
    cfg = {"token_endpoint_auth_method": "client_secret_basic"}  # no client_secret_var
    with pytest.raises(SystemExit) as e:
        _resolve_client_secret(cfg)
    assert "client_secret_var" in str(e.value)


def test_token_kwargs_public_client_sends_no_secret() -> None:
    kw = _token_request_kwargs(body={"grant_type": "authorization_code"},
                               client_id="pub-app", client_secret=None, auth_method="none")
    assert kw["data"]["client_id"] == "pub-app"
    assert "client_secret" not in kw["data"]
    assert "Authorization" not in kw["headers"]


def test_token_kwargs_secret_post_includes_secret() -> None:
    kw = _token_request_kwargs(body={"grant_type": "authorization_code"},
                               client_id="app", client_secret="s3cr3t", auth_method="client_secret_post")
    assert kw["data"]["client_secret"] == "s3cr3t"
