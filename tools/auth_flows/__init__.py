"""Per-EHR auth-flow dispatcher.

Each EHR's `EHR_CONFIG` entry carries a `flow` key (`client_credentials` or
`auth_code`); `get_access_token(ehr)` dispatches to the right module and returns
`(access_token, fhir_base)`. Tools that need a token never hard-code a flow.

Token caching lives under `<repo>/.tokens/{ehr}.json` (gitignored). For
auth_code, this is the only way to reuse the human consent across phases — the
token cache is opened, refreshed if expired, and rewritten.

Public API:
    EHR_CONFIG                 — dict[ehr, config]
    get_access_token(ehr)      — (access_token, fhir_base)
    build_client_assertion(...) — re-exported for backward compatibility
    request_token(...)          — re-exported
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from tools._env import load_env

from .client_credentials import build_client_assertion, get_token_client_credentials, request_token

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TOKEN_CACHE_DIR = REPO_ROOT / ".tokens"

# Per-EHR auth + endpoint config. Each entry's `flow` determines which auth_flows
# submodule handles get_access_token. Add a new EHR by inserting a key here and
# (if needed) implementing a new flow module.
EHR_CONFIG: dict[str, dict] = {
    "epic": {
        "flow": "client_credentials",
        "client_id_var": "EPIC_NONPROD_CLIENT_ID",
        "private_key_var": "EPIC_NONPROD_PRIVATE_KEY_PATH",
        "kid_var": "EPIC_NONPROD_JWKS_KID",
        "token_url_var": "EPIC_NONPROD_TOKEN_URL",
        "fhir_base_var": "EPIC_NONPROD_FHIR_BASE",
        "default_scope": "system/Patient.read system/Observation.read system/Encounter.read",
        "canonical_patient_id": "eq081-VQEgP8drUUqCWzHfw3",
    },
    "cerner": {
        "flow": "client_credentials",
        # Cerner registered this app as a confidential client (shared secret),
        # not a JWKS-system account — so client_secret_basic, not JWT-bearer.
        # The deployed JWKS at /.well-known/jwks-cerner-{nonprod,prod}.json is
        # unused by this client but stays in place for a future system-account.
        "auth_method": "client_secret_basic",
        "client_id_var": "CERNER_NONPROD_CLIENT_ID",
        "client_secret_var": "CERNER_NONPROD_CLIENT_SECRET",
        "token_url_var": "CERNER_NONPROD_TOKEN_URL",
        "fhir_base_var": "CERNER_NONPROD_FHIR_BASE",
        # Standard Cerner sandbox patient (Nancy Smart). Lab-rich, unlike Epic's Derrick Lin.
        "canonical_patient_id": "12724066",
        # CRITICAL: Cerner uses SMART v2 scope syntax (`.r` / `.rs`), NOT v1 `.read`.
        # `system/Patient.read` returns `empty-scopes`; `system/Patient.r` succeeds.
        # Phase A finding: Epic accepts v1, Cerner requires v2.
        "default_scope": (
            "system/Patient.rs system/Observation.rs system/Encounter.rs "
            "system/AllergyIntolerance.rs system/Condition.rs "
            "system/MedicationRequest.rs system/Procedure.rs system/Immunization.rs"
        ),
        # Phase B sweep runs against the OPEN tier (fhir-open.cerner.com) — no auth
        # needed and the open tier is empirically richer than what this app's
        # System Account class can read on the secure tier (per phase_b_findings).
        # The secure-tier creds above stay configured for token-lifecycle / transport
        # measurements that require a real auth round-trip.
        "phase_b_base_var": "CERNER_NONPROD_FHIR_OPEN_BASE",
        "phase_b_no_auth": True,
    },
    "meditech": {
        # Greenfield Workspace — MEDITECH's vendor-managed sandbox. Centralized
        # (single tenant for all developers), unlike Phase F's 542 customer
        # endpoints. Path is /v2/uscore/STU6 — different from what production
        # customers expose (R4 v1.0.0 frozen 2021 + STU7 v2.0.0 from 2024).
        # Greenfield is "aspirationally most common" per Getting Started v3.0 p.3,
        # not deployed reality — worth distinguishing from Phase F findings.
        "flow": "auth_code",
        # Greenfield REQUIRES client_secret_post per Getting Started v3.0 p.7
        # ("Client Authentication: Send client credentials in body"). Basic
        # auth is rejected.
        "token_endpoint_auth_method": "client_secret_post",
        "client_id_var": "MEDITECH_NONPROD_CLIENT_ID",
        "client_secret_var": "MEDITECH_NONPROD_CLIENT_SECRET",
        "token_url_var": "MEDITECH_NONPROD_TOKEN_URL",
        "authorize_url_var": "MEDITECH_NONPROD_AUTHORIZE_URL",
        "fhir_base_var": "MEDITECH_NONPROD_FHIR_BASE",
        # SMART standalone-launch v2 scopes. patient/*.read alone yields a token
        # with empty `smartOnFhir: {}` — usable for direct reads with hard-coded
        # patient IDs but no launch context. Adding launch/patient triggers the
        # "select patient" UI on consent, populates `patient` in the token
        # response and `smartOnFhir.launchPatient` in the JWT — i.e., the real
        # patient-app flow. openid+fhirUser yields an id_token; offline_access
        # yields an explicit refresh_token. Greenfield accepts all five.
        "default_scope": "openid fhirUser launch/patient offline_access patient/*.read",
        # Greenfield's bundled Postman collection does NOT send `aud` in the
        # authorize request (empty authRequestParams). Sending it is harmless
        # for the bare patient/*.read flow but the standalone-launch flow that
        # actually populates SMART context was validated without it — match
        # Postman exactly to stay in the documented happy path.
        "omit_aud": True,
        # The shipped Postman collection has no explicit redirect_uri — Postman
        # uses its hosted callback by default and the issued credentials are tied
        # to that URI. oauth.pstmn.io is not a localhost we can listen on, so the
        # paste-callback flow handles it: copy the post-consent URL from the
        # browser address bar, paste it back to the CLI.
        "redirect_uri": "https://oauth.pstmn.io/v1/callback",
        "callback_port": None,
        "use_pkce": True,  # Postman collection sets challengeAlgorithm=S256
        # Sandbox patient from the Postman collection (visible to every developer
        # via direct reads): Sarai Mccall, born 1959-08-14. With launch/patient
        # in scope the operator picks a patient from a UI list at consent — the
        # `patient` claim in the token response wins over this canonical ID.
        "canonical_patient_id": "0218f2d0-968b-5888-976f-68a554670f6e",
        # MEDITECH ties consent to a Google account registered with Greenfield
        # (Getting Started v3.0 p.8) — sign in with the Google account that
        # received the 1Password credentials link.
    },
}


def _load_env() -> None:
    load_env(strict=False)


def resolve_url(cfg: dict, key: str) -> str:
    """Return cfg[f'{key}_override'] if set, else os.environ[cfg[f'{key}_var']].

    `key` is one of {token_url, authorize_url, fhir_base}. Override-first lets
    the_map repo pin a sandbox version (e.g. AP25) without depending on a sibling
    project's .env staying current.
    """
    override = cfg.get(f"{key}_override")
    if override:
        return override
    var = cfg.get(f"{key}_var")
    if var and var in os.environ:
        return os.environ[var]
    raise KeyError(f"neither {key}_override nor env[{var}] set for this EHR")


def get_access_token(ehr: str, *, force_refresh: bool = False) -> tuple[str, str]:
    """Return (access_token, fhir_base) for the given EHR.

    Dispatches to the right flow based on EHR_CONFIG[ehr]['flow']. For auth_code
    flows, this opens a browser for the initial consent and reuses the cached
    refresh_token thereafter.
    """
    if ehr not in EHR_CONFIG:
        sys.exit(f"unknown EHR {ehr!r}; known: {sorted(EHR_CONFIG)}")
    cfg = EHR_CONFIG[ehr]
    _load_env()
    flow = cfg.get("flow", "client_credentials")
    if flow == "client_credentials":
        return get_token_client_credentials(cfg)
    if flow == "auth_code":
        # Late import — auth_code pulls in stdlib http.server which is heavy
        from .auth_code import get_token_auth_code
        return get_token_auth_code(ehr, cfg, force_refresh=force_refresh)
    sys.exit(f"unsupported flow {flow!r} for EHR {ehr!r}")


def get_access_token_with_meta(ehr: str, *, force_refresh: bool = False) -> dict:
    """Like get_access_token but returns the full token-cache dict (includes
    `patient`, `scope`, `expires_at`). Useful for auth_code flows where the
    `patient` claim drives the per-EHR sweep."""
    if ehr not in EHR_CONFIG:
        sys.exit(f"unknown EHR {ehr!r}")
    cfg = EHR_CONFIG[ehr]
    _load_env()
    flow = cfg.get("flow", "client_credentials")
    if flow == "auth_code":
        from .auth_code import get_token_auth_code_meta
        return get_token_auth_code_meta(ehr, cfg, force_refresh=force_refresh)
    # For client_credentials there's no patient; synthesize a meta dict
    access_token, fhir_base = get_token_client_credentials(cfg)
    return {
        "access_token": access_token,
        "fhir_base": fhir_base,
        "patient": cfg.get("canonical_patient_id"),
        "scope": cfg.get("default_scope"),
        "flow": "client_credentials",
    }


__all__ = [
    "EHR_CONFIG",
    "TOKEN_CACHE_DIR",
    "build_client_assertion",
    "get_access_token",
    "get_access_token_with_meta",
    "request_token",
]
