"""pytest fixtures shared across the test suite.

Test data lives in two places:
  - tests/golden/   — captured CapStmts and per-endpoint harvests (fixture data).
                      DO NOT recurse into this directory; it is data, not tests.
  - tests/fixtures/ — synthetic, deliberately-broken inputs for adversarial tests
                      (overlays missing iron-rule fields, XSS payloads, etc.).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def schema_overlay() -> dict:
    return json.loads((REPO_ROOT / "schema" / "overlay.schema.json").read_text())


@pytest.fixture
def schema_production_fleet() -> dict:
    return json.loads((REPO_ROOT / "schema" / "production_fleet.schema.json").read_text())


@pytest.fixture
def fixtures_dir() -> Path:
    return REPO_ROOT / "tests" / "fixtures"


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip env vars that leak between tests. Each test that needs creds sets them explicitly."""
    for key in list(os.environ):
        if key.startswith(("EPIC_", "CERNER_", "MEDITECH_")) or key in {"THE_MAP_ENV"}:
            monkeypatch.delenv(key, raising=False)
