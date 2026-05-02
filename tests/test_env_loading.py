"""P0 regression: tools/_env.py loader behavior.

Eight pipeline tools depend on this helper. If we break it, every contributor's
pipeline breaks silently — these tests are the canary.

Resolution order (first hit wins):
  1. THE_MAP_ENV env var
  2. <repo>/.env
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Ensure tools/ is importable when running from a fresh checkout.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools import _env


@pytest.fixture
def fake_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Re-point _env's REPO_ROOT at a temp dir so we can stage candidate .envs there."""
    monkeypatch.setattr(_env, "REPO_ROOT", tmp_path)
    return tmp_path


def test_no_env_anywhere_strict_exits(fake_repo: Path, capsys: pytest.CaptureFixture) -> None:
    """No project .env, no THE_MAP_ENV. strict=True must exit code 1 and name
    every path tried."""
    with pytest.raises(SystemExit) as exc:
        _env.load_env(strict=True)
    assert exc.value.code != 0
    # The user needs to see the missing path so they know where to put .env
    assert ".env" in str(exc.value)
    _ = capsys.readouterr()  # drain captured output (assertion is on exc.value)


def test_no_env_anywhere_lenient_returns_none(fake_repo: Path) -> None:
    """No env files. strict=False returns None silently. Phase A must work this way."""
    assert _env.load_env(strict=False) is None


def test_project_env_loads(
    fake_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Project .env in the repo root loads when THE_MAP_ENV is unset."""
    monkeypatch.delenv("PROJECT_VAR", raising=False)
    project_env = fake_repo / ".env"
    project_env.write_text("PROJECT_VAR=yes\n")

    loaded = _env.load_env(strict=False)
    assert loaded == project_env
    assert os.environ.get("PROJECT_VAR") == "yes"


def test_explicit_the_map_env_override_wins(
    fake_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE_MAP_ENV is the explicit override — must win over project .env."""
    monkeypatch.delenv("EXPLICIT_VAR", raising=False)
    monkeypatch.delenv("PROJECT_VAR", raising=False)
    project_env = fake_repo / ".env"
    project_env.write_text("PROJECT_VAR=yes\n")

    explicit = tmp_path / "explicit.env"
    explicit.write_text("EXPLICIT_VAR=yes\n")
    monkeypatch.setenv("THE_MAP_ENV", str(explicit))

    loaded = _env.load_env(strict=False)
    assert loaded == explicit
    assert os.environ.get("EXPLICIT_VAR") == "yes"
    # Project .env is never loaded
    assert "PROJECT_VAR" not in os.environ


def test_strict_with_only_the_map_env_loads_quietly(
    fake_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """strict=True must succeed when THE_MAP_ENV points at a real file, even
    if there is no project .env — the explicit override is honored."""
    explicit = tmp_path / "explicit.env"
    explicit.write_text("VAR=yes\n")
    monkeypatch.setenv("THE_MAP_ENV", str(explicit))

    # Should not raise SystemExit
    loaded = _env.load_env(strict=True)
    assert loaded == explicit
