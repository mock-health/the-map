"""Shared env-file loader for the-map CLI tools.

Resolution order (first hit wins):
  1. ``$THE_MAP_ENV`` if set — explicit override.
  2. ``<repo>/.env`` — the public-user default. See ``.env.example``.

Phase A (anonymous ``/metadata`` fetch) and Phase F (anonymous brands-bundle
harvest) need zero credentials and should run on a fresh clone with no .env.
Those callers either don't import this module or call ``load_env(strict=False)``.

Phase 1+ (sandbox sweeps, transport probes, cross-validation) require sandbox
credentials. Those callers use ``load_env(strict=True)`` so contributors get
a clear error message naming every path that was tried, instead of a confusing
``KeyError`` ten frames deep.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent


def _candidate_env_files() -> list[Path]:
    """Return the ordered list of .env paths we'll try, in priority order."""
    paths: list[Path] = []

    explicit = os.environ.get("THE_MAP_ENV")
    if explicit:
        paths.append(Path(explicit).expanduser())

    paths.append(REPO_ROOT / ".env")

    # De-duplicate while preserving order — caller may set THE_MAP_ENV to the
    # project .env path and we shouldn't load it twice.
    seen: set[Path] = set()
    deduped: list[Path] = []
    for p in paths:
        try:
            resolved = p.resolve()
        except (OSError, RuntimeError):
            resolved = p
        if resolved not in seen:
            seen.add(resolved)
            deduped.append(p)
    return deduped


def load_env(*, strict: bool = False) -> Path | None:
    """Load the first candidate .env that exists. Return its path or None.

    If ``strict=True`` and no candidate is readable, exit code 1 with a message
    listing every path tried and a pointer to ``.env.example``.
    """
    for path in _candidate_env_files():
        if path.is_file():
            load_dotenv(path)
            return path
    if strict:
        tried = "\n  ".join(str(p) for p in _candidate_env_files())
        sys.exit(
            "No .env file found. Tried:\n  "
            f"{tried}\n\n"
            "Copy .env.example to .env and fill in the credentials for the "
            "EHR sandbox(es) you want to test against. See CONTRIBUTING.md "
            "for per-vendor sandbox program signup links."
        )
    return None


__all__ = ["REPO_ROOT", "load_env"]
