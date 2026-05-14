"""Tests for ``tools/_fetch.py`` shared download utilities.

Network-touching code (``stream_download``) is exercised indirectly by the
per-dataset fetchers in CI; here we focus on the pure primitives.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from tools import _fetch


def test_format_bytes() -> None:
    assert _fetch.format_bytes(0) == "0 B"
    assert _fetch.format_bytes(512) == "512 B"
    assert _fetch.format_bytes(2048) == "2.0 KB"
    assert _fetch.format_bytes(5 * 1024 * 1024) == "5.0 MB"
    assert _fetch.format_bytes(int(2.5 * 1024 * 1024 * 1024)) == "2.50 GB"


def test_now_utc_iso_shape() -> None:
    stamp = _fetch.now_utc_iso()
    # YYYY-MM-DDTHH:MM:SSZ — 20 chars, ends with Z, has T at position 10
    assert len(stamp) == 20
    assert stamp[10] == "T"
    assert stamp.endswith("Z")


def test_storage_root_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THE_MAP_TEST_DIR", raising=False)
    root = _fetch.storage_root("test-dataset", env_var="THE_MAP_TEST_DIR")
    assert root == _fetch.REPO_ROOT / "data" / "raw" / "test-dataset"


def test_storage_root_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THE_MAP_TEST_DIR", str(tmp_path / "external"))
    root = _fetch.storage_root("test-dataset", env_var="THE_MAP_TEST_DIR")
    assert root == tmp_path / "external"


def test_storage_root_empty_env_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    # Empty string env var should fall through to the default, not point at $PWD.
    monkeypatch.setenv("THE_MAP_TEST_DIR", "")
    root = _fetch.storage_root("test-dataset", env_var="THE_MAP_TEST_DIR")
    assert root == _fetch.REPO_ROOT / "data" / "raw" / "test-dataset"


def test_dated_storage_dir_creates_parents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THE_MAP_TEST_DIR", str(tmp_path / "bucket"))
    out = _fetch.dated_storage_dir("test-dataset", "2026-05-14", env_var="THE_MAP_TEST_DIR")
    assert out.is_dir()
    assert out == tmp_path / "bucket" / "2026-05-14"


def test_compute_sha256_matches_hashlib(tmp_path: Path) -> None:
    path = tmp_path / "blob.bin"
    payload = os.urandom(64 * 1024) + b"trailer\n"
    path.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert _fetch.compute_sha256(path) == expected


def test_archive_provenance_round_trip(tmp_path: Path) -> None:
    f1 = tmp_path / "data.bin"
    f1.write_bytes(b"hello world\n")
    # files that shouldn't appear in auto-discovery
    (tmp_path / ".hidden").write_text("nope")
    (tmp_path / "partial.zip.partial").write_bytes(b"x" * 10)

    path = _fetch.archive_provenance(
        tmp_path,
        dataset="test-dataset",
        source_url="https://example.cms.gov/data.bin",
        release_date="2026-05-14",
        tool="tests/test_fetch_lib.py",
    )
    assert path == tmp_path / ".provenance.json"

    record = json.loads(path.read_text())
    assert record["dataset"] == "test-dataset"
    assert record["source_url"] == "https://example.cms.gov/data.bin"
    assert record["release_date"] == "2026-05-14"
    assert record["tool"] == "tests/test_fetch_lib.py"
    assert record["captured_at"].endswith("Z")
    names = [f["name"] for f in record["files"]]
    assert "data.bin" in names
    assert ".hidden" not in names
    assert "partial.zip.partial" not in names
    [entry] = [f for f in record["files"] if f["name"] == "data.bin"]
    assert entry["size_bytes"] == len(b"hello world\n")
    assert entry["sha256"] == hashlib.sha256(b"hello world\n").hexdigest()


def test_archive_provenance_with_explicit_files(tmp_path: Path) -> None:
    f1 = tmp_path / "a.json"
    f1.write_text("{}")
    f2 = tmp_path / "b.json"
    f2.write_text("{}")
    f3 = tmp_path / "skip.json"  # exists on disk but not in `files=` arg
    f3.write_text("[]")

    _fetch.archive_provenance(
        tmp_path,
        dataset="test-dataset",
        source_url="https://example.cms.gov/index.json",
        release_date="2026-05-14",
        tool="t",
        files=[f1, f2],
    )
    record = json.loads((tmp_path / ".provenance.json").read_text())
    names = {f["name"] for f in record["files"]}
    assert names == {"a.json", "b.json"}


def test_read_provenance_round_trip(tmp_path: Path) -> None:
    assert _fetch.read_provenance(tmp_path) is None
    _fetch.archive_provenance(
        tmp_path,
        dataset="test-dataset",
        source_url="https://example.cms.gov/x",
        release_date="2026-05-14",
        tool="t",
    )
    record = _fetch.read_provenance(tmp_path)
    assert record is not None
    assert record["dataset"] == "test-dataset"


def test_latest_dated_subdir_prefers_newest(tmp_path: Path) -> None:
    for d in ("2026-04-01", "2026-05-08", "2026-05-14", "not-a-date", "2026"):
        (tmp_path / d).mkdir()
    # No provenance anywhere → fall back to alphabetically-newest valid date.
    assert _fetch.latest_dated_subdir(tmp_path) == tmp_path / "2026-05-14"


def test_latest_dated_subdir_prefers_provenanced(tmp_path: Path) -> None:
    for d in ("2026-04-01", "2026-05-08", "2026-05-14"):
        (tmp_path / d).mkdir()
    # Newest is missing provenance (e.g., interrupted download); the older
    # 2026-05-08 has one → it should win.
    _fetch.archive_provenance(
        tmp_path / "2026-05-08",
        dataset="test-dataset",
        source_url="https://example.cms.gov/x",
        release_date="2026-05-08",
        tool="t",
    )
    assert _fetch.latest_dated_subdir(tmp_path) == tmp_path / "2026-05-08"


def test_latest_dated_subdir_missing_root(tmp_path: Path) -> None:
    assert _fetch.latest_dated_subdir(tmp_path / "nonexistent") is None


def test_latest_dated_subdir_empty(tmp_path: Path) -> None:
    assert _fetch.latest_dated_subdir(tmp_path) is None
