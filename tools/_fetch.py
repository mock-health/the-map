"""Shared fetch utilities for the-map CMS data ingesters.

Reusable primitives extracted from ``tools/fetch_cms_npd.py`` so the same
patterns power the per-dataset fetchers for NPPES, POS, and PECOS:

* Atomic streaming downloads with size + sha256 verification.
* Dated storage-directory layout (``data/raw/{dataset}/{release_date}/``)
  with an env-var escape hatch (``THE_MAP_<DATASET>_DIR``) for users with
  existing out-of-repo caches.
* Provenance archival — every successful fetch writes a ``.provenance.json``
  recording the upstream URL, release date, captured-at timestamp, per-file
  sizes/hashes, and the tool's git revision. This supersedes the older
  ``.download_complete`` sentinel which carried no source information.

Per-dataset fetchers wrap these with source-specific URL discovery logic
(directory.cms.gov manifest for NPD, download.cms.gov directory listing for
NPPES, data.cms.gov Socrata API for POS/PECOS).
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent

USER_AGENT = (
    "mockhealth-map/0.1 (+https://mock.health; nate@mock.health) the-map-fetcher"
)

PROVENANCE_FILENAME = ".provenance.json"


def storage_root(dataset: str, env_var: str | None = None) -> Path:
    """Return the bucket directory for a dataset.

    Precedence: ``$env_var`` (if set and non-empty) → ``<repo>/data/raw/{dataset}/``.
    """
    if env_var:
        v = os.environ.get(env_var)
        if v:
            return Path(v).expanduser()
    return REPO_ROOT / "data" / "raw" / dataset


def dated_storage_dir(
    dataset: str,
    release_date: str,
    *,
    env_var: str | None = None,
) -> Path:
    """``storage_root(dataset, env_var) / release_date``, with parents created."""
    out = storage_root(dataset, env_var) / release_date
    out.mkdir(parents=True, exist_ok=True)
    return out


def format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    return f"{n / 1024 / 1024 / 1024:.2f} GB"


def now_utc_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def git_rev() -> str | None:
    """Short git revision of the repo, or None outside a repo / no git."""
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return result.stdout.strip() or None
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return None


def compute_sha256(path: Path, *, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def stream_download(
    url: str,
    dest: Path,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    user_agent: str = USER_AGENT,
    chunk: int = 1 << 20,
    timeout: int = 300,
    headers: dict[str, str] | None = None,
    show_progress: bool = True,
) -> dict[str, Any]:
    """Atomically download ``url`` to ``dest`` via a ``.partial`` sidecar.

    Re-fetch on failure is cheap relative to dealing with stale signed URLs
    and Range-request edge cases, so this does not attempt resume.

    Returns ``{"size_bytes": int, "sha256": str}``. Raises ``RuntimeError`` if
    ``expected_size`` or ``expected_sha256`` is set and disagrees with the
    download.
    """
    tmp = dest.with_suffix(dest.suffix + ".partial")
    req_headers = {"User-Agent": user_agent}
    if headers:
        req_headers.update(headers)

    if show_progress:
        size_hint = format_bytes(expected_size) if expected_size else "unknown size"
        print(f"  Downloading {dest.name}  ({size_hint} expected)")

    h = hashlib.sha256()
    t0 = time.monotonic()
    downloaded = 0
    with requests.get(
        url,
        headers=req_headers,
        stream=True,
        timeout=timeout,
        allow_redirects=True,
    ) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for buf in resp.iter_content(chunk_size=chunk):
                if not buf:
                    continue
                f.write(buf)
                h.update(buf)
                downloaded += len(buf)
                if show_progress:
                    elapsed = time.monotonic() - t0
                    if elapsed > 0:
                        mbps = downloaded / 1024 / 1024 / elapsed
                        if expected_size:
                            pct = 100 * downloaded / expected_size
                            sys.stdout.write(
                                f"\r    {format_bytes(downloaded):>10s} / "
                                f"{format_bytes(expected_size):<10s}  "
                                f"({pct:5.1f}%, {mbps:5.1f} MB/s)   "
                            )
                        else:
                            sys.stdout.write(
                                f"\r    {format_bytes(downloaded):>10s}  "
                                f"({mbps:5.1f} MB/s)   "
                            )
                        sys.stdout.flush()
    if show_progress:
        sys.stdout.write("\n")
        sys.stdout.flush()

    actual_size = tmp.stat().st_size
    actual_sha = h.hexdigest()

    if expected_size is not None and actual_size != expected_size:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"size mismatch for {dest.name}: got {actual_size} bytes, "
            f"expected {expected_size}"
        )
    if expected_sha256 is not None and actual_sha != expected_sha256:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"sha256 mismatch for {dest.name}: got {actual_sha}, "
            f"expected {expected_sha256}"
        )

    tmp.replace(dest)
    return {"size_bytes": actual_size, "sha256": actual_sha}


def archive_provenance(
    out_dir: Path,
    *,
    dataset: str,
    source_url: str,
    release_date: str,
    tool: str,
    files: list[Path] | None = None,
    captured_at: str | None = None,
    extra: dict[str, Any] | None = None,
    filename: str = PROVENANCE_FILENAME,
) -> Path:
    """Write ``out_dir/<filename>`` and return its path.

    If ``files`` is provided, each entry's size and sha256 are recomputed
    from disk so the provenance record describes exactly what currently
    exists in ``out_dir`` (idempotent across re-runs that skip cached
    downloads). Hidden files (``.provenance*.json``, ``.partial`` sidecars)
    are excluded automatically from the auto-discovery path.

    ``filename`` lets a single dated dir hold provenance for multiple
    upstream sources side-by-side (e.g. ``.provenance-qies.json`` and
    ``.provenance-iqies.json``) without clobbering each other.
    """
    if files is None:
        files = sorted(
            p for p in out_dir.iterdir()
            if p.is_file()
            and not p.name.startswith(".")
            and not p.name.endswith(".partial")
            and not (p.name.startswith("provenance") and p.name.endswith(".json"))
        )

    file_entries = []
    for path in files:
        if not path.exists():
            continue
        file_entries.append({
            "name": path.name,
            "size_bytes": path.stat().st_size,
            "sha256": compute_sha256(path),
        })

    record: dict[str, Any] = {
        "dataset": dataset,
        "source_url": source_url,
        "release_date": release_date,
        "captured_at": captured_at or now_utc_iso(),
        "tool": tool,
        "tool_git_rev": git_rev(),
        "files": file_entries,
    }
    if extra:
        record.update(extra)

    path = out_dir / filename
    path.write_text(json.dumps(record, indent=2) + "\n")
    return path


def discover_dcat_csv_distribution(
    catalog_url: str,
    *,
    title: str,
    user_agent: str = USER_AGENT,
    timeout: int = 30,
) -> dict[str, Any]:
    """Find a CSV distribution for a dataset in a DCAT 1.1 JSON catalog.

    ``data.cms.gov`` publishes its full dataset catalog at
    ``https://data.cms.gov/data.json`` (DCAT 1.1). Datasets list multiple
    distributions per release — JSON API endpoints (``format: 'API'``) and
    a downloadable CSV (``format: 'CSV'`` / ``mediaType: 'text/csv'``).

    Matches ``title`` *exactly* against ``dataset.title`` so we don't pick
    up sibling datasets that happen to share a prefix (e.g. "Provider of
    Services File - Quality Improvement and Evaluation System" vs. the
    Internet QIES variant).

    Returns a dict with keys: ``downloadURL``, ``modified`` (ISO date),
    ``filename``, ``distribution_title``, ``dataset_identifier``.

    Raises ``RuntimeError`` if zero or multiple datasets match the title,
    or if the matching dataset has no CSV distribution.
    """
    resp = requests.get(
        catalog_url,
        headers={"User-Agent": user_agent},
        timeout=timeout,
    )
    resp.raise_for_status()
    catalog = resp.json()
    datasets = catalog.get("dataset") or []

    target_title = title.strip()
    matches = [d for d in datasets if (d.get("title") or "").strip() == target_title]
    if not matches:
        # Helpful fallback: case-insensitive substring match for diagnostics.
        approx = [
            d.get("title") for d in datasets
            if target_title.lower() in (d.get("title") or "").lower()
        ][:5]
        hint = ""
        if approx:
            hint = "\nNear matches:\n  " + "\n  ".join(repr(t) for t in approx)
        raise RuntimeError(
            f"no dataset in {catalog_url} has title {target_title!r}.{hint}"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"multiple datasets in {catalog_url} share title {target_title!r} "
            f"({len(matches)} matches) — refine the title."
        )
    dataset = matches[0]

    csv_dists = [
        d for d in (dataset.get("distribution") or [])
        if (d.get("format") or "").upper() == "CSV" or d.get("mediaType") == "text/csv"
    ]
    csv_dists = [d for d in csv_dists if d.get("downloadURL")]
    if not csv_dists:
        raise RuntimeError(
            f"dataset {target_title!r} has no CSV distribution with a downloadURL"
        )

    # If the catalog lists multiple CSV distributions (e.g. historical
    # archives), prefer the most-recently-modified one — that's the current
    # release. Catalogs almost always sort newest-first, but don't rely on it.
    def _mod(d: dict[str, Any]) -> str:
        return d.get("modified") or dataset.get("modified") or ""

    csv_dists.sort(key=_mod, reverse=True)
    chosen = csv_dists[0]

    download_url = chosen["downloadURL"]
    dist_title = chosen.get("title") or dataset.get("title") or ""
    # data.cms.gov distribution titles encode the data-as-of date as a
    # ' : YYYY-MM-DD' suffix (e.g. "Provider of Services ... : 2026-01-01").
    # Extract it; the directory layout uses this for semantic naming.
    import re

    m = re.search(r":\s*(\d{4}-\d{2}-\d{2})\s*$", dist_title)
    data_as_of = m.group(1) if m else None
    return {
        "downloadURL": download_url,
        "modified": _mod(chosen),
        "data_as_of": data_as_of,
        "filename": download_url.rsplit("/", 1)[-1],
        "distribution_title": dist_title,
        "dataset_identifier": dataset.get("identifier"),
    }


def read_provenance(out_dir: Path) -> dict[str, Any] | None:
    """Return parsed provenance record for a dated dir, or None if absent."""
    path = out_dir / PROVENANCE_FILENAME
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def latest_dated_subdir(root: Path) -> Path | None:
    """Pick the alphabetically-newest YYYY-MM-DD subdirectory under ``root``.

    Returns ``None`` if ``root`` doesn't exist or has no dated subdirs. The
    naming convention is ISO date, so lexical sort = chronological sort.
    Releases without a provenance record (interrupted downloads) are skipped.
    """
    if not root.is_dir():
        return None
    candidates = sorted(
        p for p in root.iterdir()
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-"
    )
    for cand in reversed(candidates):
        if (cand / PROVENANCE_FILENAME).is_file():
            return cand
    return candidates[-1] if candidates else None


__all__ = [
    "PROVENANCE_FILENAME",
    "REPO_ROOT",
    "USER_AGENT",
    "archive_provenance",
    "compute_sha256",
    "dated_storage_dir",
    "discover_dcat_csv_distribution",
    "format_bytes",
    "git_rev",
    "latest_dated_subdir",
    "now_utc_iso",
    "read_provenance",
    "storage_root",
    "stream_download",
]
