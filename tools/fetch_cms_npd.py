"""Download CMS National Provider Directory bulk releases.

CMS publishes the NPD as zstd-compressed NDJSON on a weekly cadence at
https://directory.cms.gov/. Each file is fetched via
    GET https://directory.cms.gov/downloads/{filename}.zst
which 302-redirects to a pre-signed S3 URL (us-east-1, TTL = 3600s). Following
redirects works in `requests`; the signing happens server-side per-request, so
re-runs always get a fresh URL — no auth or refresh-token bookkeeping needed.

The manifest at /downloads/manifest.json lists every available resource with
compressed/original byte counts. We pin the release date from the filename
(`Endpoint_2026-05-07_2128.ndjson` → `2026-05-07`) and snapshot the manifest
alongside the downloads so the same parquet/json indexes can be rebuilt later.

Default skips Practitioner (18GB compressed, no use case for the-map's
endpoint→hospital identity work). Pass --all to include it.

Storage layout (out-of-repo, parallels ~/back/data/pos/ for the POS pipeline):
    $THE_MAP_CMS_NPD_DIR/  (default: ~/back/data/cms-npd/)
      {release_date}/
        manifest.json
        Endpoint.ndjson.zst
        Organization.ndjson.zst
        OrganizationAffiliation.ndjson.zst
        Location.ndjson.zst
        PractitionerRole.ndjson.zst
        .download_complete

Usage:
    python -m tools.fetch_cms_npd                          # default 5 files
    python -m tools.fetch_cms_npd --files Endpoint,Organization
    python -m tools.fetch_cms_npd --all                    # include 18GB Practitioner
    python -m tools.fetch_cms_npd --dir /custom/path       # override storage location
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import requests

MANIFEST_URL = "https://directory.cms.gov/downloads/manifest.json"
FILE_URL_TEMPLATE = "https://directory.cms.gov/downloads/{name}.zst"

# Default fetch set. Practitioner (18GB compressed, ~5.5M records of individual
# clinicians) is excluded by default: the-map cares about endpoint→organization
# identity, not individual provider directories. --all includes it.
DEFAULT_RESOURCES = ("Endpoint", "Organization", "OrganizationAffiliation", "Location", "PractitionerRole")
ALL_RESOURCES = (*DEFAULT_RESOURCES, "Practitioner")

USER_AGENT = (
    "mockhealth-map/0.1 (+https://mock.health; nate@mock.health) cms-npd-fetcher"
)


def default_storage_dir() -> Path:
    env = os.environ.get("THE_MAP_CMS_NPD_DIR")
    if env:
        return Path(env).expanduser()
    return Path("~/back/data/cms-npd").expanduser()


def _resource_from_filename(filename: str) -> str:
    """Endpoint_2026-05-07_2128.ndjson → Endpoint"""
    return filename.split("_", 1)[0]


def _release_date_from_filename(filename: str) -> str:
    """Endpoint_2026-05-07_2128.ndjson → 2026-05-07"""
    m = re.search(r"_(\d{4}-\d{2}-\d{2})_", filename)
    if not m:
        raise ValueError(f"could not parse release date from {filename!r}")
    return m.group(1)


def fetch_manifest() -> dict:
    r = requests.get(MANIFEST_URL, headers={"User-Agent": USER_AGENT}, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return r.json()


def _format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    return f"{n / 1024 / 1024 / 1024:.2f} GB"


def stream_download(url: str, dest: Path, expected_bytes: int) -> None:
    """Atomic download via .partial sidecar. No resume — re-fetch on failure
    is cheap relative to dealing with stale signed URLs and Range edge cases."""
    tmp = dest.with_suffix(dest.suffix + ".partial")
    headers = {"User-Agent": USER_AGENT}
    chunk_size = 1024 * 1024  # 1 MB

    print(f"  Downloading {dest.name}  ({_format_bytes(expected_bytes)} expected)")
    t0 = time.monotonic()
    downloaded = 0
    with requests.get(url, headers=headers, stream=True, timeout=300, allow_redirects=True) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                elapsed = time.monotonic() - t0
                if elapsed > 0:
                    mbps = downloaded / 1024 / 1024 / elapsed
                    pct = 100 * downloaded / expected_bytes if expected_bytes else 0
                    sys.stdout.write(
                        f"\r    {_format_bytes(downloaded):>10s} / {_format_bytes(expected_bytes):<10s}"
                        f"  ({pct:5.1f}%, {mbps:5.1f} MB/s)   "
                    )
                    sys.stdout.flush()
    sys.stdout.write("\n")
    sys.stdout.flush()

    actual = tmp.stat().st_size
    if expected_bytes and actual != expected_bytes:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"size mismatch for {dest.name}: got {actual} bytes, expected {expected_bytes}"
        )
    tmp.replace(dest)


def fetch(resources: tuple[str, ...], storage_dir: Path, *, release_override: str | None = None) -> Path:
    print(f"Fetching manifest from {MANIFEST_URL}")
    manifest = fetch_manifest()
    files: dict[str, dict] = manifest.get("files") or {}
    if not files:
        sys.exit("ERROR: manifest has no `files` block")

    sample_filename = next(iter(files))
    release_date = _release_date_from_filename(sample_filename)
    if release_override and release_override != release_date:
        sys.exit(
            f"ERROR: manifest is release {release_date}, --release={release_override} was requested. "
            f"CMS only publishes the current release; historical snapshots aren't backfetched."
        )

    out_dir = storage_dir / release_date
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    print(f"Release: {release_date}")
    print(f"Storage: {out_dir}")

    requested = set(resources)
    unknown_in_manifest = requested - {_resource_from_filename(f) for f in files}
    if unknown_in_manifest:
        sys.exit(f"ERROR: requested resources not in manifest: {sorted(unknown_in_manifest)}")

    plan: list[tuple[str, dict]] = []
    total_bytes = 0
    for filename, info in sorted(files.items()):
        resource = _resource_from_filename(filename)
        if resource not in requested:
            continue
        plan.append((filename, info))
        total_bytes += info.get("compressed_bytes") or 0

    print(f"Files to fetch: {len(plan)} ({_format_bytes(total_bytes)} compressed)")

    for filename, info in plan:
        resource = _resource_from_filename(filename)
        dest = out_dir / f"{resource}.ndjson.zst"
        expected = info.get("compressed_bytes") or 0
        if dest.exists() and (not expected or dest.stat().st_size == expected):
            print(f"  {dest.name} already present ({_format_bytes(dest.stat().st_size)}), skipping")
            continue
        url = FILE_URL_TEMPLATE.format(name=filename)
        stream_download(url, dest, expected)

    (out_dir / ".download_complete").write_text(release_date + "\n")
    print(f"\nDone. {len(plan)} files written to {out_dir}")
    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir",
        help="Storage directory (default: $THE_MAP_CMS_NPD_DIR or ~/back/data/cms-npd)",
    )
    ap.add_argument(
        "--files",
        help=f"Comma-separated resource names (default: {','.join(DEFAULT_RESOURCES)})",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Include Practitioner (18 GB compressed); off by default",
    )
    ap.add_argument(
        "--release",
        help="Pin a release date (e.g. 2026-05-07). CMS only serves the current release; "
        "this flag is for asserting which release you expect.",
    )
    args = ap.parse_args()

    storage_dir = Path(args.dir).expanduser() if args.dir else default_storage_dir()

    if args.all:
        resources = ALL_RESOURCES
    elif args.files:
        resources = tuple(r.strip() for r in args.files.split(",") if r.strip())
        unknown = set(resources) - set(ALL_RESOURCES)
        if unknown:
            sys.exit(f"ERROR: unknown resources: {sorted(unknown)}. Known: {ALL_RESOURCES}")
    else:
        resources = DEFAULT_RESOURCES

    fetch(resources, storage_dir, release_override=args.release)
    return 0


if __name__ == "__main__":
    sys.exit(main())
