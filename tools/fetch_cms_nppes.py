"""Download the latest CMS NPPES monthly Data Dissemination file.

CMS publishes the full NPPES catalogue once per month at
    https://download.cms.gov/nppes/NPPES_Data_Dissemination_{Month}_{Year}.zip
typically on the second weekend of the month (around the 8th). The zip is
~7 GB compressed and contains the master ``npidata_pfile_*.csv``,
``endpoint_pfile.csv``, and several taxonomy/PL files.

Filename discovery:
* Walk back from the current month, HEADing each candidate URL until one
  returns 200. We accept up to 4 months of slip because cutover days near
  the start of a month can lag CMS's publication.
* The directory listing at https://download.cms.gov/nppes/ is the canonical
  index; the predictable filename pattern is the fast path and the error
  message points contributors at the index if discovery fails.

Storage layout (in-repo, gitignored under ``/data/raw/``):
    data/raw/cms-nppes/
      {YYYY-MM}-01/
        NPPES_Data_Dissemination_{Month}_{Year}.zip
        .provenance.json

Set ``$THE_MAP_NPPES_DIR`` to redirect the bucket location.

The inner CSVs embed the exact data-as-of date (e.g. ``20260208``); that
date drives the downstream ``orgs-YYYY-MM-DD.jsonl`` /
``fhir-endpoints-YYYY-MM-DD.json`` filenames written by
``tools.build_nppes_index``. Using ``{YYYY-MM}-01`` for the raw-zip
directory keeps discovery predictable without a post-download rename.

Usage:
    python -m tools.fetch_cms_nppes
    python -m tools.fetch_cms_nppes --month 2026-04   # pin a specific release
    python -m tools.fetch_cms_nppes --dir /custom/path
"""
from __future__ import annotations

import argparse
import calendar
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import requests

from tools._fetch import (
    USER_AGENT,
    archive_provenance,
    dated_storage_dir,
    storage_root,
    stream_download,
)

NPPES_INDEX_URL = "https://download.cms.gov/nppes/"
URL_TEMPLATE = "https://download.cms.gov/nppes/NPPES_Data_Dissemination_{month}_{year}.zip"
DATASET = "cms-nppes"
ENV_VAR = "THE_MAP_NPPES_DIR"

MAX_MONTHS_BACK = 4


def _month_name(month: int) -> str:
    # calendar.month_name[1] == 'January'; index by 1..12.
    return calendar.month_name[month]


def discover_latest_url(
    today: date | None = None,
    *,
    max_months_back: int = MAX_MONTHS_BACK,
) -> tuple[str, date]:
    """Walk back from ``today`` (default: now) until a NPPES monthly zip exists.

    Returns ``(url, first_of_release_month)``. Raises ``RuntimeError`` if
    nothing is found within ``max_months_back``.
    """
    today = today or datetime.now(UTC).date()
    tried: list[str] = []
    for months_back in range(max_months_back + 1):
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        url = URL_TEMPLATE.format(month=_month_name(month), year=year)
        tried.append(url)
        resp = requests.head(
            url,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
            timeout=15,
        )
        if resp.status_code == 200:
            return url, date(year, month, 1)
    raise RuntimeError(
        "Could not locate a NPPES monthly release in the last "
        f"{max_months_back + 1} months. URLs tried:\n  "
        + "\n  ".join(tried)
        + f"\nCheck the index at {NPPES_INDEX_URL} — CMS may have changed the filename pattern."
    )


def url_for_month(release_month: date) -> str:
    return URL_TEMPLATE.format(month=_month_name(release_month.month), year=release_month.year)


def fetch(
    *,
    storage_dir: Path | None = None,
    pin_month: date | None = None,
) -> Path:
    if pin_month:
        url = url_for_month(pin_month)
        release_month = pin_month
        print(f"Pinned NPPES release: {release_month.isoformat()[:7]}")
    else:
        url, release_month = discover_latest_url()
        print(f"Latest NPPES release: {release_month.isoformat()[:7]}")

    release_date = release_month.isoformat()  # YYYY-MM-01

    if storage_dir is not None:
        out_dir = storage_dir / release_date
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = dated_storage_dir(DATASET, release_date, env_var=ENV_VAR)

    filename = url.rsplit("/", 1)[-1]
    dest = out_dir / filename

    # Probe size for the progress display; ignore on failure.
    expected_size: int | None = None
    head = requests.head(url, headers={"User-Agent": USER_AGENT}, allow_redirects=True, timeout=15)
    if head.status_code == 200 and head.headers.get("Content-Length"):
        expected_size = int(head.headers["Content-Length"])

    print(f"Storage: {out_dir}")
    print(f"Source : {url}")

    if dest.exists() and (not expected_size or dest.stat().st_size == expected_size):
        print(f"  {dest.name} already present, skipping download")
    else:
        stream_download(url, dest, expected_size=expected_size)

    archive_provenance(
        out_dir,
        dataset=DATASET,
        source_url=url,
        release_date=release_date,
        tool="tools/fetch_cms_nppes.py",
        files=[dest],
    )
    print(f"Done. {dest}")
    return out_dir


def _parse_pin_month(value: str) -> date:
    # Accept YYYY-MM or YYYY-MM-DD; anchor to first-of-month.
    parts = value.split("-")
    if len(parts) not in (2, 3):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM or YYYY-MM-DD, got {value!r}")
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"invalid YYYY-MM in {value!r}") from e
    if not (1 <= month <= 12):
        raise argparse.ArgumentTypeError(f"month out of range in {value!r}")
    return date(year, month, 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir",
        help=f"Storage bucket directory (default: ${ENV_VAR} or <repo>/data/raw/{DATASET})",
    )
    ap.add_argument(
        "--month",
        type=_parse_pin_month,
        help="Pin a specific release month (YYYY-MM). Default: latest available.",
    )
    args = ap.parse_args()

    if args.dir:
        storage_dir: Path | None = Path(args.dir).expanduser()
    else:
        storage_dir = storage_root(DATASET, env_var=ENV_VAR)

    try:
        fetch(storage_dir=storage_dir, pin_month=args.month)
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
