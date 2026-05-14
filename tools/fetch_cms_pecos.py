"""Download the latest CMS Medicare Public Provider Enrollment File (PPEF).

CMS publishes the PPEF (the public companion to the PECOS enrollment system)
quarterly through the data.cms.gov DCAT catalog. The CSV is ~400 MB and
contains one row per (NPI, enrollment, program), with the cross-NPI-stable
PAC ID (``PECOS_ASCT_CNTL_ID``) and provider-type code.

Discovery:
* Read ``https://data.cms.gov/data.json``.
* Match dataset.title exactly: ``Medicare Fee-For-Service  Public Provider
  Enrollment`` (CMS publishes the title with a double space between
  "Service" and "Public" — preserve it verbatim).
* Take the CSV distribution's ``downloadURL``.

Storage layout (in-repo, gitignored under ``/data/raw/``):
    data/raw/cms-pecos/
      {data_as_of}/                      # e.g. 2026-01-02
        PPEF_Enrollment_Extract_YYYY.MM.DD.csv
        .provenance.json

Set ``$THE_MAP_PECOS_DIR`` to redirect the bucket location.

Usage:
    python -m tools.fetch_cms_pecos
    python -m tools.fetch_cms_pecos --dir /custom/path
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from tools._fetch import (
    archive_provenance,
    dated_storage_dir,
    discover_dcat_csv_distribution,
    storage_root,
    stream_download,
)

CATALOG_URL = "https://data.cms.gov/data.json"
DATASET = "cms-pecos"
ENV_VAR = "THE_MAP_PECOS_DIR"

# CMS publishes the dataset.title with a double space between "Service" and
# "Public". This is verbatim what data.json returns; if CMS ever fixes the
# whitespace we'll need to update this constant.
DATASET_TITLE = "Medicare Fee-For-Service  Public Provider Enrollment"


def fetch(*, storage_dir: Path | None = None) -> Path:
    print(f"Looking up '{DATASET_TITLE}' in {CATALOG_URL}")
    dist = discover_dcat_csv_distribution(CATALOG_URL, title=DATASET_TITLE)

    release_date = dist["data_as_of"] or dist["modified"]
    url = dist["downloadURL"]
    filename = dist["filename"]

    if storage_dir is not None:
        out_dir = storage_dir / release_date
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = dated_storage_dir(DATASET, release_date, env_var=ENV_VAR)
    dest = out_dir / filename

    print(f"Distribution: {dist['distribution_title']}")
    print(f"Release    : {release_date}")
    print(f"Source     : {url}")
    print(f"Storage    : {out_dir}")

    if dest.exists():
        print(f"  {dest.name} already present ({dest.stat().st_size} bytes), skipping download")
    else:
        stream_download(url, dest)

    archive_provenance(
        out_dir,
        dataset=DATASET,
        source_url=url,
        release_date=release_date,
        tool="tools/fetch_cms_pecos.py",
        files=[dest],
        extra={
            "distribution_title": dist["distribution_title"],
            "dataset_identifier": dist["dataset_identifier"],
        },
    )
    print(f"Done. {dest}")
    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir",
        help=f"Storage bucket directory (default: ${ENV_VAR} or <repo>/data/raw/{DATASET})",
    )
    args = ap.parse_args()

    if args.dir:
        storage_dir: Path | None = Path(args.dir).expanduser()
    else:
        storage_dir = storage_root(DATASET, env_var=ENV_VAR)

    try:
        fetch(storage_dir=storage_dir)
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
