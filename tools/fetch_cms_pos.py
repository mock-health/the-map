"""Download the latest CMS Provider-of-Services (POS) hospital + other-facility CSV.

CMS publishes the POS file quarterly through the data.cms.gov DCAT catalog.
The catalog at ``https://data.cms.gov/data.json`` lists multiple distributions
per dataset — JSON API endpoints (Socrata) and a downloadable CSV. We grab
the CSV.

Historically the dataset was named
``POS_File_Hospital_Non_Hospital_Facilities_YYYYMMDD.zip``. Around 2024 CMS
split it across the QIES, iQIES, and Clinical-Labs source systems and
switched to bare CSV. The Hospital+Other-Facility data — the same scope
the pre-split file covered — now lives at:

    Provider of Services File - Quality Improvement and Evaluation System
    (current filename: Hospital_and_other.DATA.QX_YYYY.csv)

iQIES (Internet QIES) carries the LTC / post-acute long-term-care providers.
``--source iqies`` fetches that file instead; default is ``qies``.

Storage layout (in-repo, gitignored under ``/data/raw/``):
    data/raw/cms-pos/
      {modified_date}/                          # catalog "modified" field
        Hospital_and_other.DATA.QX_YYYY.csv
        .provenance.json

Set ``$THE_MAP_POS_DIR`` to redirect the bucket location.

Usage:
    python -m tools.fetch_cms_pos
    python -m tools.fetch_cms_pos --source iqies
    python -m tools.fetch_cms_pos --dir /custom/path
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
DATASET = "cms-pos"
ENV_VAR = "THE_MAP_POS_DIR"

SOURCES = {
    "qies": "Provider of Services File - Quality Improvement and Evaluation System",
    "iqies": "Provider of Services File - Internet Quality Improvement and Evaluation System",
    "labs": "Provider of Services File - Clinical Laboratories",
}


def fetch(
    *,
    source: str = "qies",
    storage_dir: Path | None = None,
) -> Path:
    if source not in SOURCES:
        sys.exit(f"ERROR: unknown --source {source!r}. Choose from {sorted(SOURCES)}.")
    title = SOURCES[source]

    print(f"Looking up '{title}' in {CATALOG_URL}")
    dist = discover_dcat_csv_distribution(CATALOG_URL, title=title)

    # Prefer the data-as-of date from the distribution title for the directory
    # name — that's what downstream consumers care about (e.g. the build
    # script's `hospitals-{captured_date}.json` output). Fall back to the
    # catalog `modified` date if the title doesn't have a parseable suffix.
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

    # Per-source provenance filename so a single dated dir can hold the QIES
    # and iQIES extracts side-by-side without one fetch overwriting the other.
    archive_provenance(
        out_dir,
        dataset=DATASET,
        source_url=url,
        release_date=release_date,
        tool="tools/fetch_cms_pos.py",
        files=[dest],
        extra={
            "source": source,
            "distribution_title": dist["distribution_title"],
            "dataset_identifier": dist["dataset_identifier"],
        },
        filename=f".provenance-{source}.json",
    )
    print(f"Done. {dest}")
    return out_dir


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dir",
        help=f"Storage bucket directory (default: ${ENV_VAR} or <repo>/data/raw/{DATASET})",
    )
    ap.add_argument(
        "--source",
        choices=sorted(SOURCES),
        default="qies",
        help="Which POS source-system extract to fetch. "
             "qies = Hospital+Other Facilities (default, replaces the old "
             "POS_File_Hospital_Non_Hospital_Facilities.zip). "
             "iqies = Long-term care / post-acute. "
             "labs = Clinical Laboratories.",
    )
    args = ap.parse_args()

    if args.dir:
        storage_dir: Path | None = Path(args.dir).expanduser()
    else:
        storage_dir = storage_root(DATASET, env_var=ENV_VAR)

    try:
        fetch(source=args.source, storage_dir=storage_dir)
    except RuntimeError as e:
        sys.exit(f"ERROR: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
