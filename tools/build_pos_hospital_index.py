"""Build a hospital-only index from CMS Provider-of-Services (POS) CSV.

CMS publishes a quarterly POS file at https://data.cms.gov/provider-characteristics/
hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities
listing every Medicare-certified facility — hospitals, SNFs, HHAs, ESRDs, etc. —
keyed by CMS Certification Number (CCN, the field is `PRVDR_NUM`).

This tool streams the CSV, filters to *active hospitals* (`PRVDR_CTGRY_CD == "01"`
AND `PGM_TRMNTN_CD == "00"`), and emits a compact JSON catalog under
`data/cms-pos/hospitals-{captured_date}.json`. Downstream
`tools.resolve_endpoints_to_pos` joins each FHIR brand-bundle endpoint against
this catalog to attach a CCN + normalized address to every endpoint.

The CSV is ~168 MB and external to the repo. The filtered output is ~3-4 MB.

Usage:
    # autodiscover the newest POS_File_*.zip in ~/back/data/pos/
    python -m tools.build_pos_hospital_index

    # explicit input
    python -m tools.build_pos_hospital_index --input ~/back/data/pos/POS_File_Hospital_Non_Hospital_Facilities_20250601.zip

    # include terminated hospitals (default: only PGM_TRMNTN_CD == "00" actives)
    python -m tools.build_pos_hospital_index --include-terminated
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "data" / "cms-pos"

DEFAULT_GLOB_DIRS = [
    Path.home() / "back" / "data" / "pos",
    REPO_ROOT / "data" / "pos",
]

POS_ZIP_PATTERN = re.compile(
    r"POS_File_Hospital_Non_Hospital_Facilities_(\d{8})\.zip$"
)

HOSPITAL_CATEGORY = "01"
ACTIVE_TERMINATION_CODE = "00"

# CMS Provider-of-Services facility categories (PRVDR_CTGRY_CD).
# Source: POS Data Dictionary, code 21 reserved/internal-use only — exclude.
# This list is the *full* set of FHIR-publishing-eligible categories: any
# org with a CCN under one of these codes is a Medicare-certified facility
# that COULD plausibly publish a FHIR endpoint. Hospital-only is a tight
# subset of this; "providers" mode unions everything below.
FHIR_RELEVANT_CATEGORIES: dict[str, str] = {
    "01": "Hospital",
    "02": "Skilled Nursing Facility",
    "04": "Comprehensive Outpatient Rehab / PRTF",
    "05": "Home Health Agency / Hospice",
    "06": "End-Stage Renal Disease Facility",
    "08": "Rural Health Clinic",
    "09": "Comprehensive Outpatient Rehab Facility",
    "10": "FQHC (legacy)",
    "11": "FQHC",  # NB: codes 10 vs 11 vary across POS releases; include both.
    "12": "Ambulatory Surgical Center",
    "14": "Outpatient Physical Therapy",
    "15": "FQHC",
    "16": "Clinic",
    "17": "Specialty Hospital (subset of 01)",
    "21": "Ambulatory Surgical Center (alt)",
    "23": "Religious Non-Medical Health Care Institution",
}

PROJECTED_FIELDS = [
    "ccn",
    "name",
    "address_line",
    "city",
    "state",
    "zip",
    "phone",
    "category_code",
    "category_label",
    "fac_subtype_code",
    "bed_count",
    "urban_rural",
    "fips_state",
    "fips_county",
    "cbsa_code",
    "certification_date",
    "termination_code",
]


def discover_input(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit).expanduser()
        if not p.exists():
            sys.exit(f"ERROR: --input {p} does not exist")
        return p
    env = os.environ.get("THE_MAP_POS_CSV")
    if env:
        p = Path(env).expanduser()
        if not p.exists():
            sys.exit(f"ERROR: $THE_MAP_POS_CSV={p} does not exist")
        return p
    candidates: list[Path] = []
    for d in DEFAULT_GLOB_DIRS:
        if d.is_dir():
            candidates.extend(p for p in d.iterdir() if POS_ZIP_PATTERN.search(p.name))
    if not candidates:
        sys.exit(
            "ERROR: no POS file found. Pass --input <path> or set $THE_MAP_POS_CSV.\n"
            "Searched:\n  " + "\n  ".join(str(d) for d in DEFAULT_GLOB_DIRS)
        )
    candidates.sort(key=lambda p: p.name)
    return candidates[-1]


def captured_date_from_filename(p: Path) -> str:
    m = POS_ZIP_PATTERN.search(p.name)
    if m:
        ymd = m.group(1)
        return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
    return ""


def open_csv_stream(input_path: Path) -> tuple[io.TextIOBase, object | None]:
    """Return (text_stream, owned_handle_to_close).

    Supports either a raw .csv file or a .zip containing exactly one .csv.
    """
    if input_path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(input_path)
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            sys.exit(f"ERROR: expected 1 CSV in {input_path}, found {len(members)}: {members}")
        # POS files are Latin-1 / Windows-1252 in practice (older CMS files
        # have non-ASCII bytes in some FAC_NAME entries). Read as bytes,
        # decode permissively.
        raw = zf.open(members[0])
        return io.TextIOWrapper(raw, encoding="latin-1", newline=""), zf
    return open(input_path, encoding="latin-1", newline=""), None


def normalize_zip(z: str) -> str:
    z = (z or "").strip()
    if not z:
        return ""
    # POS zip can be 5 or 9 digit; we keep 5.
    digits = re.sub(r"\D", "", z)
    if len(digits) >= 5:
        return digits[:5]
    return digits


def normalize_phone(p: str) -> str:
    digits = re.sub(r"\D", "", p or "")
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return digits


def normalize_iso_date(yyyymmdd: str) -> str:
    s = (yyyymmdd or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return ""


def project_row(row: dict) -> dict:
    # POS column dictionary: https://data.cms.gov/sites/default/files/2024-12/
    # provider-of-services-file-hospital-and-non-hospital-facilities-data-dictionary.pdf
    cat_code = row.get("PRVDR_CTGRY_CD", "").strip()
    return {
        "ccn": row["PRVDR_NUM"].strip(),
        "name": row["FAC_NAME"].strip(),
        "address_line": row["ST_ADR"].strip(),
        "city": row["CITY_NAME"].strip(),
        "state": row["STATE_CD"].strip(),
        "zip": normalize_zip(row["ZIP_CD"]),
        "phone": normalize_phone(row.get("PHNE_NUM", "")),
        "category_code": cat_code,
        "category_label": FHIR_RELEVANT_CATEGORIES.get(cat_code, "unknown"),
        "fac_subtype_code": row["PRVDR_CTGRY_SBTYP_CD"].strip(),
        "bed_count": _parse_int(row.get("BED_CNT")),
        "urban_rural": row.get("CBSA_URBN_RRL_IND", "").strip(),
        "fips_state": row.get("FIPS_STATE_CD", "").strip(),
        "fips_county": row.get("FIPS_CNTY_CD", "").strip(),
        "cbsa_code": row.get("CBSA_CD", "").strip(),
        "certification_date": normalize_iso_date(row.get("CRTFCTN_DT", "")),
        "termination_code": row["PGM_TRMNTN_CD"].strip(),
    }


def _parse_int(s: str | None) -> int | None:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def build_index(
    input_path: Path,
    *,
    include_terminated: bool,
    categories: set[str] | None = None,
) -> tuple[list[dict], int]:
    """Build a CCN catalog from one POS CSV.

    categories: filter to these PRVDR_CTGRY_CD codes. None => hospitals only
    (back-compat). Pass `set(FHIR_RELEVANT_CATEGORIES)` for the broader
    provider catalog (FQHCs, ASCs, RHCs, SNFs, etc.).
    """
    if categories is None:
        categories = {HOSPITAL_CATEGORY}
    text, handle = open_csv_stream(input_path)
    try:
        reader = csv.DictReader(text)
        out: list[dict] = []
        scanned = 0
        for row in reader:
            scanned += 1
            if row.get("PRVDR_CTGRY_CD", "").strip() not in categories:
                continue
            if (
                not include_terminated
                and row.get("PGM_TRMNTN_CD", "").strip() != ACTIVE_TERMINATION_CODE
            ):
                continue
            try:
                out.append(project_row(row))
            except KeyError as e:
                sys.exit(f"ERROR: POS CSV missing expected column {e!s}")
        return out, scanned
    finally:
        text.close()
        if handle is not None:
            handle.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", help="Path to POS CSV or ZIP")
    ap.add_argument(
        "--include-terminated",
        action="store_true",
        help="Include facilities with PGM_TRMNTN_CD != '00' (default: actives only)",
    )
    ap.add_argument(
        "--categories",
        default="hospitals",
        help="'hospitals' (default; PRVDR_CTGRY_CD=01 only), 'all' (every "
        "FHIR-publishing-eligible category — FQHCs, ASCs, RHCs, SNFs, ESRD, "
        "hospice, …), or a comma-separated code list (e.g., '01,12,15').",
    )
    ap.add_argument(
        "--captured-date",
        help="ISO date for the output filename. Default: parsed from input filename, "
        "or today if not parseable.",
    )
    args = ap.parse_args()

    if args.categories == "hospitals":
        categories = {HOSPITAL_CATEGORY}
        catalog_label = "hospitals"
    elif args.categories == "all":
        categories = set(FHIR_RELEVANT_CATEGORIES)
        catalog_label = "providers"
    else:
        categories = {c.strip() for c in args.categories.split(",") if c.strip()}
        catalog_label = "providers" if categories != {HOSPITAL_CATEGORY} else "hospitals"

    input_path = discover_input(args.input)
    print(f"Input: {input_path}")

    rows, scanned = build_index(
        input_path,
        include_terminated=args.include_terminated,
        categories=categories,
    )
    rows.sort(key=lambda r: r["ccn"])
    cat_counts: dict[str, int] = {}
    for r in rows:
        cat_counts[r["category_code"]] = cat_counts.get(r["category_code"], 0) + 1
    print(f"  scanned   : {scanned} POS rows")
    print(f"  kept      : {len(rows)} (categories={sorted(categories)}"
          f"{', actives only' if not args.include_terminated else ', all'})")
    if catalog_label == "providers":
        for c, n in sorted(cat_counts.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {c} {FHIR_RELEVANT_CATEGORIES.get(c, '?'):>40}: {n:>6}")

    captured_date = args.captured_date or captured_date_from_filename(input_path)
    if not captured_date:
        import datetime
        captured_date = datetime.date.today().isoformat()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{catalog_label}-{captured_date}.json"
    # Keep the legacy key name `hospitals` for back-compat with
    # resolve_endpoints_to_pos so the resolver picks up the broader catalog
    # transparently when present.
    payload = {
        "source": "cms_provider_of_services",
        "source_filename": input_path.name,
        "captured_date": captured_date,
        "include_terminated": args.include_terminated,
        "categories_included": sorted(categories),
        "facility_count": len(rows),
        "facility_count_by_category": cat_counts,
        "fields": PROJECTED_FIELDS,
        "hospitals": rows,
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"  wrote     : {out_path.relative_to(REPO_ROOT)}  ({out_path.stat().st_size // 1024} KiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
