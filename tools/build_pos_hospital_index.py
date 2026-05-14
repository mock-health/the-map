"""Build a Medicare-facility CCN catalog from CMS POS / iQIES extracts.

CMS publishes Provider-of-Services data quarterly through three source systems:
* QIES (Hospital_and_other.DATA.QX_YYYY.csv): hospitals + ambulatory facilities
  with UPPERCASE column names and the classic ``PRVDR_CTGRY_CD`` taxonomy.
* iQIES (POS_File_iQIES_QX_YYYY.csv): post-acute / long-term-care providers
  (SNFs, HHAs, hospices, IRFs, LTCHs) with ``lowercase_snake_case`` columns
  and a different ``prvdr_type_id`` taxonomy.
* Clinical Labs: out of scope here.

This tool detects each CSV's format by header inspection, projects rows into a
single canonical schema, and emits a compact JSON catalog under
``data/cms-pos/hospitals-{captured_date}.json`` (hospitals only — back-compat)
or ``data/cms-pos/providers-{captured_date}.json`` (broader categories).
Downstream ``tools.resolve_endpoints_to_pos`` joins each FHIR brand-bundle
endpoint against this catalog to attach a CCN + normalized address.

The QIES CSV is ~30 MB; iQIES is ~175 MB. When both are present in the same
dated source directory, ``build_index`` unions them; iQIES rows are tagged
with the synthetic category code ``iqies``.

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

# Search paths in priority order. ``data/raw/cms-pos`` is the new automated
# location written by ``tools.fetch_cms_pos`` and is scanned recursively
# (zips/CSVs live in dated subdirectories). The legacy paths preserve
# pre-PR-3 manual-download layouts.
DEFAULT_GLOB_DIRS = [
    REPO_ROOT / "data" / "raw" / "cms-pos",
    Path.home() / "back" / "data" / "pos",
    REPO_ROOT / "data" / "pos",
]

# Old (pre-2024) CMS layout: a single hospital+non-hospital-facilities zip
# named by data-as-of date.
POS_ZIP_PATTERN = re.compile(
    r"POS_File_Hospital_Non_Hospital_Facilities_(\d{8})\.zip$"
)
# Current data.cms.gov layout (QIES source system): a bare CSV named by
# quarter. The data-as-of date is encoded in the catalog distribution
# title — preserved in the sibling ``.provenance.json``.
POS_CSV_PATTERN = re.compile(r"Hospital_and_other\.DATA\.Q(\d)_(\d{4})\.csv$")
# iQIES source system: same dated subdir alongside the QIES file. Different
# filename, different column schema (see ``_detect_format``).
IQIES_CSV_PATTERN = re.compile(r"POS_File_iQIES_Q(\d)_(\d{4})\.csv$")

HOSPITAL_CATEGORY = "01"
ACTIVE_TERMINATION_CODE = "00"

# Synthetic category code that tags iQIES (post-acute / LTC) rows. iQIES uses
# a different taxonomy (``prvdr_type_id`` integers — 1=SNF, 3=HHA, 12=hospice,
# 20=ALF, etc.) that doesn't map cleanly onto the QIES PRVDR_CTGRY_CD set.
# Rather than guess a mapping, we lump every iQIES facility under one synthetic
# label so the resolver can opt into post-acute coverage via the
# ``providers-*`` catalog while leaving hospital-mode (PRVDR_CTGRY_CD="01")
# untouched.
IQIES_SYNTHETIC_CATEGORY = "iqies"

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
    IQIES_SYNTHETIC_CATEGORY: "iQIES (LTC / post-acute)",
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


def _pos_filename_matches(name: str) -> bool:
    return bool(
        POS_ZIP_PATTERN.search(name)
        or POS_CSV_PATTERN.search(name)
        or IQIES_CSV_PATTERN.search(name)
    )


def _detect_format(fieldnames: list[str] | None) -> str:
    """Return 'qies' or 'iqies' based on header casing.

    The two CMS source systems use disjoint header conventions —
    UPPERCASE for QIES, lowercase for iQIES — so the first row's
    ``PRVDR_NUM`` vs ``prvdr_num`` field is an unambiguous signal.
    Raises ``ValueError`` if neither marker is present.
    """
    if not fieldnames:
        raise ValueError("POS CSV has no header row")
    fields = set(fieldnames)
    if "PRVDR_NUM" in fields:
        return "qies"
    if "prvdr_num" in fields:
        return "iqies"
    raise ValueError(
        f"Unrecognised POS CSV header: missing both PRVDR_NUM and prvdr_num "
        f"(first 8 columns: {fieldnames[:8]})"
    )


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
        if not d.is_dir():
            continue
        # The fetcher's bucket dir has a dated-subdirectory layout, so scan
        # recursively. Legacy dirs hold loose files, so a shallow iterdir is
        # sufficient and avoids wandering into unrelated trees.
        iterator = d.rglob("*") if d == REPO_ROOT / "data" / "raw" / "cms-pos" else d.iterdir()
        candidates.extend(p for p in iterator if p.is_file() and _pos_filename_matches(p.name))
    if not candidates:
        sys.exit(
            "ERROR: no POS file found. Pass --input <path> or set $THE_MAP_POS_CSV, "
            "or run `python -m tools.fetch_cms_pos` to download.\n"
            "Searched:\n  " + "\n  ".join(str(d) for d in DEFAULT_GLOB_DIRS)
        )
    # Newest by mtime so we follow the latest fetch automatically. Lexical
    # sort would break across naming conventions (zip vs csv).
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _quarter_start_date(quarter: int, year: int) -> str:
    month = {1: "01", 2: "04", 3: "07", 4: "10"}[quarter]
    return f"{year}-{month}-01"


def captured_date_from_filename(p: Path) -> str:
    """Best-effort data-as-of date for a POS input file.

    Order of preference:
      1. ``release_date`` from a sibling ``.provenance.json`` (canonical,
         written by ``tools.fetch_cms_pos`` from the DCAT distribution title).
      2. Legacy zip filename ``..._YYYYMMDD.zip``.
      3. New CSV filename ``Hospital_and_other.DATA.QX_YYYY.csv`` → first
         day of quarter.
    """
    provenance = p.parent / ".provenance.json"
    if provenance.is_file():
        try:
            record = json.loads(provenance.read_text())
            release = record.get("release_date")
            if isinstance(release, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", release):
                return release
        except (OSError, ValueError):
            pass
    m_zip = POS_ZIP_PATTERN.search(p.name)
    if m_zip:
        ymd = m_zip.group(1)
        return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
    m_csv = POS_CSV_PATTERN.search(p.name)
    if m_csv:
        return _quarter_start_date(int(m_csv.group(1)), int(m_csv.group(2)))
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
        "source_system": "qies",
    }


def project_row_iqies(row: dict) -> dict:
    """iQIES row → canonical dict.

    iQIES stores termination as a *date* (``trmntn_exprtn_dt``) rather than
    QIES's two-letter code, and lowercases every column. Provider type codes
    (``prvdr_type_id``) live in a different value space — see
    ``IQIES_SYNTHETIC_CATEGORY``. ``certification_date`` already arrives in
    ISO format (no normalisation needed).
    """
    return {
        "ccn": row["prvdr_num"].strip(),
        "name": row["fac_name"].strip(),
        "address_line": row.get("st_adr", "").strip(),
        "city": row.get("city_name", "").strip(),
        "state": row.get("state_cd", "").strip(),
        "zip": normalize_zip(row.get("zip_cd", "")),
        "phone": normalize_phone(row.get("phne_num", "")),
        "category_code": IQIES_SYNTHETIC_CATEGORY,
        "category_label": FHIR_RELEVANT_CATEGORIES[IQIES_SYNTHETIC_CATEGORY],
        "fac_subtype_code": row.get("prvdr_sbtyp_id", "").strip(),
        "bed_count": _parse_int(row.get("bed_cnt")),
        "urban_rural": row.get("cbsa_urbn_rrl_ind", "").strip(),
        "fips_state": row.get("fips_state_cd", "").strip(),
        "fips_county": row.get("fips_cnty_cd", "").strip(),
        "cbsa_code": row.get("cbsa_cd", "").strip(),
        "certification_date": row.get("crtfctn_dt", "").strip(),
        # No QIES-equivalent code; preserve the raw date for downstream filters.
        "termination_code": ACTIVE_TERMINATION_CODE,
        "termination_expiration_date": row.get("trmntn_exprtn_dt", "").strip(),
        "prvdr_type_id": row.get("prvdr_type_id", "").strip(),
        "source_system": "iqies",
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


def _build_index_one(
    input_path: Path,
    *,
    include_terminated: bool,
    categories: set[str],
    today_iso: str,
) -> tuple[list[dict], int]:
    """Build a CCN catalog from one POS or iQIES CSV.

    Format is detected from the header row (UPPERCASE → QIES,
    lowercase → iQIES). For iQIES rows, "active" means
    ``trmntn_exprtn_dt`` is empty or in the future relative to
    ``today_iso``. iQIES rows are tagged with the synthetic category code
    so callers requesting just hospitals don't inadvertently get the
    post-acute world.
    """
    text, handle = open_csv_stream(input_path)
    try:
        reader = csv.DictReader(text)
        fmt = _detect_format(list(reader.fieldnames) if reader.fieldnames else None)
        out: list[dict] = []
        scanned = 0
        for row in reader:
            scanned += 1
            if fmt == "qies":
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
                    sys.exit(f"ERROR: POS CSV missing expected column {e!s} in {input_path.name}")
            else:  # iqies
                if IQIES_SYNTHETIC_CATEGORY not in categories:
                    continue
                if not include_terminated:
                    trmn = row.get("trmntn_exprtn_dt", "").strip()
                    if trmn and trmn <= today_iso:
                        continue
                try:
                    out.append(project_row_iqies(row))
                except KeyError as e:
                    sys.exit(f"ERROR: iQIES CSV missing expected column {e!s} in {input_path.name}")
        return out, scanned
    finally:
        text.close()
        if handle is not None:
            handle.close()


def build_index(
    input_path: Path | list[Path],
    *,
    include_terminated: bool,
    categories: set[str] | None = None,
) -> tuple[list[dict], int]:
    """Build a CCN catalog from one or more POS / iQIES CSVs.

    Multiple inputs (e.g. the QIES Hospital_and_other CSV plus the iQIES
    LTC/post-acute CSV from the same dated dir) are concatenated. Output
    rows from different source systems carry a ``source_system`` field so
    downstream consumers can re-stratify.

    categories: filter rows by ``category_code``. ``None`` defaults to
    hospitals-only ({"01"}, back-compat). Pass
    ``set(FHIR_RELEVANT_CATEGORIES)`` for the broader catalog including
    iQIES.
    """
    if categories is None:
        categories = {HOSPITAL_CATEGORY}
    paths = [input_path] if isinstance(input_path, Path) else list(input_path)
    if not paths:
        return [], 0
    import datetime as _dt
    today_iso = _dt.date.today().isoformat()

    all_rows: list[dict] = []
    total_scanned = 0
    for p in paths:
        rows, scanned = _build_index_one(
            p,
            include_terminated=include_terminated,
            categories=categories,
            today_iso=today_iso,
        )
        all_rows.extend(rows)
        total_scanned += scanned
    return all_rows, total_scanned


def discover_all_inputs(primary: Path) -> list[Path]:
    """Given the primary POS input, return every sibling POS/iQIES CSV in
    its parent dir (e.g. ``Hospital_and_other.DATA.Q1_2026.csv`` paired with
    ``POS_File_iQIES_Q1_2026.csv``). Returned list is sorted by name and
    always contains the primary input first.
    """
    if not primary.parent.is_dir() or primary.suffix.lower() == ".zip":
        return [primary]
    siblings = []
    for f in primary.parent.iterdir():
        if not f.is_file():
            continue
        if f == primary:
            continue
        if POS_CSV_PATTERN.search(f.name) or IQIES_CSV_PATTERN.search(f.name):
            siblings.append(f)
    return [primary, *sorted(siblings)]


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
        "FHIR-publishing-eligible category — QIES FQHCs/ASCs/RHCs + iQIES "
        "SNFs/HHAs/hospices/IRFs), or a comma-separated code list "
        "(e.g., '01,12,15' or '01,iqies').",
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
    # In providers mode, union every POS/iQIES CSV sitting in the same dated
    # source dir so the catalog covers QIES + iQIES. Hospitals mode stays
    # single-file — iQIES has no PRVDR_CTGRY_CD="01" rows anyway.
    if catalog_label == "providers":
        inputs = discover_all_inputs(input_path)
        if len(inputs) > 1:
            print(f"Inputs ({len(inputs)}):")
            for p in inputs:
                print(f"  {p}")
        else:
            print(f"Input: {input_path}")
    else:
        inputs = [input_path]
        print(f"Input: {input_path}")

    rows, scanned = build_index(
        inputs,
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
