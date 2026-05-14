"""Smoke + filter tests for tools/build_pos_hospital_index.py.

Builds a tiny synthetic POS CSV with a mix of hospital/non-hospital rows and
active/terminated entries, then asserts the projection only emits the rows we
expect under each --include-terminated mode.
"""
from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import pytest

from tools import build_pos_hospital_index as bph

# Minimum subset of POS columns the projection actually reads. The real CSV
# has ~400 columns; reproducing all of them in a fixture would be brittle and
# add no signal — projection is keyed by name, missing columns surface as
# KeyError if the projection reads one we forgot.
POS_COLUMNS = [
    "PRVDR_CTGRY_CD", "PRVDR_CTGRY_SBTYP_CD", "PRVDR_NUM", "FAC_NAME",
    "ST_ADR", "CITY_NAME", "STATE_CD", "ZIP_CD", "PHNE_NUM",
    "PGM_TRMNTN_CD", "BED_CNT", "CBSA_URBN_RRL_IND",
    "FIPS_STATE_CD", "FIPS_CNTY_CD", "CBSA_CD", "CRTFCTN_DT",
]


def _row(**overrides) -> dict:
    base = {c: "" for c in POS_COLUMNS}
    base.update(overrides)
    return base


def _write_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=POS_COLUMNS, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _write_pos_zip(tmp_path: Path, rows: list[dict]) -> Path:
    csv_text = _write_csv(rows)
    zpath = tmp_path / "POS_File_Hospital_Non_Hospital_Facilities_20991231.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("POS_File_Hospital_Non_Hospital_Facilities_Q4_2099.csv", csv_text)
    return zpath


def test_filters_to_active_hospitals_only(tmp_path: Path) -> None:
    rows = [
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010001", FAC_NAME="ACTIVE HOSPITAL",
             STATE_CD="AL", ZIP_CD="36301", PGM_TRMNTN_CD="00",
             PRVDR_CTGRY_SBTYP_CD="01", BED_CNT="120"),
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010002", FAC_NAME="TERMINATED HOSPITAL",
             STATE_CD="AL", ZIP_CD="36302", PGM_TRMNTN_CD="01",
             PRVDR_CTGRY_SBTYP_CD="01", BED_CNT="0"),
        _row(PRVDR_CTGRY_CD="11", PRVDR_NUM="015001", FAC_NAME="A NURSING FACILITY",
             STATE_CD="AL", ZIP_CD="36303", PGM_TRMNTN_CD="00"),
    ]
    zpath = _write_pos_zip(tmp_path, rows)

    projected, scanned = bph.build_index(zpath, include_terminated=False)
    assert scanned == 3
    assert len(projected) == 1
    assert projected[0]["ccn"] == "010001"
    assert projected[0]["name"] == "ACTIVE HOSPITAL"
    assert projected[0]["bed_count"] == 120


def test_include_terminated_includes_them(tmp_path: Path) -> None:
    rows = [
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010001", FAC_NAME="ACTIVE",
             STATE_CD="TX", ZIP_CD="75001", PGM_TRMNTN_CD="00",
             PRVDR_CTGRY_SBTYP_CD="01"),
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010002", FAC_NAME="TERMINATED",
             STATE_CD="TX", ZIP_CD="75002", PGM_TRMNTN_CD="01",
             PRVDR_CTGRY_SBTYP_CD="01"),
    ]
    zpath = _write_pos_zip(tmp_path, rows)

    projected, _ = bph.build_index(zpath, include_terminated=True)
    ccns = sorted(r["ccn"] for r in projected)
    assert ccns == ["010001", "010002"]


def test_zip5_normalization(tmp_path: Path) -> None:
    """ZIP+4 fields must be truncated; whitespace and dashes stripped."""
    rows = [
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010001", FAC_NAME="ZIP9",
             STATE_CD="OH", ZIP_CD="44101-1234", PGM_TRMNTN_CD="00",
             PRVDR_CTGRY_SBTYP_CD="01"),
    ]
    zpath = _write_pos_zip(tmp_path, rows)
    projected, _ = bph.build_index(zpath, include_terminated=False)
    assert projected[0]["zip"] == "44101"


def test_phone_normalization(tmp_path: Path) -> None:
    rows = [
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010001", FAC_NAME="PHONE",
             STATE_CD="GA", ZIP_CD="30301", PGM_TRMNTN_CD="00",
             PRVDR_CTGRY_SBTYP_CD="01", PHNE_NUM="4045551212"),
    ]
    zpath = _write_pos_zip(tmp_path, rows)
    projected, _ = bph.build_index(zpath, include_terminated=False)
    assert projected[0]["phone"] == "404-555-1212"


def test_certification_date_iso(tmp_path: Path) -> None:
    rows = [
        _row(PRVDR_CTGRY_CD="01", PRVDR_NUM="010001", FAC_NAME="DATE",
             STATE_CD="NY", ZIP_CD="10001", PGM_TRMNTN_CD="00",
             PRVDR_CTGRY_SBTYP_CD="01", CRTFCTN_DT="20100315"),
    ]
    zpath = _write_pos_zip(tmp_path, rows)
    projected, _ = bph.build_index(zpath, include_terminated=False)
    assert projected[0]["certification_date"] == "2010-03-15"


def test_real_catalog_validates(repo_root: Path) -> None:
    """Smoke: if data/cms-pos/hospitals-*.json is checked in, it must have a
    sane shape. This guards against a contributor regenerating the catalog from
    a future POS file with a column rename and silently emitting empty rows."""
    import json
    catalogs = sorted((repo_root / "data" / "cms-pos").glob("hospitals-*.json"))
    if not catalogs:
        pytest.skip("no committed POS catalog to check")
    catalog = json.loads(catalogs[-1].read_text())
    # `facility_count` superseded `hospital_count` when the catalog gained
    # multi-category mode. Older committed catalogs still carry the old key.
    count = catalog.get("facility_count") or catalog.get("hospital_count")
    assert count == len(catalog["hospitals"])
    assert count > 1000, "hospital catalog suspiciously small"
    # Spot-check the first row has the projected shape
    first = catalog["hospitals"][0]
    for required in ("ccn", "name", "city", "state", "zip", "fac_subtype_code"):
        assert required in first, f"row missing {required!r}: {first}"
    # Sorted by CCN
    ccns = [h["ccn"] for h in catalog["hospitals"]]
    assert ccns == sorted(ccns), "catalog must be sorted by CCN for diff stability"
