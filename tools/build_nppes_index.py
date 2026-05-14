"""Build NPPES indexes from the CMS monthly Data Dissemination zip.

Streams two CSVs out of the zip and produces two compact indexes used by
`tools.resolve_endpoints_to_nppes`:

  1. data/cms-nppes/fhir-endpoints-{release_yyyymmdd}.json
     The subset of `endpoint_pfile.csv` where `Endpoint Type ∈ {FHIR, REST}`.
     Each row carries (npi, url, url_norm, affiliation_name, state). ~46K
     records in the 2026-02 release — a small file (~5-10 MB).

  2. data/cms-nppes/orgs-{release_yyyymmdd}.jsonl
     The subset of `npidata_pfile.csv` where `Entity Type Code = 2` (orgs).
     Each line is a compact record with NPI + names + practice address +
     primary taxonomy. ~1.5M rows. JSONL for streaming downstream — full
     materialization would be ~300 MB.

The Endpoint Type filter is on purpose: NPPES catalogs Direct Messaging
(354K rows), CONNECT (90K), SOAP (56K), and OTHERS (46K) alongside FHIR/REST.
Only FHIR/REST give URL-shape parity with our fleet — the rest are not
queryable as `/metadata`. (CONNECT and SOAP could in principle resolve to
the same managing org, but that's a future Tier-3 join, not Tier-1.)

Org-name normalization: legal-entity suffixes (LLC, INC, PA, ...) are
stripped because vendor brands bundles use them inconsistently — a Cerner
endpoint labeled "Trinity Health Corporation" should join an NPPES org
named "TRINITY HEALTH CORP" without us hand-curating the suffix. This is
the same normalization the existing `harvest_production_capstmts` brands
lookup uses; kept in sync via the shared regex constants.

Usage:
    python -m tools.build_nppes_index
    python -m tools.build_nppes_index --zip /custom/path.zip
    python -m tools.build_nppes_index --skip-orgs   # endpoints only (5s)
    python -m tools.build_nppes_index --skip-endpoints
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import time
import zipfile
from pathlib import Path

from tools.luxera_endpoint_discovery import normalize_address

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "cms-nppes"
# Search paths in priority order. The first is the new automated-fetch
# location (tools.fetch_cms_nppes writes here); the second preserves the
# pre-PR-1 layout for users with existing manual downloads.
DEFAULT_NPPES_SEARCH_DIRS = (
    REPO_ROOT / "data" / "raw" / "cms-nppes",
    Path("~/back/data/downloads").expanduser(),
)

# Monthly NPPES release zips are named like
# NPPES_Data_Dissemination_February_2026.zip. The inner CSV filename embeds
# the actual end-date of the snapshot (20260208), which we prefer for the
# output path since it's unambiguous.
MONTHLY_RE = re.compile(r"^NPPES_Data_Dissemination_(\w+)_(\d{4})\.zip$")
INNER_CSV_DATE_RE = re.compile(r"[-_](\d{8})\.csv$")

# Endpoint types we care about. SOAP could in principle host a FHIR REST
# server but in practice NPPES uses SOAP for legacy CCDA exchanges. Keep
# only FHIR + REST — the ones that match our fleet URL shape.
FHIR_ENDPOINT_TYPES = {"FHIR", "REST"}

# Legal-entity suffix tokens — stripped from end of an org name for the
# normalized form used in lookups. Match order-sensitive: "P.A." before "PA"
# would be redundant since we strip punctuation first.
LEGAL_SUFFIX_TOKENS = (
    "LLC", "L L C", "INC", "INCORPORATED", "CORP", "CORPORATION",
    "LP", "LLP", "PLLC", "PA", "PC", "PLC", "LTD", "LIMITED", "CO", "COMPANY",
    "PROFESSIONAL CORPORATION",
)


def find_latest_monthly(search_dirs: tuple[Path, ...] = DEFAULT_NPPES_SEARCH_DIRS) -> Path:
    """Pick the most recent monthly zip across one or more candidate dirs.

    ``data/raw/cms-nppes/`` is scanned recursively (zips live in dated
    subdirectories written by ``tools.fetch_cms_nppes``). Other candidate
    dirs are scanned non-recursively for backward compatibility with
    manually-downloaded zips. Sort by mtime not name since
    'December_2025' sorts before 'February_2026' lexically.
    """
    candidates: list[Path] = []
    for d in search_dirs:
        if not d.is_dir():
            continue
        iterator = d.rglob("*") if d == REPO_ROOT / "data" / "raw" / "cms-nppes" else d.iterdir()
        for p in iterator:
            if p.is_file() and MONTHLY_RE.match(p.name):
                candidates.append(p)
    if not candidates:
        tried = "\n  ".join(str(d) for d in search_dirs)
        sys.exit(
            f"ERROR: no NPPES monthly zip found. Tried:\n  {tried}\n"
            f"Run `python -m tools.fetch_cms_nppes` to download the latest release."
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def release_date_from_inner_name(inner_name: str) -> str:
    """`npidata_pfile_20050523-20260208.csv` → `2026-02-08`."""
    m = INNER_CSV_DATE_RE.search(inner_name)
    if not m:
        return "unknown"
    s = m.group(1)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def normalize_org_name(name: str | None) -> str:
    """Compact key for joining org names across NPPES / vendor bundles.

    Lowercases, strips punctuation/whitespace, strips one trailing legal
    suffix token. Conservative — does NOT collapse 'and'/'&', 'hospital'/'hosp',
    etc.; those normalizations are too aggressive for unsupervised matching.
    """
    if not name:
        return ""
    s = name.upper().strip()
    # Drop punctuation that varies across catalogs (commas, periods,
    # apostrophes). Keep spaces and ampersand for now — those are
    # token-level info.
    s = re.sub(r"[\.,;:'\"()]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Strip a single trailing legal-entity suffix if present.
    for sfx in LEGAL_SUFFIX_TOKENS:
        if s.endswith(" " + sfx):
            s = s[: -len(sfx) - 1].rstrip()
            break
    return s


def _find_inner_csv(zf: zipfile.ZipFile, prefix: str) -> str:
    candidates = [
        n for n in zf.namelist()
        if n.startswith(prefix) and n.endswith(".csv") and "_fileheader" not in n
    ]
    if not candidates:
        sys.exit(f"ERROR: no {prefix}*.csv in {zf.filename}")
    return candidates[0]


def _stream_csv_from_zip(zf: zipfile.ZipFile, inner_name: str):
    """Yield dict rows from a CSV inside a zip without extracting.

    Wraps the binary stream in a TextIOWrapper so csv.DictReader can do
    standards-compliant quoted-field parsing — NPPES has commas embedded in
    org names like 'COMMUNITY HEALTH CENTERS OF AMERICA, LLC'.
    """
    with zf.open(inner_name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
        reader = csv.DictReader(text)
        yield from reader


def extract_fhir_endpoints(zip_path: Path, out_path: Path) -> tuple[int, int]:
    """Write the FHIR/REST endpoint subset of endpoint_pfile to out_path.

    Returns (total_endpoints_scanned, kept).
    """
    print(f"Extracting FHIR/REST endpoints from {zip_path.name}")
    t0 = time.monotonic()
    records: list[dict] = []
    total = 0
    by_type: dict[str, int] = {}
    with zipfile.ZipFile(zip_path) as zf:
        inner = _find_inner_csv(zf, "endpoint_pfile")
        release = release_date_from_inner_name(inner)
        for row in _stream_csv_from_zip(zf, inner):
            total += 1
            t = (row.get("Endpoint Type") or "").strip()
            by_type[t] = by_type.get(t, 0) + 1
            if t not in FHIR_ENDPOINT_TYPES:
                continue
            url = (row.get("Endpoint") or "").strip()
            npi = (row.get("NPI") or "").strip()
            if not url or not npi:
                continue
            records.append({
                "npi": npi,
                "url": url,
                "url_norm": normalize_address(url),
                "endpoint_type": t,
                "endpoint_desc": (row.get("Endpoint Description") or "").strip(),
                "affiliation_name": (row.get("Affiliation Legal Business Name") or "").strip(),
                "city": (row.get("Affiliation Address City") or "").strip(),
                "state": (row.get("Affiliation Address State") or "").strip(),
                "postal": (row.get("Affiliation Address Postal Code") or "").strip(),
            })
    dt = int(time.monotonic() - t0)
    print(f"  scanned {total:,} endpoints; kept {len(records):,} FHIR/REST ({dt}s)")
    print(f"  type distribution: {sorted(by_type.items(), key=lambda kv: -kv[1])[:6]}")
    out_path.write_text(json.dumps({
        "release_date": release,
        "source_zip": zip_path.name,
        "total_npp_endpoint_rows": total,
        "kept_fhir_rest": len(records),
        "endpoints": records,
    }, indent=2) + "\n")
    print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return total, len(records)


def extract_orgs(zip_path: Path, out_path: Path) -> tuple[int, int]:
    """Write Entity-Type-2 (organization) NPPES rows to out_path as JSONL.

    Returns (rows_scanned, rows_kept). Deactivated NPIs are skipped.
    """
    print(f"Extracting organization NPIs from {zip_path.name}")
    t0 = time.monotonic()
    total = 0
    kept = 0
    with zipfile.ZipFile(zip_path) as zf:
        inner = _find_inner_csv(zf, "npidata_pfile")
        release = release_date_from_inner_name(inner)
        with out_path.open("w") as out:
            # Header line containing release metadata so consumers can verify.
            out.write(json.dumps({
                "_header": True,
                "release_date": release,
                "source_zip": zip_path.name,
            }) + "\n")
            for row in _stream_csv_from_zip(zf, inner):
                total += 1
                if total % 500000 == 0:
                    print(f"  scanned {total:,} ({kept:,} kept, {int(time.monotonic() - t0)}s)")
                # Entity Type Code: "1" = individual provider, "2" = organization.
                if (row.get("Entity Type Code") or "").strip() != "2":
                    continue
                # Skip deactivated NPIs — they pollute the lookup with
                # since-dead orgs (closed hospitals, dissolved practices).
                if (row.get("NPI Deactivation Reason Code") or "").strip():
                    continue
                npi = (row.get("NPI") or "").strip()
                legal_name = (row.get("Provider Organization Name (Legal Business Name)") or "").strip()
                if not npi or not legal_name:
                    continue
                rec = {
                    "npi": npi,
                    "name": legal_name,
                    "name_norm": normalize_org_name(legal_name),
                    "city": (row.get("Provider Business Practice Location Address City Name") or "").strip(),
                    "state": (row.get("Provider Business Practice Location Address State Name") or "").strip(),
                    "addr": (row.get("Provider First Line Business Practice Location Address") or "").strip(),
                    "postal": (row.get("Provider Business Practice Location Address Postal Code") or "").strip(),
                    "taxonomy": (row.get("Healthcare Provider Taxonomy Code_1") or "").strip(),
                }
                other_name = (row.get("Provider Other Organization Name") or "").strip()
                if other_name:
                    rec["other_name"] = other_name
                    rec["other_name_norm"] = normalize_org_name(other_name)
                out.write(json.dumps(rec) + "\n")
                kept += 1
    dt = int(time.monotonic() - t0)
    print(f"  done: scanned {total:,}, kept {kept:,} orgs ({dt}s)")
    print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return total, kept


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    search_help = ", ".join(str(d) for d in DEFAULT_NPPES_SEARCH_DIRS)
    ap.add_argument(
        "--zip",
        help=f"Path to NPPES monthly zip (default: most recent in {search_help})",
    )
    ap.add_argument("--out-dir", help=f"Output dir (default: {DEFAULT_OUT_DIR.relative_to(REPO_ROOT)}/)")
    ap.add_argument("--skip-endpoints", action="store_true")
    ap.add_argument("--skip-orgs", action="store_true")
    args = ap.parse_args()

    zip_path = Path(args.zip).expanduser() if args.zip else find_latest_monthly()
    if not zip_path.exists():
        sys.exit(f"ERROR: zip not found: {zip_path}")

    out_dir = Path(args.out_dir).expanduser() if args.out_dir else DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # Probe the zip for the release date so output filenames are stable
    # across runs against the same release.
    with zipfile.ZipFile(zip_path) as zf:
        ep_inner = _find_inner_csv(zf, "endpoint_pfile")
        release = release_date_from_inner_name(ep_inner)

    ep_out = out_dir / f"fhir-endpoints-{release}.json"
    org_out = out_dir / f"orgs-{release}.jsonl"

    if not args.skip_endpoints:
        extract_fhir_endpoints(zip_path, ep_out)
    if not args.skip_orgs:
        extract_orgs(zip_path, org_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
