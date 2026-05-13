"""Build a compact NPI → (PAC ID, provider type) index from CMS PECOS PPEF.

The Medicare Public Provider Enrollment File (PPEF) is published quarterly at
https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment.
Each enrollment record carries:
  - NPI (10-digit)
  - PECOS_ASCT_CNTL_ID (PAC ID — Medicare's stable cross-NPI provider key)
  - PROVIDER_TYPE_CD / PROVIDER_TYPE_DESC
  - STATE_CD
  - ORG_NAME (for Type 2 orgs) or LAST/FIRST_NAME (for Type 1 individuals)

This tool reads `PPEF_Enrollment_Extract_*.csv`, aggregates per NPI (an NPI
may have multiple enrollment rows across states/programs), and writes a
compact lookup at `data/cms-pecos/enrollment-{captured_date}.json`. The
output is small (~1M NPIs × a few short fields ≈ 50-80 MB JSON), suitable
to load into memory for the overlay layer in analyze_fleet_drift.

Why we want PAC ID: it's STABLE across NPI changes. An org that's enrolled
in Medicare keeps the same PAC ID even if it gets a new NPI after a merger
or ownership change. So `pac_id` is the most reliable longitudinal join
key — useful for cross-system data engineering when the underlying entity
is a Medicare-enrolled provider.

Usage:
    python -m tools.build_pecos_index
    python -m tools.build_pecos_index --input ~/back/data/ppef/2025-Q3/PPEF_Enrollment_Extract_2025.10.01.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "data" / "cms-pecos"

DEFAULT_PPEF_DIR = Path.home() / "back" / "data" / "ppef"
PPEF_DATE_RE = re.compile(r"PPEF_Enrollment_Extract_(\d{4})\.(\d{2})\.(\d{2})\.csv$")


def discover_latest_enrollment(d: Path) -> Path:
    """Find the most recent PPEF_Enrollment_Extract_*.csv under d/<quarter>/."""
    candidates: list[tuple[str, Path]] = []
    if d.is_dir():
        for quarter_dir in d.iterdir():
            if not quarter_dir.is_dir():
                continue
            for f in quarter_dir.iterdir():
                m = PPEF_DATE_RE.search(f.name)
                if m:
                    candidates.append((f"{m.group(1)}-{m.group(2)}-{m.group(3)}", f))
    if not candidates:
        sys.exit(
            f"ERROR: no PPEF_Enrollment_Extract_*.csv under {d}/. "
            "Pass --input <path>."
        )
    candidates.sort(key=lambda kv: kv[0])
    return candidates[-1][1]


def captured_date(p: Path) -> str:
    m = PPEF_DATE_RE.search(p.name)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else "unknown"


def build_index(csv_path: Path) -> dict[str, dict]:
    """One pass over PPEF enrollment; aggregate per NPI to a compact record.

    PPEF rows are denormalized — one row per (NPI, enrollment, program). For
    each NPI we keep the first non-empty PAC ID and the first non-empty
    provider-type description we see. Multiple-PAC cases (rare; happens after
    ownership change) drop secondary PAC IDs — they're chasing a longer-form
    longitudinal use case than `the-map` needs today.

    Output schema per NPI: `{pac, type_desc, type_cd, is_org}`.
    """
    per_npi: dict[str, dict] = {}
    rows = 0
    t0 = time.monotonic()
    with csv_path.open(encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            npi = (row.get("NPI") or "").strip()
            if not npi:
                continue
            slot = per_npi.get(npi)
            if slot is None:
                slot = {"pac": None, "type_desc": None, "type_cd": None, "is_org": False}
                per_npi[npi] = slot
            if not slot["pac"]:
                pac = (row.get("PECOS_ASCT_CNTL_ID") or "").strip()
                if pac:
                    slot["pac"] = pac
            if not slot["type_desc"]:
                td = (row.get("PROVIDER_TYPE_DESC") or "").strip()
                if td:
                    slot["type_desc"] = td
                    slot["type_cd"] = (row.get("PROVIDER_TYPE_CD") or "").strip() or None
            if not slot["is_org"] and (row.get("ORG_NAME") or "").strip():
                slot["is_org"] = True
            if rows % 500000 == 0:
                print(f"  scanned {rows:,} rows, {len(per_npi):,} NPIs ({int(time.monotonic() - t0)}s)")
    print(f"  done: {rows:,} rows, {len(per_npi):,} distinct NPIs ({int(time.monotonic() - t0)}s)")
    return per_npi


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", help="Path to PPEF_Enrollment_Extract_*.csv")
    ap.add_argument("--out", help="Output JSON path (default: data/cms-pecos/enrollment-{captured_date}.json)")
    args = ap.parse_args()

    csv_path = Path(args.input).expanduser() if args.input else discover_latest_enrollment(DEFAULT_PPEF_DIR)
    if not csv_path.exists():
        sys.exit(f"ERROR: PPEF CSV not found: {csv_path}")

    date = captured_date(csv_path)
    print(f"PECOS enrollment input: {csv_path.name} (release {date})")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else OUTPUT_DIR / f"enrollment-{date}.json"

    per_npi = build_index(csv_path)

    # Compose payload: stable across builds — sort NPIs for diff readability.
    payload = {
        "source": "cms_ppef_enrollment_extract",
        "source_filename": csv_path.name,
        "captured_date": date,
        "npi_count": len(per_npi),
        "npis": dict(sorted(per_npi.items())),
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {out_path.relative_to(REPO_ROOT)} ({out_path.stat().st_size // 1024 // 1024} MiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
