"""One-shot backfill: ensure every existing element_deviations[] row carries a
stable `row_id` derived from `(profile_id, path, deviation_category)`.

Run once after introducing the row_id schema requirement; idempotent thereafter.
The forward pipeline (tools.measure_phase_b) calls `ensure_row_id` on every
deviation it writes, so new rows always have row_id at creation time.

Usage:
    python -m tools.backfill_row_ids                   # all EHRs
    python -m tools.backfill_row_ids --ehr epic        # one EHR
    python -m tools.backfill_row_ids --dry-run         # report only
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.row_id import ensure_row_id

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"


def backfill_overlay(overlay_path: Path, *, dry_run: bool) -> tuple[int, int]:
    overlay = json.loads(overlay_path.read_text())
    deviations = overlay.get("element_deviations") or []
    added = 0
    skipped = 0
    for dev in deviations:
        if "row_id" in dev:
            skipped += 1
            continue
        ensure_row_id(dev)
        added += 1
    if added and not dry_run:
        overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    return added, skipped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ehr", help="restrict to one EHR (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="report only, no writes")
    args = ap.parse_args()

    targets = (
        [EHRS_DIR / args.ehr / "overlay.json"]
        if args.ehr
        else sorted(EHRS_DIR.glob("*/overlay.json"))
    )

    total_added = 0
    total_skipped = 0
    for path in targets:
        if not path.exists():
            print(f"  {path.relative_to(REPO_ROOT)}: missing — skipped", file=sys.stderr)
            continue
        added, skipped = backfill_overlay(path, dry_run=args.dry_run)
        total_added += added
        total_skipped += skipped
        verb = "would add" if args.dry_run else "added"
        print(f"  {path.relative_to(REPO_ROOT)}: {verb} {added} row_ids ({skipped} already had one)")

    print(f"\nTotal: {total_added} row_ids {'would be added' if args.dry_run else 'added'}, {total_skipped} preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
