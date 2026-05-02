"""One-shot pre-launch fleet refresh + freshness audit.

Per vendor (epic, cerner, meditech), this script:
  1. Re-fetches the brands bundle (tools.fetch_brands)
  2. Re-harvests CapStmts + smart-configs from every endpoint (tools.harvest_production_capstmts)
  3. Re-runs the cluster/drift analysis (tools.analyze_fleet_drift) to write
     a fresh ehrs/{vendor}/production_fleet.json
  4. Re-resolves endpoints to POS hospitals (tools.resolve_endpoints_to_pos) so
     data/hospital-overrides/ stays in sync with the new fleet

Outputs an acceptance line per vendor:
    OK  ehr=epic  captured_date=2026-05-01  age_days=0  delta_endpoints=+0  (481 → 481)

Acceptance criterion:
    All vendors have age_days <= --max-age-days (default 14) AND
    |delta_endpoints / previous_total| <= --max-delta-pct (default 2%).

Wall time: ~25 minutes (Cerner alone is ~16 min — 2,810 endpoints).

Usage:
    python -m tools.prelaunch_fleet_audit                  # dry-run (prints plan, no execution)
    python -m tools.prelaunch_fleet_audit --execute        # actually run
    python -m tools.prelaunch_fleet_audit --execute epic   # one vendor only
    python -m tools.prelaunch_fleet_audit --execute --skip-resolve   # skip POS step
"""
from __future__ import annotations

import argparse
import datetime
import json
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
DEFAULT_VENDORS = ("epic", "cerner", "meditech")


def _previous_fleet_state(vendor: str) -> dict:
    """Snapshot the committed production_fleet.json so we can compute deltas after."""
    p = EHRS_DIR / vendor / "production_fleet.json"
    if not p.exists():
        return {"endpoints_attempted": None, "captured_date": None}
    f = json.loads(p.read_text())
    return {
        "endpoints_attempted": f.get("harvest_summary", {}).get("endpoints_attempted"),
        "captured_date": f.get("captured_date"),
    }


def _run(label: str, cmd: list[str], *, dry_run: bool) -> int:
    """Run a subprocess, streaming output. Returns exit code (0 on dry-run)."""
    sys.stderr.write(f"\n  >> {label}\n     {' '.join(cmd)}\n")
    if dry_run:
        return 0
    t0 = time.time()
    rc = subprocess.call(cmd, cwd=REPO_ROOT)
    elapsed = time.time() - t0
    sys.stderr.write(f"     ({elapsed:.0f}s, rc={rc})\n")
    return rc


def audit_vendor(vendor: str, *, dry_run: bool, skip_resolve: bool) -> dict:
    sys.stderr.write(f"\n=== {vendor} ===\n")
    before = _previous_fleet_state(vendor)
    sys.stderr.write(
        f"  before: captured_date={before['captured_date']} "
        f"endpoints_attempted={before['endpoints_attempted']}\n"
    )

    rc1 = _run(f"fetch_brands {vendor}",
               [sys.executable, "-m", "tools.fetch_brands", vendor], dry_run=dry_run)
    rc2 = _run(f"harvest_production_capstmts {vendor}",
               [sys.executable, "-m", "tools.harvest_production_capstmts", vendor], dry_run=dry_run)
    rc3 = _run(f"analyze_fleet_drift {vendor}",
               [sys.executable, "-m", "tools.analyze_fleet_drift", vendor], dry_run=dry_run)
    rc4 = 0
    if not skip_resolve:
        rc4 = _run(f"resolve_endpoints_to_pos {vendor}",
                   [sys.executable, "-m", "tools.resolve_endpoints_to_pos", vendor], dry_run=dry_run)

    after = _previous_fleet_state(vendor)
    rcs = {"fetch_brands": rc1, "harvest": rc2, "analyze": rc3, "resolve": rc4}

    today = datetime.date.today()
    age_days = None
    if after["captured_date"]:
        try:
            age_days = (today - datetime.date.fromisoformat(after["captured_date"])).days
        except ValueError:
            age_days = None
    delta = None
    if before["endpoints_attempted"] is not None and after["endpoints_attempted"] is not None:
        delta = after["endpoints_attempted"] - before["endpoints_attempted"]

    return {
        "vendor": vendor,
        "before": before,
        "after": after,
        "age_days": age_days,
        "delta_endpoints": delta,
        "exit_codes": rcs,
        "any_failed": any(rc != 0 for rc in rcs.values()),
    }


def _format_acceptance(r: dict, *, max_age: int, max_delta_pct: float) -> tuple[str, bool]:
    vendor = r["vendor"]
    age = r["age_days"]
    delta = r["delta_endpoints"]
    before_total = r["before"]["endpoints_attempted"]
    after_total = r["after"]["endpoints_attempted"]

    parts = [f"ehr={vendor}", f"captured_date={r['after']['captured_date']}"]
    parts.append(f"age_days={age if age is not None else '?'}")
    if delta is not None:
        parts.append(f"delta_endpoints={'+' if delta >= 0 else ''}{delta}")
    if before_total is not None and after_total is not None:
        parts.append(f"({before_total} → {after_total})")

    failures: list[str] = []
    if r["any_failed"]:
        failures.append(f"subprocess failed (exit codes: {r['exit_codes']})")
    if age is None:
        failures.append("age_days unknown")
    elif age > max_age:
        failures.append(f"age_days={age} > {max_age}")
    if delta is not None and before_total:
        pct = abs(delta) / before_total
        if pct > max_delta_pct:
            failures.append(f"delta {pct:.1%} > {max_delta_pct:.1%}")

    label = "OK  " if not failures else "FAIL"
    line = f"{label}  " + "  ".join(parts)
    if failures:
        line += "  -- " + "; ".join(failures)
    return line, not failures


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("vendors", nargs="*", default=list(DEFAULT_VENDORS))
    ap.add_argument("--execute", action="store_true",
                    help="Actually run the harvest. Without this, prints the plan only.")
    ap.add_argument("--skip-resolve", action="store_true",
                    help="Skip the resolve_endpoints_to_pos step.")
    ap.add_argument("--max-age-days", type=int, default=14)
    ap.add_argument("--max-delta-pct", type=float, default=0.02)
    args = ap.parse_args()

    if not args.execute:
        sys.stderr.write(
            "\n--- DRY RUN ---\n"
            "This script will refresh the production fleet for: "
            f"{', '.join(args.vendors)}\n"
            "Wall-clock: ~25 minutes for all three (Cerner alone is ~16 min).\n"
            "Pass --execute to actually run.\n"
        )

    results = [audit_vendor(v, dry_run=not args.execute, skip_resolve=args.skip_resolve) for v in args.vendors]

    if not args.execute:
        sys.stderr.write("\n(dry-run complete; no changes made)\n")
        return 0

    sys.stderr.write("\n--- ACCEPTANCE ---\n")
    all_ok = True
    for r in results:
        line, ok = _format_acceptance(r, max_age=args.max_age_days, max_delta_pct=args.max_delta_pct)
        print(line)
        all_ok = all_ok and ok

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
