"""Verify that re-running the CMS pipeline produces sensible deltas.

After ``make fetch-all-cms && make rebuild-overlays``, the committed overlay
files at ``data/hospital-overlays/{vendor}-{npd,nppes}.json`` and
``data/hospital-overrides/{vendor}-pos.json`` will have changed because the
upstream CMS data moved on. This script compares the *committed* version
of each overlay (read via ``git show HEAD:``) against the *current* on-disk
version produced by the rebuild, and reports the drift.

Outputs a Markdown report under ``data/verification/overlay-refresh-{date}.md``.

Exit codes:
  0 — drift is within expected bounds (or only data changes; no schema breakage)
  1 — structural breakage (top-level keys missing/added) or git/IO failure

This is the proof step for the "re-run + verify" path in the reproducibility
refactor: if the fetcher + builder + resolver chain still produces JSON of
the same shape with reasonable totals, we trust the refresh and commit the
new overlays as the new ground truth.

Usage:
    python -m tools.verify_overlay_refresh
    python -m tools.verify_overlay_refresh --ref main           # diff vs main
    python -m tools.verify_overlay_refresh --tolerance 0.30     # 30% row-count delta OK
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
VERIFICATION_DIR = REPO_ROOT / "data" / "verification"

# Overlay files whose source data flows from the CMS pipeline. Vendor list
# is discovered at import time from the ehrs/ directory tree so the
# verification stays current as new vendors land.
def _discovered_vendors() -> tuple[str, ...]:
    ehrs_dir = REPO_ROOT / "ehrs"
    if not ehrs_dir.is_dir():
        return ()
    return tuple(sorted(p.name for p in ehrs_dir.iterdir() if p.is_dir()))


_VENDORS = _discovered_vendors()
OVERLAY_PATHS: tuple[tuple[Path, str], ...] = tuple(
    (REPO_ROOT / "data" / "hospital-overlays" / f"{vendor}-{src}.json", "matches")
    for vendor in _VENDORS
    for src in ("npd", "nppes")
) + tuple(
    (REPO_ROOT / "data" / "hospital-overrides" / f"{vendor}-pos.json", "endpoints")
    for vendor in _VENDORS
)


@dataclass
class OverlayDiff:
    path: Path
    status: str  # "ok", "new", "missing-committed", "missing-current", "schema-break", "io-error"
    old_keys: set[str] = field(default_factory=set)
    new_keys: set[str] = field(default_factory=set)
    old_count: int | None = None
    new_count: int | None = None
    detail: str = ""

    @property
    def count_delta(self) -> int | None:
        if self.old_count is None or self.new_count is None:
            return None
        return self.new_count - self.old_count

    @property
    def count_pct(self) -> float | None:
        if not self.old_count:
            return None
        delta = self.count_delta
        return None if delta is None else delta / self.old_count


def git_show_committed(path: Path, ref: str = "HEAD") -> dict | None:
    """Return parsed JSON at ``ref:path`` or None if the path doesn't exist there."""
    rel = path.relative_to(REPO_ROOT)
    try:
        result = subprocess.run(
            ["git", "show", f"{ref}:{rel}"],
            check=True,
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
    except subprocess.CalledProcessError:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def _count_rows(record: dict, count_key: str) -> int | None:
    val = record.get(count_key)
    if isinstance(val, list):
        return len(val)
    # Some overlays put counts under summary.{count_key} or similar.
    summary = record.get("summary")
    if isinstance(summary, dict):
        for k in ("count", "total", "row_count", "matches"):
            v = summary.get(k)
            if isinstance(v, int):
                return v
    return None


def diff_overlay(path: Path, count_key: str, *, ref: str, tolerance: float) -> OverlayDiff:
    if not path.is_file():
        old = git_show_committed(path, ref=ref)
        if old is None:
            return OverlayDiff(path, "missing-both", detail="not committed and not on disk")
        return OverlayDiff(path, "missing-current", old_keys=set(old.keys()), detail="on-disk file is missing")

    try:
        new = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        return OverlayDiff(path, "io-error", detail=f"current file unreadable: {e}")

    old = git_show_committed(path, ref=ref)
    if old is None:
        return OverlayDiff(
            path, "new",
            new_keys=set(new.keys()),
            new_count=_count_rows(new, count_key),
            detail="not present in committed tree (new file)",
        )

    old_keys = set(old.keys())
    new_keys = set(new.keys())
    old_count = _count_rows(old, count_key)
    new_count = _count_rows(new, count_key)

    if old_keys != new_keys:
        added = new_keys - old_keys
        removed = old_keys - new_keys
        return OverlayDiff(
            path, "schema-break",
            old_keys=old_keys, new_keys=new_keys,
            old_count=old_count, new_count=new_count,
            detail=f"keys changed: +{sorted(added) or '∅'} / -{sorted(removed) or '∅'}",
        )

    diff = OverlayDiff(
        path, "ok",
        old_keys=old_keys, new_keys=new_keys,
        old_count=old_count, new_count=new_count,
    )

    if old_count is not None and new_count is not None and old_count > 0:
        pct = (new_count - old_count) / old_count
        if abs(pct) > tolerance:
            diff.status = "drift-outside-tolerance"
            diff.detail = (
                f"row count Δ={new_count - old_count:+d} ({pct:+.1%}) "
                f"exceeds tolerance ±{tolerance:.0%}"
            )
    return diff


def render_report(diffs: list[OverlayDiff], *, ref: str, tolerance: float) -> str:
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# Overlay Refresh Verification",
        "",
        f"- Generated: `{now}`",
        f"- Compared against: `{ref}` (`git show {ref}:<path>`)",
        f"- Row-count tolerance: ±{tolerance:.0%}",
        "",
        "## Summary",
        "",
        "| Status | Count |",
        "|---|---|",
    ]
    by_status: dict[str, int] = {}
    for d in diffs:
        by_status[d.status] = by_status.get(d.status, 0) + 1
    for status, count in sorted(by_status.items()):
        lines.append(f"| `{status}` | {count} |")

    lines.extend([
        "",
        "## Detail",
        "",
        "| Path | Status | Old | New | Δ | Note |",
        "|---|---|---:|---:|---:|---|",
    ])
    for d in sorted(diffs, key=lambda x: (x.status != "ok", str(x.path))):
        rel = d.path.relative_to(REPO_ROOT)
        oc = "—" if d.old_count is None else str(d.old_count)
        nc = "—" if d.new_count is None else str(d.new_count)
        if d.count_delta is None:
            delta = "—"
        else:
            pct = d.count_pct
            delta = f"{d.count_delta:+d}" + (f" ({pct:+.1%})" if pct is not None else "")
        note = d.detail.replace("|", "\\|")
        lines.append(f"| `{rel}` | `{d.status}` | {oc} | {nc} | {delta} | {note} |")

    breakage = [d for d in diffs if d.status in {"schema-break", "io-error", "missing-current"}]
    if breakage:
        lines.extend([
            "",
            "## Action required",
            "",
            "The following overlays have structural problems that should be resolved before "
            "committing the refresh:",
            "",
        ])
        for d in breakage:
            lines.append(f"- `{d.path.relative_to(REPO_ROOT)}` — {d.detail}")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ref", default="HEAD", help="Git ref to compare against (default: HEAD)")
    ap.add_argument(
        "--tolerance",
        type=float,
        default=0.25,
        help="Row-count delta beyond which a diff is flagged as out-of-tolerance (default: 0.25 = ±25%%)",
    )
    ap.add_argument("--out", help="Path to write the Markdown report (default: data/verification/overlay-refresh-{date}.md)")
    args = ap.parse_args()

    diffs = [diff_overlay(path, count_key, ref=args.ref, tolerance=args.tolerance) for path, count_key in OVERLAY_PATHS]
    report = render_report(diffs, ref=args.ref, tolerance=args.tolerance)

    if args.out:
        out_path = Path(args.out).expanduser()
    else:
        VERIFICATION_DIR.mkdir(parents=True, exist_ok=True)
        date = datetime.now(UTC).strftime("%Y-%m-%d")
        out_path = VERIFICATION_DIR / f"overlay-refresh-{date}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report)

    print(report)
    print(f"\nReport written to {out_path.relative_to(REPO_ROOT)}")

    # Exit non-zero only on structural breakage. Data drift (counts changing
    # within tolerance) is the expected outcome of fetching newer upstream
    # data and is informational, not gating.
    breakage = [d for d in diffs if d.status in {"schema-break", "io-error", "missing-current"}]
    if breakage:
        print(f"\nFAIL: {len(breakage)} overlay(s) have structural problems. See report.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
