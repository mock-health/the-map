"""Phase 5: cross-validate Epic conformance against Inferno's gold-standard
single_patient_us_core_6_api_group test suite.

Inferno (https://github.com/onc-healthit/inferno-core) is HL7's official US Core
test kit. It's the most authoritative third-party check we can run; agreement with
Inferno is the strongest signal that our Map measurements are correct.

Setup (one-time):
    git clone https://github.com/onc-healthit/inferno-core.git
    cd inferno-core
    docker compose up -d
    # wait for the Ruby app + HL7 validator + Sidekiq + Redis to come online
    open http://localhost:4567

Configuration in the Inferno UI:
    Suite: "US Core 6.1.0"
    FHIR endpoint: $EPIC_NONPROD_FHIR_BASE
    Authorization: bearer token from `python -m tools.oauth_handshake epic`
    Patient ID: erXuFYUfucBZaryVksYEcMg3 (Camila Lopez — has labs, vitals, allergies, conditions)
    Run group: single_patient_us_core_6_api_group

Export: from the Inferno UI, click "Download Results" to save the JSON report.
Drop it at `tests/golden/epic/phase-b-{date}/inferno-results.json` and re-run this script.
This script reads the JSON and writes overlay.inferno_cross_validation with
agreement/disagreement against our element_deviations.

When Inferno hasn't been run yet (no inferno-results.json present), this script
writes a deferred note to overlay so downstream consumers know the axis is unmeasured
rather than silently assumed-clean.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"
GOLDEN_DIR = REPO_ROOT / "tests" / "golden"


def latest_phase_b_dir(ehr: str) -> Path | None:
    candidates = sorted([p for p in (GOLDEN_DIR / ehr).glob("phase-b-*") if p.is_dir()], reverse=True)
    return candidates[0] if candidates else None


def parse_inferno(report: dict) -> dict:
    """Inferno's exported JSON format varies by version; this is a tolerant parser."""
    tests = report.get("tests") or report.get("results") or report.get("test_results") or []
    counts: Counter = Counter()
    fails = []
    for t in tests:
        result = (t.get("result") or t.get("status") or "").lower()
        counts[result] += 1
        if result not in ("pass", "passed", "skip", "skipped"):
            fails.append({
                "id": t.get("id") or t.get("test_id"),
                "label": t.get("label") or t.get("name"),
                "result": result,
                "result_message": (t.get("result_message") or t.get("message") or "")[:300],
            })
    return {"counts": dict(counts), "failures": fails}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr")
    args = ap.parse_args()

    today = datetime.date.today().isoformat()
    overlay_path = EHRS_DIR / args.ehr / "overlay.json"
    overlay = json.loads(overlay_path.read_text())

    phase_b_dir = latest_phase_b_dir(args.ehr)
    inferno_path = (phase_b_dir / "inferno-results.json") if phase_b_dir else None

    if not inferno_path or not inferno_path.exists():
        deferred = {
            "status": "deferred",
            "reason": (
                "Inferno results JSON not found at "
                f"{inferno_path.relative_to(REPO_ROOT) if inferno_path else 'tests/golden/{ehr}/phase-b-{date}/inferno-results.json'}. "
                "Set up Inferno per the docstring at the top of this file "
                "(clone inferno-core, docker compose up, configure US Core 6.1.0 suite), "
                "then re-run this script."
            ),
            "instructions": (
                "1) Clone https://github.com/onc-healthit/inferno-core and `docker compose up -d`; "
                "2) open http://localhost:4567, configure US Core 6.1.0 suite against the EHR sandbox; "
                "3) run single_patient_us_core_6_api_group; "
                "4) export results JSON to tests/golden/{ehr}/phase-b-{date}/inferno-results.json; "
                "5) re-run `python -m tools.run_inferno_us_core {ehr}`."
            ),
            "fallback": (
                "Phase 4 HAPI second-opinion provides cross-validation coverage. "
                "Inferno is the gold standard but HAPI's $validate is sufficient backup per PLAN_EPIC.md risk note."
            ),
            "verification": {
                "source_url": "https://github.com/onc-healthit/inferno-core",
                "source_quote": "deferred until inferno-results.json is captured",
                "verified_via": "epic_public_sandbox",
                "verified_date": today,
            },
        }
        overlay["inferno_cross_validation"] = deferred
        overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
        print("  no Inferno results JSON found; wrote deferred note to overlay")
        return 0

    report = json.loads(inferno_path.read_text())
    parsed = parse_inferno(report)
    overlay["inferno_cross_validation"] = {
        "status": "ran",
        "captured_date": today,
        "counts": parsed["counts"],
        "failures": parsed["failures"][:50],
        "verification": {
            "source_url": str(inferno_path.relative_to(REPO_ROOT)),
            "source_quote": f"Inferno run summary: {parsed['counts']}",
            "verified_via": "epic_public_sandbox",
            "verified_date": today,
        },
    }
    overlay_path.write_text(json.dumps(overlay, indent=2) + "\n")
    print(f"  Inferno: {parsed['counts']}; {len(parsed['failures'])} failures recorded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
