"""Production cross-check — validate the sandbox-derived overlay against ONE real deployment.

The-map's `ehrs/epic/overlay.json` element-deviation rows are entirely
sandbox-derived (`verified_via: epic_public_sandbox`). This tool takes resources
pulled from a *real* deployment — patient-mediated, via a SMART patient-access
launch (see EHR_CONFIG['epic_unc']) — runs them through the SAME four-axis
conformance analyzer the sandbox sweep uses, and diffs the findings against the
sandbox overlay by `row_id`. It answers: does the modal Epic shape behave on the
wire the way the sandbox said it would?

This is a ONE-OFF validation/derisk pass, n=1 patient at n=1 site (UNC Health,
epic-cluster-A — the modal Epic shape). It does NOT write to overlay.json. It
emits:
  * <phi-out>/raw_findings.json      full per-resource findings (PHI-bearing — gitignored)
  * <phi-out>/candidate_rows.json    overlay-shaped rows for human review (PHI-bearing)
  * <report>                          REDACTED markdown diff report (safe to commit)

PHI rule (non-negotiable): raw resources and the PHI-bearing outputs live ONLY
under --phi-out (a gitignored path). The committed report carries structural facts
only — profile_id, path, deviation_category, verdict, counts — never values, names,
dates, identifiers, or note text. A human de-identifies a `source_quote` by hand
before any row is appended to overlay.json with verified_via=production_patient_smart.

Two evidence tiers, kept separate (--source):
  wire — genuine production wire bytes (SMART pull). Eligible to become overlay
         rows (after human de-id) carrying verified_via=production_patient_smart.
  ehi  — ehi-to-fhir reconstruction of an EHI export. The FHIR *shape* is the
         tool's mapping, not UNC's wire — corroboration only. Reported, NEVER
         emitted as candidate overlay rows.

Usage:
    # after pulling your own record to a gitignored dir (see HANDOFF):
    python -m tools.crosscheck_production \
        --source wire \
        --in ~/.the-map-phi/unc/wire \
        --phi-out ~/.the-map-phi/unc/crosscheck \
        --report reports/production-crosscheck-epic-unc-$(date +%F).md

    # ehi-to-fhir corroboration pass (report-only):
    python -m tools.crosscheck_production --source ehi \
        --in ~/.the-map-phi/unc/ehi-to-fhir/out --phi-out ~/.the-map-phi/unc/crosscheck-ehi \
        --report reports/production-crosscheck-epic-unc-ehi-$(date +%F).md
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

from tools.conformance import ValueSetIndex
from tools.measure_phase_b import (
    IG_PACKAGE_DIR,
    US_CORE_BASELINE,
    aggregate_multi_patient,
    evaluate_one,
)
from tools.row_id import ensure_row_id

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OVERLAY = REPO_ROOT / "ehrs" / "epic" / "overlay.json"
UNC_ENDPOINT = "https://epicfe.unch.unc.edu/FHIR/api/FHIR/R4"


def load_resources(in_path: Path) -> list[dict]:
    """Load every FHIR resource under `in_path` (a file or directory of JSON).

    Each JSON may be a Bundle (entries are unwrapped) or a single resource.
    Returns a flat list of resource dicts.
    """
    files = sorted(in_path.rglob("*.json")) if in_path.is_dir() else [in_path]
    if not files:
        sys.exit(f"no .json files found under {in_path}")
    resources: list[dict] = []
    for f in files:
        try:
            doc = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"  WARN skipping {f.name}: {e}", file=sys.stderr)
            continue
        # A file may hold a single resource, a Bundle, a saved-response envelope,
        # or a top-level array of resources (e.g. jmandel/ehi-to-fhir's per-type
        # fhir-target/*.json). Normalize all of these to a list of resource dicts.
        for d in (doc if isinstance(doc, list) else [doc]):
            if not isinstance(d, dict):
                continue
            # Tolerate a saved-response envelope ({_request, _captured_at, body: <FHIR>}),
            # e.g. the-map's golden-fixture shape. Real wire/ehi inputs are bare FHIR.
            if not d.get("resourceType") and isinstance(d.get("body"), dict):
                d = d["body"]
            if d.get("resourceType") == "Bundle":
                for entry in d.get("entry", []) or []:
                    res = entry.get("resource")
                    if isinstance(res, dict) and res.get("resourceType"):
                        resources.append(res)
            elif d.get("resourceType"):
                resources.append(d)
    return resources


def run_analyzer(resources: list[dict], patient_label: str) -> tuple[list[dict], list[dict]]:
    """Run the four-axis analyzer across one merged 'body' for every baseline profile.

    Returns (deduped_rows, coverage) exactly as the Phase B sweep produces them, so
    row shapes match `overlay.json#element_deviations`. ehr='epic' so the analyzer
    resolves Epic profiles/value-sets correctly; verified_via is rewritten downstream.
    """
    body = {"resourceType": "Bundle", "entry": [{"resource": r} for r in resources]}
    baseline = json.loads(US_CORE_BASELINE.read_text())
    vs_index = ValueSetIndex(IG_PACKAGE_DIR)
    today = datetime.date.today().isoformat()

    per_patient: list[dict] = []
    for profile in baseline["profiles"]:
        rows, _diag = evaluate_one(
            body=body,
            profile_id=profile["profile_id"],
            baseline=baseline,
            ehr="epic",
            vs_index=vs_index,
            today=today,
            patient_id=patient_label,
        )
        per_patient.extend(rows)
    return aggregate_multi_patient(per_patient)


def correct_n1_gaps(rows: list[dict]) -> None:
    """n=1 honesty correction (in place).

    aggregate_multi_patient labels an element absent across all swept patients a
    'vendor-implementation-gap'. With a SINGLE patient that inference is unsound —
    a real person legitimately lacks whole resource types. Relabel to a patient-data
    gap so we never assert a vendor gap from n=1. This is the iron rule applied to
    coverage: claim only what one record can support.
    """
    for row in rows:
        mpe = row.get("multi_patient_evidence") or {}
        if mpe.get("category") == "vendor-implementation-gap":
            mpe["category"] = "patient-data-gap-n1"
            row["multi_patient_evidence"] = mpe
            row["deviation"] = (
                (row.get("deviation", "") or "")
                + " [n=1 correction] Single-patient pull: absence cannot be attributed "
                "to the vendor vs. this patient's record. Treated as patient-data gap."
            ).strip()


def diff_against_sandbox(prod_rows: list[dict], overlay: dict) -> dict:
    """Bucket production findings against the existing sandbox overlay.

    Buckets are chosen so the *signal* (reproduced or new deviations) isn't drowned
    by background agreement:
      confirmed_deviations — production reproduced a sandbox DEVIATION (row_id hit, not 'matches')
      confirmed_matches    — production matched spec where the sandbox also matched (row_id hit, 'matches')
      coverage_gaps_n1     — row_id hit a sandbox 'missing' row, but this single record
                             also just lacks the element (mpe category patient-data-gap-n1).
                             n=1 can't attribute that to the vendor, so it is NOT a reproduced
                             deviation — kept separate so it doesn't inflate confirmed_deviations.
      divergent            — same (profile,path), DIFFERENT behavior than the sandbox recorded
      novel_deviations     — a deviation at a (profile,path) the sandbox never recorded
      absent_types         — whole resource types missing from this record (n=1 coverage, not a finding)
      untested             — sandbox rows this single record never exercised
    Benign matches at paths the sandbox doesn't track are counted, not listed (noise).
    """
    sandbox = overlay.get("element_deviations", []) or []
    by_rowid = {d.get("row_id"): d for d in sandbox if d.get("row_id")}
    paths_cats: dict[tuple[str, str], set[str]] = {}
    for d in sandbox:
        paths_cats.setdefault((d.get("profile_id"), d.get("path")), set()).add(d.get("deviation_category"))

    confirmed_deviations, confirmed_matches, divergent, novel_deviations, absent_types = [], [], [], [], []
    coverage_gaps_n1: list[dict] = []
    benign_untracked_matches = 0
    prod_rowids = set()
    for row in prod_rows:
        ensure_row_id(row)
        rid = row["row_id"]
        prod_rowids.add(rid)
        path = row.get("path", "")
        cat = row.get("deviation_category")
        key = (row.get("profile_id"), path)
        is_match = cat == "matches"
        mpe_cat = (row.get("multi_patient_evidence") or {}).get("category")
        # Element paths contain a '.'; a bare token (e.g. "Immunization") is a
        # resource-type presence row from evaluate_one when the type was absent.
        if "." not in path:
            absent_types.append(row)
            continue
        if rid in by_rowid:
            if is_match:
                confirmed_matches.append(row)
            elif mpe_cat == "patient-data-gap-n1":
                # The sandbox flagged this path 'missing' and this single record also
                # lacks it — but a lone patient legitimately lacks elements, so this is
                # a coverage gap, not a reproduced VENDOR deviation. Don't inflate the
                # confirmed-deviations headline with it.
                coverage_gaps_n1.append(row)
            else:
                confirmed_deviations.append(row)
        elif key in paths_cats:
            row["_sandbox_categories"] = sorted(paths_cats[key])
            divergent.append(row)
        elif is_match:
            benign_untracked_matches += 1  # both agree it's fine; sandbox just didn't log it
        else:
            novel_deviations.append(row)

    untested = [d for d in sandbox if d.get("row_id") not in prod_rowids]
    return {
        "confirmed_deviations": confirmed_deviations,
        "confirmed_matches": confirmed_matches,
        "coverage_gaps_n1": coverage_gaps_n1,
        "divergent": divergent,
        "novel_deviations": novel_deviations,
        "absent_types": absent_types,
        "untested": untested,
        "benign_untracked_matches": benign_untracked_matches,
    }


def write_phi_outputs(phi_out: Path, prod_rows: list[dict], buckets: dict, source: str,
                      endpoint: str, verified_via: str | None, cite: str | None) -> None:
    """Full, PHI-bearing artifacts — local only, never committed.

    `verified_via` is the iron-rule evidence tier stamped on candidate rows; `cite`
    is their source_url. For our own SMART pull use production_patient_smart + the
    endpoint; for a third party's published, already-de-identified capture (e.g.
    jmandel/ehi-to-fhir) use community_report + the repo/commit URL.
    """
    phi_out.mkdir(parents=True, exist_ok=True)
    (phi_out / "raw_findings.json").write_text(json.dumps(prod_rows, indent=2) + "\n")

    if source == "wire":
        # Stage overlay-shaped candidate rows with the production verified_via. The
        # source_quote is left as a placeholder for a human to fill with a
        # de-identified structural excerpt before anything lands in overlay.json.
        # confirmed_deviations -> confidence upgrade on an existing row;
        # novel_deviations/divergent -> potential new/corrected rows.
        candidates = []
        intent = (
            [("confidence-upgrade-existing", r) for r in buckets["confirmed_deviations"]]
            + [("new-row", r) for r in buckets["novel_deviations"]]
            + [("review-divergence", r) for r in buckets["divergent"]]
        )
        for why, row in intent:
            r = dict(row)
            r.pop("_sandbox_categories", None)
            r["_crosscheck_intent"] = why
            # community_report data is already de-identified + published, so a real
            # structural source_quote can be filled now; our own SMART pull is PHI, so
            # it stays a TODO until a human de-identifies it.
            placeholder = (
                "(fill structural excerpt from the cited public capture)"
                if verified_via == "community_report"
                else "TODO-DEIDENTIFY: structural excerpt only (codes/systems/cardinalities; no PHI)"
            )
            r["verification"] = {
                "source_url": cite or endpoint,
                "source_quote": placeholder,
                "verified_via": verified_via,
                "verified_date": datetime.date.today().isoformat(),
            }
            candidates.append(r)
        (phi_out / "candidate_rows.json").write_text(json.dumps(candidates, indent=2) + "\n")


def _verdict_table(rows: list[dict], extra_col: str | None = None) -> str:
    if not rows:
        return "_(none)_\n"
    head = "| profile | path | category |" + (f" {extra_col} |" if extra_col else "")
    sep = "|---|---|---|" + ("---|" if extra_col else "")
    lines = [head, sep]
    for r in sorted(rows, key=lambda d: (d.get("profile_id", ""), d.get("path", ""))):
        cat = r.get("multi_patient_evidence", {}).get("category") or r.get("deviation_category", "")
        cells = f"| `{r.get('profile_id','')}` | `{r.get('path','')}` | {cat} |"
        if extra_col:
            cells += f" {', '.join(r.get('_sandbox_categories', []))} |"
        lines.append(cells)
    return "\n".join(lines) + "\n"


def resolve_verified_via(source: str, verified_via: str | None) -> str | None:
    """Validate the (source, verified_via) pair; return the tier candidate rows carry.

    wire  → an overlay-eligible tier (defaults to production_patient_smart).
    ehi   → report-only, emits no overlay rows, so verified_via must NOT be set;
            passing one is a category error (the reconstructed shape can never become
            an overlay row regardless of tier). Returns None.
    """
    if source == "ehi":
        if verified_via is not None:
            sys.exit("--verified-via does not apply to --source ehi "
                     "(report-only: reconstructed shape is never promoted to overlay rows)")
        return None
    return verified_via or "production_patient_smart"


def evidence_descriptor(source: str, verified_via: str | None) -> str:
    """Unambiguous one-line evidence label for the report title — never the bare
    '{source} tier' (which read as 'wire tier' even for a third-party published capture)."""
    if source == "ehi":
        return "ehi-to-fhir reconstruction — corroboration only"
    if verified_via == "community_report":
        return "production wire — third-party published, de-identified capture"
    if verified_via == "customer_evidence":
        return "production wire — customer-provided"
    return "production wire — first-party SMART patient-access pull"


def write_report(report_path: Path, *, source: str, endpoint: str, n_resources: int,
                 resource_types: dict[str, int], buckets: dict, patient_label: str,
                 verified_via: str | None = "production_patient_smart", cite: str | None = None) -> None:
    """REDACTED report — structural facts only. Safe to commit."""
    today = datetime.date.today().isoformat()
    if source == "ehi":
        tier = "ehi-to-fhir RECONSTRUCTION of an EHI export — corroboration only, NOT wire evidence"
    elif verified_via == "community_report":
        tier = "genuine production Epic FHIR (live API), de-identified + published by a third party"
    else:
        tier = "genuine production wire bytes (SMART patient-access pull)"
    type_summary = ", ".join(f"{t}×{n}" for t, n in sorted(resource_types.items()))
    md = f"""# Production cross-check — Epic ({evidence_descriptor(source, verified_via)})

**Date:** {today}
**Source:** {endpoint}
**Iron-rule citation:** {cite or endpoint}
**Evidence tier:** {tier}
**Scope:** n=1 patient (`{patient_label}`), n=1 site. Resources analyzed: {n_resources} ({type_summary}).

This report contains structural facts only (profile / path / deviation category /
verdict). No values, identifiers, dates, names, or note text. The PHI-bearing raw
findings and candidate rows live under the gitignored --phi-out path.

## Method

Resources were run through the-map's four-axis conformance analyzer
(`tools/conformance/`: presence, cardinality, value-set, format) against
`us-core/us-core-6.1-baseline.json` — the same analyzer `tools/measure_phase_b.py`
runs on sandbox golden fixtures — then diffed against the sandbox-derived
`ehrs/epic/overlay.json` by `row_id` (= sha256(profile_id|path|deviation_category)).

## Verdict summary

| bucket | count | meaning |
|---|---|---|
| **confirmed deviations** | {len(buckets['confirmed_deviations'])} | production REPRODUCED a sandbox deviation, excluding n=1 absences (row_id hit, not a coverage gap) → strongest signal; confidence upgrade |
| **confirmed matches** | {len(buckets['confirmed_matches'])} | production matched spec where the sandbox also matched → background agreement |
| **coverage gaps (n=1)** | {len(buckets['coverage_gaps_n1'])} | row_id hit a sandbox 'missing' row, but this single record also just lacks the element → NOT a reproduced vendor deviation (n=1 can't attribute absence to the vendor) |
| **divergent** | {len(buckets['divergent'])} | same path, *different* behavior than the sandbox recorded → sandbox claim may be sandbox-specific |
| **novel deviations** | {len(buckets['novel_deviations'])} | a deviation the sandbox never recorded → potential new row |
| **absent types** | {len(buckets['absent_types'])} | whole resource types not in this record (n=1 coverage, not a finding) |
| **untested** | {len(buckets['untested'])} | sandbox rows this single record never exercised |

_Benign matches at paths the sandbox doesn't track (agreement, not listed): {buckets['benign_untracked_matches']}._

## Confirmed deviations (production reproduced the sandbox finding)

{_verdict_table(buckets['confirmed_deviations'])}
## Divergent (production differs from what the sandbox recorded)

{_verdict_table(buckets['divergent'], extra_col='sandbox_categories')}
## Novel deviations (only seen in production)

{_verdict_table(buckets['novel_deviations'])}
## Absent resource types (n=1 coverage)

{_verdict_table(buckets['absent_types'])}
## Honest limitations

- **n=1 patient, n=1 site.** No generalization beyond this single record on {today}.
  Absent resource types are patient-data gaps, not vendor gaps (see n=1 correction).
- **Patient scopes ≠ system scopes.** A patient-access launch may expose a narrower
  slice than the system-scope sandbox sweep; some untested rows reflect scope, not absence.
{"- **De-identified third-party capture.** Direct-identifier *values* (MRN, etc.) were redacted by the publisher, so format-axis findings on identifier values may be redaction artifacts — presence, coding-system, and cardinality findings are unaffected. The site is an undisclosed Epic org with no CapabilityStatement shipped, so its cluster is unknown; this validates Epic broadly, not a named deployment. Only the confirmed-deviation and divergent buckets are trustworthy without triage." if verified_via == "community_report" else ""}
{"- **Reconstructed shape.** ehi-to-fhir rows reflect the converter's mapping decisions, not the provider's wire. Corroboration only — never promoted to overlay rows." if source == "ehi" else ""}

## Next step

{"Report-only (ehi tier): no overlay rows are produced from reconstructed shape." if source == "ehi" else
 f"Review `<phi-out>/candidate_rows.json`, fill a de-identified `source_quote` for each row worth keeping, then append confirmed/novel rows to `ehrs/epic/overlay.json` with verified_via={verified_via}. Re-run `python -m tools.validate epic`."}
"""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(md)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["wire", "ehi"], required=True,
                    help="wire = SMART production pull (overlay-eligible); ehi = ehi-to-fhir reconstruction (report-only)")
    ap.add_argument("--in", dest="in_path", required=True, type=Path,
                    help="file or directory of pulled FHIR JSON (must be a gitignored PHI path)")
    ap.add_argument("--phi-out", required=True, type=Path,
                    help="gitignored output dir for PHI-bearing artifacts")
    ap.add_argument("--report", required=True, type=Path, help="path for the REDACTED markdown report")
    ap.add_argument("--endpoint", default=UNC_ENDPOINT, help="FHIR base the data came from (for citation)")
    ap.add_argument("--overlay", default=DEFAULT_OVERLAY, type=Path, help="sandbox overlay to diff against")
    ap.add_argument("--patient-label", default="unc-self", help="de-identified label for the single patient")
    ap.add_argument("--verified-via", default=None,
                    choices=["production_patient_smart", "community_report", "customer_evidence"],
                    help="iron-rule evidence tier for candidate rows (wire only; defaults to "
                         "production_patient_smart; community_report for a third party's published "
                         "capture). Must be omitted for --source ehi (report-only).")
    ap.add_argument("--cite", default=None,
                    help="source_url for candidate rows (e.g. repo/commit URL); defaults to --endpoint")
    args = ap.parse_args()
    verified_via = resolve_verified_via(args.source, args.verified_via)

    resources = load_resources(args.in_path)
    resource_types: dict[str, int] = {}
    for r in resources:
        rt = r.get("resourceType", "?")
        resource_types[rt] = resource_types.get(rt, 0) + 1
    print(f"loaded {len(resources)} resources: {resource_types}")

    prod_rows, _coverage = run_analyzer(resources, args.patient_label)
    correct_n1_gaps(prod_rows)

    overlay = json.loads(args.overlay.read_text())
    buckets = diff_against_sandbox(prod_rows, overlay)
    print(f"diff: confirmed_deviations={len(buckets['confirmed_deviations'])} "
          f"confirmed_matches={len(buckets['confirmed_matches'])} "
          f"coverage_gaps_n1={len(buckets['coverage_gaps_n1'])} divergent={len(buckets['divergent'])} "
          f"novel_deviations={len(buckets['novel_deviations'])} absent_types={len(buckets['absent_types'])} "
          f"untested={len(buckets['untested'])}")

    write_phi_outputs(args.phi_out, prod_rows, buckets, args.source, args.endpoint,
                      verified_via, args.cite)
    write_report(args.report, source=args.source, endpoint=args.endpoint,
                 n_resources=len(resources), resource_types=resource_types,
                 buckets=buckets, patient_label=args.patient_label,
                 verified_via=verified_via, cite=args.cite)
    print(f"wrote PHI artifacts -> {args.phi_out}")
    print(f"wrote redacted report -> {args.report}")
    if args.source == "wire":
        print("next: de-identify candidate_rows.json by hand, then append to ehrs/epic/overlay.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
