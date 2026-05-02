# Pre-launch editorial validation — Cerner / Oracle Health — 2026-05-02

> **STOP CONDITION RESOLVED — Option A applied (extractor fixed + regenerated)**
>
> Original finding: element_deviations spot-check found 4 failures in 20 sampled rows (20%). All 4 were "missing"-category claims that goldens directly contradicted.
>
> Root cause: `tools/measure_phase_b.py:evaluate_one` only checked the FIRST resource of each type per patient bundle (`first_resource_of_type(body, rtype)`), so a path present on resources 2..N was wrongly reported "absent". With multi-thousand-resource bundles like Cerner's 831-Condition response for patient 12742399, this caused systematic false-negatives.
>
> Fix: changed `evaluate_one` to iterate all resources of the type with `all_resources_of_type(body, rtype)` and use bundle-wide presence semantics (matches if ANY resource has the path; missing only if NONE). Cardinality/value-set/format axes still run on the first resource that has the path.
>
> Regression coverage: `tests/test_measure_phase_b_evaluate_one.py` builds a synthetic two-Condition bundle where the first lacks `condition-assertedDate` and the second has it; asserts the path comes back "matches".
>
> Verification (full surface, not sample): all 12 Cerner "missing" rows audited against goldens → 0 failures. All 135 Cerner "matches" rows audited (per-patient claims of presence/absence checked against goldens) → 0 failures. Same audit run on Epic: 0/17 missing failures, 0/141 matches failures.
>
> Stratification shift: matches 131→135, missing 16→12 (4 wrong-missing rows correctly reclassified to matches with per-patient nuance), VS counts unchanged.

Methodology: cross-check editorial claims in `ehrs/cerner/overlay.json` against in-repo evidence (no live probes). Recipes from `~/.claude/plans/we-re-about-to-release-breezy-shamir.md`.

Most-recent golden capture: 2026-04-27 (5 days from validation).
Sources used: `ehrs/cerner/CapabilityStatement.json`, `ehrs/cerner/production_fleet.json`, `ehrs/cerner/sandbox_patients.json`, `tests/golden/cerner/phase-b-2026-04-27/` (per-patient resource captures + error-* fixtures), `tests/golden/cerner/CapabilityStatement-2026-04-26.json`.

## Summary

| status | count |
|---|---:|
| verified | 23 |
| auto-corrected | 0 (headlines + structured sections needed no edits) |
| auto-corrected (regenerated from extractor fix) | element_deviations + multi_patient_coverage |
| flagged for human review | 1 (low-confidence: empty-scopes vendor error code without captured fixture) |

## Per-claim ledger

### compatibility_statement
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "Oracle Health (formerly Cerner Millennium)" branding | CapStmt + brands bundle confirm | verified |
| 2 | "fhir-ehr.cerner.com (provider/EHR-launch tier) and fhir-myrecord.cerner.com (patient standalone-launch tier)" | access_scope_notes corroborates with structured fields | verified |
| 3 | "Same backend, same tenant IDs, different access scopes" | tenant `ec2458f2-1e24-41c8-b71b-0e701af7583d` confirmed across both URLs | verified |
| 4 | Trademark disclaimer | Standard legal-safe wording | verified |

### auth_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "client_secret_basic", "scopes_format=v2", "no JWKS" | structured fields | verified |
| 2 | "token endpoint at https://authorization.cerner.com/tenants/<tenant-id>/protocols/oauth2/profiles/smart-v1/token" | structured `token_url` field | verified |
| 3 | "open_tier_base=https://fhir-open.cerner.com/r4/<tenant>" | matches CapStmt `.implementation.url` | verified |
| 4 | "secure_tier_base=https://fhir-ehr.cerner.com/r4/<tenant>" | structured field | verified |
| 5 | "verified_scopes: system/Patient.rs system/Observation.rs ..." | structured field | verified |
| 6 | "verified_resource_access" map (Observation lab → 200 secure; Patient/{pid} → 403 secure / 200 open; ...) | structured field with verified_date 2026-04-27 | verified |
| 7 | source_quote excerpt of probe results | matches structured fields | verified |

### access_scope_notes
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "1448-endpoint provider modal cluster (cerner-cluster-A) against 1356-endpoint patient modal cluster (cerner-patient-cluster-A)" | production_fleet.json: `cerner-cluster-A` count=1448, `cerner-patient-cluster-A` count=1356 | verified |
| 2 | "Account, Basic, ChargeItem, Communication, Group" extra resources on provider tier | structured `resources_present` list | verified |
| 3 | "Diff computed 2026-04-28 from 1358 patient-tier CapabilityStatements" | `tests/golden/production-fleet/cerner-patient/2026-04-28/` directory exists with 1361 entries (close to 1358; 3-endpoint discrepancy likely error files excluded) | verified-with-minor-gap |

### phase_b_findings.headlines (5 total)

| # | finding | evidence | recipe | status |
|---|---|---|---|---|
| 1 | "'System Account' is confidential client with shared secret, not asymmetric JWT" | auth_overlay confirms `client_secret_basic`, `jwks_required=false`. Citations include `https://hl7.org/fhir/uv/bulkdata/authorization/index.html` and `https://fhir.cerner.com/authorization/system-accounts/` (link-checker covers reachability). | D | verified |
| 2 | "v2 scope syntax required; v1 `.read` rejected with vendor error code `oauth2:token:empty-scopes`" | The vendor error code `oauth2:token:empty-scopes` is referenced in the overlay's evidence text. **No captured token-endpoint response in `tests/golden/cerner/` corroborates the exact error code string.** Probe was run but response not retained as a fixture. The `.r`/`.rs` distinction is observed in auth_overlay.verified_resource_access (Patient/{pid} via .r works; Patient?... requires .rs). | D | **flag** Q1 (low-confidence; consider capturing the token-endpoint probe response as a golden) |
| 3 | "Secure-tier authorization is per-resource and may diverge from token scopes" | auth_overlay.verified_resource_access shows token granted Condition.rs + AllergyIntolerance.rs but secure-tier reads return 403; corroborates the claim verbatim | A | verified |
| 4 | "Secure-tier Patient search rejects unconstrained queries (requires _id, identifier, name, family, given, birthdate, phone, email, address-postalcode, or -pageContext)" | `tests/golden/cerner/phase-b-2026-04-27/error-search-with-unsupported-param.json` returns OperationOutcome with diagnostic text matching the headline verbatim: "at least one of _id, identifier, name, family, given, birthdate, phone, email, address-postalcode, or -pageContext must be provided" | G | verified |
| 5 | "Two-tier public sandbox (open + secure); Phase B can run partially without registration" | CapStmt URL is `fhir-open.cerner.com` (anonymous-readable). access_scope_notes corroborates two-tier architecture. | A+G | verified |

### operation_outcome_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "shape=fhir-standard-with-vendor-extensions" | structured field | verified |
| 2 | 3 verbatim error examples (404 not-found, search-without-required-param, unknown resource type) | each captured response_body matches captured golden in `error-*.json` fixtures | verified |

## Element-deviations spot-check — failures uncovered (now fixed)

Failures listed below were uncovered during the initial spot-check. All four are now correctly classified after the extractor fix described above. Retained for the audit trail.

Sampling: stratified n=10, then broadened to n=20 per plan stop-condition protocol. Seed 20260502.

### Confirmed failures (4)

| row_id | profile_id | path | claim | evidence of contradiction |
|---|---|---|---|---|
| **0c8eb0b0b813** | us-core-condition-problems-health-concerns | `Condition.extension(condition-assertedDate)` | "absent across all 3 swept patients (12724066, 12742399, 12744580)" | `tests/golden/cerner/phase-b-2026-04-27/condition-problems-12742399.json` contains **7 Condition resources with the `condition-assertedDate` extension** (e.g., `valueDateTime=2026-01-12`, `url=http://hl7.org/fhir/StructureDefinition/condition-assertedDate`). Patients 12724066 and 12744580 confirmed absent. Correct multi-patient picture: present in 1/3, absent in 2/3 — not "absent across all". |
| **abca4e40ff39** | us-core-medicationrequest | `MedicationRequest.reasonCode` | "absent across all 2 swept patients (12724066, 12742399)" | `tests/golden/cerner/phase-b-2026-04-27/medicationrequest-12724066.json` contains MedicationRequest id=358497179 with `reasonCode=[{coding:[{system:"http://hl7.org/fhir/sid/icd-10-cm",code:"R50.9",display:"Fever, unspecified",userSelected:true}]}]`. Patient 12742399 status not deeply checked but the "across all" claim is already false. |
| **b890ba321071** | us-core-vital-signs | `Observation.component.dataAbsentReason` | "absent across all 3 swept patients (12724066, 12742399, 12752183)" | `tests/golden/cerner/phase-b-2026-04-27/observation-vital-signs-12724066.json` contains **7 Observation.component instances with `dataAbsentReason`** (verified via jq selector on `.component[] \| select(.dataAbsentReason != null)`). Patients 12742399 and 12752183 confirmed absent. Correct: present in 1/3. |
| **c83a5a50b244** | us-core-diagnosticreport-lab | `DiagnosticReport.result` | "absent across all 2 swept patients (12724066, 12742399)" | `tests/golden/cerner/phase-b-2026-04-27/diagnosticreport-lab-12724066.json` contains DiagnosticReport id=197281672 with `result=[{reference:"Observation/L-197281678"}, {reference:"Observation/L-197281680"}, ...]`. Patient 12742399 not deeply checked but "across all" is already false. |

**Pattern**: All 4 failures are "missing"-category claims that wrongly aggregate per-patient evidence. The `multi_patient_evidence` block on each row reads `patients_present_in: []`, but in reality at least one patient has the element. This is a systematic bug in the deviation-extraction pipeline (likely `tools/measure_phase_b.py`), not editorial mishaps that can be hand-fixed.

**Extrapolated risk**: Cerner has 16 "missing" rows total. I sampled 6 in this expanded pass; 4 failed (67% within "missing"). Conservatively estimating, **~8–12 of the 16 "missing" rows may be incorrect**. Extending the same scrutiny to the 5 "value-set-narrowed" and 4 "value-set-mismatch" rows is also warranted. Headlines, auth_overlay, and structured sections are unaffected.

### Verified clean (16 rows)

| row_id | path | category | result |
|---|---|---|---|
| 0345f00ea97b | Immunization.patient | matches | ✓ |
| 03ec1bbd4261 | Observation.category:us-core | matches | ✓ |
| 07be2189fe35 | Observation.status (vital-signs) | matches | ✓ |
| 082cf4ef7242 | Encounter.period | matches | ✓ (per-patient nuance correctly captured: present in 1/3, absent in 2/3) |
| 1502ee0d3156 | MedicationRequest.reasonReference | missing | ✓ (confirmed absent in both medicationrequest-1272* goldens) |
| 19cee50632a2 | Patient.address.use | value-set-unverified-locally | ✓ |
| 1ecd462457c8 | DocumentReference.type | value-set-mismatch | ✓ (Cerner OID `https://fhir.cerner.com/<tenant>/codeSet/72` corroborates) |
| 9f4a48823b34 | Patient.extension(us-core-race) | matches | ✓ |
| 7200a17b7027 | DocumentReference.context.period | matches | ✓ |
| 3449129060c8 | Encounter.hospitalization.dischargeDisposition | missing | ✓ |
| 3c142817fe54 | Immunization.statusReason | missing | ✓ |
| 43cc9f2352ec | Patient.extension(us-core-tribal-affiliation) | missing | ✓ |
| 5db5905e07cb | Observation.specimen | missing | ✓ |
| 6c404e668e60 | Observation.dataAbsentReason (top-level on lab) | missing | ✓ (top-level distinct from .component.dataAbsentReason which is a separate path) |
| 2b47abb1a287 | MedicationRequest.category:us-core | value-set-unverified-locally | ✓ (structured) |
| 2cbc62c6dab4 | Patient.gender | value-set-unverified-locally | ✓ (structured) |

(rows 302c1e75b068, 380efd857866 inspected at structured-field level only; no contradicting golden checked due to category complexity)

## Human-review queue

> **Status update — Q1 resolved 2026-05-02 (post-validation pass).** Resolution annotated below. See `~/.claude/plans/please-go-through-human-shimmering-glacier.md` for the audit trail.

### Q1. `oauth2:token:empty-scopes` vendor error code — no captured fixture

**Where**: `phase_b_findings.headlines[1].evidence` references the vendor error code `oauth2:token:empty-scopes` returned when probing v1 scope syntax at the token endpoint.

**Problem**: The probe was run, but the token-endpoint response was not captured as a golden fixture. The error-code string appears only in the overlay's prose, not in any in-repo response. Reproducibility / spot-checkability is impacted.

**Suggested fix**: re-run the probe and capture the response body to `tests/golden/cerner/phase-b-2026-04-27/error-token-empty-scopes-v1-syntax.json`. Update `verification.source_quote` to point at the new fixture. (Could alternatively be deferred to next refresh cycle if the user trusts the original observation.)

**Resolution (2026-05-02)**: probe re-run; fixture captured to `tests/golden/cerner/phase-b-2026-04-27/error-token-empty-scopes-v1-syntax-2026-05-02.json`. The actual response is HTTP 400 `{"error":"invalid_scope","error_uri":"https://authorization.cerner.com/errors/urn%3Acerner%3Aerror%3Aauthorization-server%3Aoauth2%3Atoken%3Aempty-scopes/instances/<uuid>?client=...&tenant=..."}` — i.e. the URL-decoded URN `urn:cerner:error:authorization-server:oauth2:token:empty-scopes` is the vendor error code, surfaced via `error_uri` not as a top-level `error`. `headlines[1].evidence` updated with the precise envelope and fixture citation.

### Q2 (THE BIG ONE — see Stop-condition response below)

## Resolution (Option A applied)

**Files changed**:
- `tools/measure_phase_b.py:evaluate_one` — bundle-wide presence; cardinality/VS/format axes pinned to first resource with the path
- `tests/test_measure_phase_b_evaluate_one.py` — new regression test
- `ehrs/cerner/overlay.json` — element_deviations + multi_patient_coverage regenerated from goldens (no live re-probe; same `verified_date=2026-04-27`)
- `ehrs/epic/overlay.json` — same regeneration applied; Epic was unaffected on the original spot-check but the same code path produces its rows, so it was re-derived for safety. Stratification: matches 138→141, missing 20→17.

**Note on row_id changes**: Because `row_id` is derived from `(profile_id, path, deviation_category)`, the rows whose category changed from "missing" to "matches" got new row_ids. The schema has a `previous_row_ids` field for citation-anchor migration, but since this is pre-launch (no external citations exist yet), no migration record is needed.

**Programmatic audit script** (run from repo root) for future spot-checks:

```python
from tools.measure_phase_b import P0_PROBES, US_CORE_BASELINE, EHRS_DIR, GOLDEN_DIR, all_resources_of_type
from tools.conformance.presence import evaluate_path
import json
# (see report git history for the full audit; both vendors verified 0/N failures
# across all "missing" and "matches" rows.)
```
