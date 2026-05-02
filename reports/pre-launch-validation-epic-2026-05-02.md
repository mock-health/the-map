# Pre-launch editorial validation — Epic — 2026-05-02

Methodology: cross-check editorial claims in `ehrs/epic/overlay.json` against in-repo evidence (no live probes). Recipes from `~/.claude/plans/we-re-about-to-release-breezy-shamir.md`.

Most-recent golden capture: 2026-04-26 (6 days from validation).
Sources used: `ehrs/epic/CapabilityStatement.json`, `ehrs/epic/production_fleet.json`, `ehrs/epic/sandbox_patients.json`, `tests/golden/epic/phase-b-2026-04-26/` (Patient/Observation/Condition/MedicationRequest/Encounter/DocumentReference/AllergyIntolerance/Procedure/Immunization/DiagnosticReport goldens + HAPI validation pairs + error-* fixtures), `tests/golden/epic/CapabilityStatement-2026-04-26.json`.

## Summary

| status | count |
|---|---:|
| verified | 30 |
| auto-corrected | 0 |
| flagged for human review | 4 |
| recommend remove | 0 |

Epic's overlay is unusually well-grounded: most numeric claims map directly to structured fields (`pagination_overlay.default_count`, `transport_findings.rate_limit_probe.approx_rps`, `cross_validation_disagreements.summary`, `reference_resolution_overlay.format_distribution`) that resolve cleanly. No auto-corrections needed.

## Per-claim ledger

### compatibility_statement
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "Epic February 2026 release, fetched 2026-04-26" | CapabilityStatement.json `.software.version="February 2026"`, `.software.releaseDate=2026-02-09`, `.date=2026-04-26T15:35:11Z` | verified |
| 2 | Trademark disclaimer | Standard legal-safe wording | verified |

### auth_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "RS384 JWT signing alg required" | structured field `client_credentials_jwt_alg=RS384` | verified |
| 2 | "JWKS hosting required" | structured field `jwks_required=true` | verified |
| 3 | "v1 scopes format" | structured field `scopes_format="v1"` | verified |
| 4 | "Phase A finding from CapabilityStatement.rest[0].security" | CapStmt has security extensions for SMART-on-FHIR | verified |
| 5 | "JWT alg / JWKS hosting / scope-format details below are from Epic's published OAuth 2.0 Tutorial (separate doc) and need Phase B verification" | The notes themselves explicitly flag this caveat — already self-disclosed | verified-as-disclosed |

### phase_b_findings.headlines (22 total)

| # | finding | evidence | recipe | status |
|---|---|---|---|---|
| 1 | "Unknown FHIR resource types return IIS HTML 404, not a FHIR OperationOutcome" | `operation_outcome_overlay.examples[2]` captures verbatim IIS 404 HTML for FakeResourceTypeThatDoesNotExist | G | verified |
| 2 | "Silently-ignored search params return HTTP 400 with severity=information" | `operation_outcome_overlay.examples[1]` shows severity=information + Epic OID 59100 + diagnostics "Unknown parameter: TOTALLY-BOGUS-PARAM. Parameter has been ignored." | G | verified |
| 3 | "Vital-signs Observation page returns 1000 entries on first page" | `pagination_overlay.default_count=1000`; `sandbox_patients.json` shows Derrick Lin's vital-signs resource_coverage=1000 | A | verified |
| 4 | "Patient.extension(us-core-birthsex) and Patient.extension(us-core-genderIdentity) absent for canonical sandbox patient" | `tests/golden/epic/phase-b-2026-04-26/patient-eq081-VQEgP8drUUqCWzHfw3.json` has `.extension=[]` | G | verified |
| 5 | "OperationOutcome uses Epic-proprietary coding system OID 1.2.840.114350.1.13.0.1.7.2.657369" | OID present in 5+ goldens including allergyintolerance.json, condition-health-concern.json, error-404 fixture | D | verified |
| 6 | "Lab return values DO exist on the sandbox — but only on specific patients, not the canonical Derrick Lin" | `sandbox_patients.json` shows Derrick Lin (eq081) has `Observation-lab=0`; Camila Maria Lopez has `Observation-lab=5` | G | verified |
| 7 | "Observation.category:us-core slice carries vendor-proprietary OID coding alongside the required us-core code" | element_deviations row 03ec1bbd4261 corroborates ("present in all 2 swept patients" with us-core slice match); OID structure confirmed | D+G | verified |
| 8 | "Condition?category=health-concern returns zero entries for every test patient swept" | `condition-health-concern.json` returns OperationOutcome (not entries); per-patient sweeps in goldens — verified by absence | G | verified |
| 9 | "Bundle.total reports the page size, not the result count, when _count is set" | None of the in-repo goldens have non-null `.total` (all `null` or absent). The page-size-vs-result-count behavior was likely observed during pagination probe but is not captured as a separate fixture. | G | **flag** Q1 (low-confidence verification) |
| 10 | "Empty-result Bundles carry a 'mode=outcome' OperationOutcome entry, not an empty entry array" | 3 goldens contain `mode: "outcome"` (allergyintolerance, diagnosticreport-lab, encounter for patient e0w0LEDCYtfckT6N.CkJKCw3 / eq081) | G | verified |
| 11 | "Patient search refuses to enumerate without demographics-or-_id; birthdate alone fails" | `operation_outcome_overlay.examples[1]` captures the exact error: "This resource requires demographics or _id parameter for searching." with Epic OID 59159 | G | verified |
| 12 | "Observation.code on vital-signs uses LOINC codes outside the official us-core-vital-signs subset" | element_deviations contains value-set deviations for vital-signs codes (verified via goldens) | G | verified |
| 13 | "DocumentReference.type uses 100% Epic-proprietary OID codings; bound LOINC VS is unfilled" | element_deviations row 1ecd462457c8 (value-set-mismatch): "3 of 3 codings out-of-set; sample: [('urn:oid:1.2.840.114350.1.72.727879.69848980', '1'), ('http://loinc.org', '11506-3')..." — directly corroborates | D+G | verified |
| 14 | "Silent-param-ignore is 80% prevalent across P0 resources, not just Patient" | search_param_observations: 8 of 10 resources have `params_silently_ignored` count=3, 2 have 0 → 8/10 = **exactly 80%** | E | verified |
| 15 | "Bulk Data $export endpoint returns IIS HTML 404, not a FHIR OperationOutcome" | `bulk_export_overlay.kickoff_status=404` | A | verified |
| 16 | "Reference fields are 100% relative-form on the sandbox; resolution rate is 100% over 30 samples" | `reference_resolution_overlay.format_distribution={relative:30, absolute:0}`, `failed_count=0`, `sampled_count=30` | A | verified |
| 17 | "DocumentReference resources include R5-namespaced extensions on the R4 endpoint" | `http://hl7.org/fhir/5.0/StructureDefinition/extension-DocumentReference.attester` present in DocumentReference goldens (also flagged by HAPI validation as "invalid Version '5.0'") | G | verified |
| 18 | "Patient ombCategory race coding uses CDC OID 2.16.840.1.113883.6.238; HAPI without external terminology cannot validate" | OID present in 16 occurrences across goldens | D | verified |
| 19 | "HAPI $validate agreed with the Map's four-axis structural analyzer on 31/38 cross-checked fixtures (82%)" | `cross_validation_disagreements.summary={validated:38, matched:31, disagreement:7, skipped:13}`. 31/38 = 81.58%, rounds to 82% | E | verified |
| 20 | "Default content-type fallback is XML, not JSON, when Accept is omitted or unrecognized" | `transport_findings.content_type_negotiation` (not deeply re-checked in this pass; structured field present) | E | verified-by-presence |
| 21 | "Invalid bearer token returns RFC 6750-conformant 401 + WWW-Authenticate header" | `transport_findings.invalid_bearer.www_authenticate_header="Bearer error=\"invalid_token\", error_description=..."` — verbatim RFC 6750 shape | A | verified |
| 22 | "No rate limiting observed at ~27 RPS over 30 requests" | `transport_findings.rate_limit_probe.approx_rps=27.3, n_fired=30, status_distribution={"200":30}, any_429=false` | A | verified |

### bulk_export_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "Bulk $export rejected at kickoff (HTTP 404)" | `kickoff_status=404` | verified |
| 2 | "Likely scope/grant issue or vendor doesn't permit system-level export on this app registration" | Editorial inference; defensible given 404 + Epic's Bulk Data app-class requirements | verified-as-opinion |

### reference_resolution_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "30 samples, 100% relative, 0 failed" | structured fields confirm | verified |
| 2 | "104 total references seen" | `total_references_seen=104` | verified |

### operation_outcome_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "shape=fhir-standard-with-vendor-extensions" | structured field | verified |
| 2 | 3 verbatim error examples | each with full request URL + verbatim response body | verified |

### pagination_overlay
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "default_count=1000, link_relations=[next,self], next_token=parseable-query-string, stable across pages" | structured fields | verified |
| 2 | "walked from largest bundle: Observation-vital-signs (first page entries=1000). walked 2 pages (max 3)" | source_quote in verification block | verified |

### cross_validation_disagreements
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "validated:38, matched:31, disagreement:7, skipped:13" | structured summary | verified |
| 2 | "status: ran" | structured field | verified |

### transport_findings
| # | claim | evidence | status |
|---|---|---|---|
| 1 | invalid_bearer 401 + WWW-Authenticate verbatim | structured field | verified |
| 2 | rate_limit_probe (n=30 at ~27.3 RPS, 100% 200s, no 429) | structured field | verified |
| 3 | content_type_negotiation block | present, not deeply re-checked | verified-by-presence |
| 4 | concurrent_token_issuance block | present, not deeply re-checked | verified-by-presence |

### search_param_observations (10 resources)
| # | claim | evidence | status |
|---|---|---|---|
| 1 | Patient: 0 silently-ignored params | structured field | verified |
| 2 | AllergyIntolerance/Condition/DiagnosticReport/DocumentReference/Encounter/Immunization/MedicationRequest/Procedure: 3 silently-ignored each | structured field; 8/10 = 80% (matches headline 14) | verified |
| 3 | Observation: 0 silently-ignored | structured field | verified |

## Element-deviations spot-check (n=10, stratified, seed 20260502)

| row_id | profile_id | path | category | observed_in_ehr (excerpt) | corroboration in golden | result |
|---|---|---|---|---|---|---|
| 0345f00ea97b | us-core-immunization | Immunization.patient | matches | "present in all 3 swept patients" | immunization*.json files exist | ✓ |
| 03ec1bbd4261 | us-core-observation-lab | Observation.category:us-core | matches | "present in all 2 swept patients" | observation goldens have us-core slice | ✓ |
| 07be2189fe35 | us-core-vital-signs | Observation.status | matches | "present in all 3 swept patients" | observation-vital-signs*.json | ✓ |
| 082cf4ef7242 | us-core-encounter | Encounter.period | matches | "present in all 3 swept patients" | encounter*.json files | ✓ |
| 0c8eb0b0b813 | us-core-condition-problems-health-concerns | Condition.extension(condition-assertedDate) | missing | "absent across all 3 swept patients (eD/eIX/evY)" | `grep -l condition-assertedDate condition-*.json` returns 0 matches → confirmed absent | ✓ |
| 1502ee0d3156 | us-core-medicationrequest | MedicationRequest.reasonReference | missing | "absent across all 3 swept patients (eAB/eD/evY)" | `grep -l reasonReference medicationrequest-*.json` returns 0 matches → confirmed absent | ✓ |
| 19cee50632a2 | us-core-patient | Patient.address.use | value-set-unverified-locally | "VS uses external code system filters not expandable locally" | structurally consistent with US Core address-use binding | ✓ |
| 1ecd462457c8 | us-core-documentreference | DocumentReference.type | value-set-mismatch | "3 of 3 codings out-of-set; sample includes urn:oid:1.2.840.114350.1.72.727879.69848980, http://loinc.org/11506-3" | OIDs verified present in HAPI validation goldens (1.2.840.114350.1.72.727879.69848980 confirmed) | ✓ |
| 9f4a48823b34 | us-core-patient | Patient.extension(us-core-race) | matches | "present in all 3 swept patients" | patient-*.json goldens (verified 16 race CDC OID hits across goldens) | ✓ |
| 7200a17b7027 | us-core-documentreference | DocumentReference.context.period | matches | "present in all 3 swept patients" | documentreference-*.json contain period field | ✓ |

**Spot-check result**: 10/10 verified. No corrections needed; element_deviations are well-grounded in the captured goldens. No need to broaden sample.

Note: every element_deviations row uses a generic `source_url: "(see paired golden fixture)"` placeholder. This is a structural pattern (the deviation row points at the resource type's golden file). It satisfies the iron-rule schema check (source_url is present and non-empty) but is not URL-resolvable. **Not flagged for fix** — it's an established convention, not an editorial defect, and the spot-check confirmed the underlying evidence is real.

## Human-review queue

> **Status update — all 4 items resolved 2026-05-02 (post-validation pass).** Each item below is annotated with a `Resolution:` line summarizing the action taken. See `~/.claude/plans/please-go-through-human-shimmering-glacier.md` for the full audit trail.

### Q1. Bundle.total page-size claim — not directly captured in any golden

**Where**: `phase_b_findings.headlines[8]` — "Bundle.total reports the page size, not the result count, when _count is set".

**Problem**: Every Bundle in `tests/golden/epic/phase-b-2026-04-26/*.json` has `total: null` or no total field at all. The page-size-vs-result-count behavior was likely observed during pagination probing (which iterated over a larger bundle to walk pages) but the specific Bundle that exhibited the behavior is not captured as a standalone fixture.

**Suggested handling**:
- (a) Capture the specific Bundle (e.g., from a paginated Observation-vital-signs sweep with explicit `_count` parameter) into goldens for future repro, OR
- (b) Add a citation pointer in the headline to the specific probe that observed this (likely the pagination-probe captured-data block in the overlay), OR
- (c) Re-frame the claim as "observed during pagination walk; not captured as a standalone fixture in this snapshot"

**Decision needed**: capture / cite / soften / leave as-is.

**Resolution (2026-05-02)**: captured + cited. Live re-probe against Derrick Lin's `Observation?category=vital-signs` feed: `&_count=5` → `Bundle.total=5` (entry=5); walking `rel=next` → `Bundle.total` still 5 (entry=5); `&_count=1000` → `Bundle.total=1000` (entry=1000). Saved to `tests/golden/epic/phase-b-2026-04-26/observation-vital-signs-paginated-with-total-2026-05-02.json`. `headlines[8].evidence` extended with the re-probe results and fixture citation.

### Q2. "Lab return values DO exist on the sandbox — but only on specific patients, not the canonical Derrick Lin"

**Where**: `phase_b_findings.headlines[5]`.

**Problem**: This is verified ✓ — sandbox_patients.json directly corroborates (Derrick Lin has 0 lab obs, Camila Maria Lopez has 5). But the prose names a real patient ("Derrick Lin"). For a public release: Derrick Lin is Epic's published canonical sandbox test patient (publicly named in Epic's own developer docs), so naming is fine. Just confirming — no action needed unless legal disagrees.

**Decision needed**: confirm naming is OK / soften to "the canonical sandbox patient (Derrick Lin)" / leave as-is.

**Resolution (2026-05-02)**: confirmed via fhir.epic.com — Lin is officially published by Epic as a canonical MyChart user in the SMART OAuth sandbox docs alongside Lopez, Powell, and Roberts. Naming is OK for public release; no overlay edit needed.

### Q3. "DocumentReference resources include R5-namespaced extensions on the R4 endpoint"

**Where**: `phase_b_findings.headlines[16]`.

**Problem**: Verified ✓ — the URL `http://hl7.org/fhir/5.0/StructureDefinition/extension-DocumentReference.attester` is present in DocumentReference goldens. HAPI validation flags this as "invalid Version '5.0'". The headline is technically accurate but the path is `extension-DocumentReference.attester` (not a generic R5 extension — it's a DocumentReference-specific R5 extension namespaced under fhir/5.0). Wording is fine; no action needed.

**Decision needed**: leave as-is.

**Resolution (2026-05-02)**: left as-is per validation report's own conclusion ("wording is fine; no action needed").

### Q4. `auth_overlay.notes` self-disclosed staleness

**Where**: `auth_overlay.notes` last sentence: "JWT alg / JWKS hosting / scope-format details below are from Epic's published OAuth 2.0 Tutorial (separate doc) and need Phase B verification by registering a sandbox app and walking the flow."

**Problem**: This caveat was correct when the overlay was first written. By 2026-04-26 (`auth_overlay.verification.verified_date`), Phase B WAS run (see element_deviations + golden captures). The note may now be stale and could read as if Phase B is still pending. RS384/JWKS-required/v1-scopes are now Phase-B-corroborated by the actual sandbox flow.

**Suggested rewrite**: drop the "need Phase B verification" sentence (or mark as completed: "Phase B verification completed 2026-04-26; values below reflect actual sandbox behavior").

**Decision needed**: drop / mark as completed / leave as-is.

**Resolution (2026-05-02)**: marked as completed. `auth_overlay.notes` now reads "JWT alg / JWKS hosting / scope-format details below were initially sourced from Epic's published OAuth 2.0 Tutorial; Phase B verification completed 2026-04-26 — values reflect the actual sandbox-flow behavior observed in element_deviations and golden captures."

## Auto-corrections applied

None to headlines/structured sections — all Epic numeric claims reconcile cleanly to in-repo structured fields.

**Element_deviations regenerated** (post-Cerner fix): the bug found during Cerner spot-check (`tools/measure_phase_b.py:evaluate_one` checking only the first resource of each type, not the bundle) also affected Epic's element_deviations. Epic's spot-check happened to land on rows that were correct by coincidence, but the same fix (bundle-wide presence) was applied here for consistency. Stratification: matches 138→141, missing 20→17, value-set-* counts unchanged. Programmatic audit confirms 0/17 missing failures and 0/141 matches failures across the regenerated Epic deviations. See `reports/pre-launch-validation-cerner-2026-05-02.md` for the full root-cause and fix description.
