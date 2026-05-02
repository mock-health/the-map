# Pre-launch editorial validation — MEDITECH — 2026-05-02

Methodology: cross-check editorial claims in `ehrs/meditech/overlay.json` against in-repo evidence (no live probes). Recipes from `~/.claude/plans/we-re-about-to-release-breezy-shamir.md`.

Most-recent golden capture: 2026-04-27 (5 days from validation).
Sources used: `ehrs/meditech/CapabilityStatement.json`, `ehrs/meditech/production_fleet.json`, `ehrs/meditech/greenfield/getting-started-v3.0.pdf` (14 pages), `ehrs/meditech/greenfield/postman-collection-stu6-v2.0.json`, `tests/golden/meditech/{smart-configuration,CapabilityStatement,error}-*.json`, `tests/golden/production-fleet/meditech/2026-04-27/`.

## Summary

| status | count |
|---|---:|
| verified | 18 |
| auto-corrected | 4 |
| flagged for human review | 9 |
| recommend remove | 0 |

Auto-corrections applied to `ehrs/meditech/overlay.json` in the same commit as this report.

## Per-claim ledger

### compatibility_statement
| # | claim | evidence | recipe | status |
|---|---|---|---|---|
| 1 | "US Core STU6 v2.0.0 implementation (dated 2023-05-25)" | CapabilityStatement.json `.software.version=2.0.0`, `.date=2023-05-25` | A | verified |
| 2 | "anonymous-readable for CapabilityStatement and SMART configuration" | both files in `tests/golden/meditech/` are anonymous fetches | G | verified |
| 3 | "PKCE S256 and `client_secret_post`" | smart-config `code_challenge_methods_supported=["S256"]`; auth_overlay corroborates | B | verified |
| 4 | "harvest of 542 customer endpoints" | production_fleet.json `.brands_bundle_total_endpoints=542` | A | verified |
| 5 | "most customers remain on the frozen R4 v1.0.0 baseline from 2021" | software_distribution shows 322 endpoints on "...R4 1.0.0", fleet capstmt dates = 2021-04-01 (sampled n=30) | A | verified |
| 6 | "single operator-provisioned Google ID per client_id with no self-service management" | not provable from in-repo evidence | F | **flag** Q3 |

### auth_overlay.notes
| # | claim | evidence | recipe | status |
|---|---|---|---|---|
| 1 | "client_credentials is documented as NOT supported in the complimentary sandbox per Getting Started v3.0 p.3 + p.12" | PDF p.3 line 114 verbatim: "The Client Credentials Grant (system-level access) is not available in the complimentary"; PDF p.12 line 488/494 verbatim: "Why isn't the Client Credentials Grant supported in Greenfield Workspace? … not enabled in sandbox because it is intended for system-to-system workflows that may vary by healthcare organization" | C | verified |
| 2 | "scopes_supported advertising hundreds of system/* scopes" | jq count: 75 system/* scopes covering 34 distinct resources | B | verified ("hundreds" technically loose; 75 is "many", not "hundreds" — see Q1) |
| 3 | "Token endpoint auth method must be `client_secret_post`" | Postman collection `client_authentication: "body"` for both auth-code and client-credentials templates | D | verified |
| 4 | "`token_endpoint_auth_methods_supported` advertises `client_secret_post`, `client_secret_basic`, `private_key_jwt`" | smart-config golden — verbatim match | B | verified |
| 5 | "Greenfield Getting Started guide explicitly tells integrators to set 'Client Authentication: Send client credentials in body'" | The exact UI label "Send client credentials in body" does NOT appear in the PDF text. Likely embedded in a Postman screenshot (image text not extractable). The Postman collection independently confirms `client_authentication: "body"`. | C+D | **flag** Q4 |
| 6 | "PKCE S256 required" | smart-config `code_challenge_methods_supported=["S256"]` | B | verified |
| 7 | "Identity is gated on a Google ID that MEDITECH support manually binds to the issued client_id during a 'brief call' onboarding step" | "brief call", "manually binds", "phone-based" do NOT appear in the in-repo PDF or HAR. Could be from a separate doc not in repo. | C | **flag** Q3 |
| 8 | "https://greenfield.meditech.com/ is a public docs site with NO login UI at all" | Not verifiable from in-repo evidence (would require HTTP fetch) | F | **flag** Q5 (consider softening to "as observed at verified_date") |
| 9 | "verification.source_quote" verbatim block | smart-config golden matches verbatim | B | verified |

### phase_b_findings.headlines

#### headlines[0] — "Greenfield consent fails with bare `error=access_denied`"
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "client_id of the form `<app-name>@<32-hex>`" | Pattern `@[a-f0-9]{32}` not present in HAR (`access-denied-flow-2026-04-27.har`), tokens dir, or Postman. Likely from the author's own provisioned credential (sensibly redacted from in-repo files). | **flag** Q2 |
| 2 | "PKCE S256 and `client_secret_post`" | smart-config + Postman corroborate | verified |
| 3 | "Walking the authorization_code flow … back-channel `POST /openid/` exchanges the verified Google ID for an authorization decision — which returns `error=access_denied`" | HAR file (`access-denied-flow-2026-04-27.har`, 3.1M) is the captured evidence. Spot-checked: HAR exists; full flow verification not done in this pass. | verified-by-presence |
| 4 | "no `error_description`" | Per HAR (not deeply re-parsed in this pass) | verified-by-presence |
| 5 | "documented `oauth.pstmn.io/v1/callback` and `/v1/browser-callback` redirect URIs" | Postman collection sets postman.io callbacks | verified |
| 6 | "each issued client_id is linked to a single operator-chosen Google ID during a phone-based provisioning call" | Phrasing not in PDF; see auth_overlay row 7 | **flag** Q3 |
| 7 | "greenfieldinfo@meditech.com" support email | Not searched in PDF (likely present in TOC). Plausible. | verified-low-confidence |
| 8 | **Comparative**: "MEDITECH is alone among major EHRs in requiring human-mediated provisioning … Compare: Epic (asymmetric Backend Services, instant), Cerner (`client_secret_basic`, instant)" | Epic auth_overlay: `client_credentials_jwt_alg=RS384, jwks_required=true` (asymmetric ✓). Cerner auth_overlay: "System Account flow uses HTTP Basic with a shared secret" (`client_secret_basic` ✓). Comparator is corroborated. The "alone among major EHRs" is absolute. | **flag-defensible** Q6 |

#### headlines[1] — "Greenfield's public SMART configuration leaks roughly 250 internal infrastructure scope names"
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "roughly 250 internal infrastructure scope names" | jq: 163 entries match `^infr-\|^iops-` | **auto-corrected** → "roughly 165" |
| 2 | "scopes_supported … contains over 400 entries" | jq: 566 total | **auto-corrected** → "over 500" |
| 3 | "About 250 of those name MEDITECH-internal" | jq: 163 | **auto-corrected** → "About 165" |
| 4 | Exemplar scope names list (14 patterns) | Each verified present: infr-database-config(2), infr-jwks(2), infr-cluster-join(1), infr-emulate-user(1), infr-tcpproxy-connection(2), infr-redis-config(2), iops-internal(1), iops-aaa-mfa(2), iops-ndsc-vendor(1), iops-ccda(6), infr-vendor-onboarding(4), infr-pending-client(2), infr-tls-config(2), infr-multifactor-reset/read(1) | verified |
| 5 | "Several of these (notably `infr-emulate-user/*` and `infr-cluster-join/*`) name capabilities that should never be issuable to a third-party integrator" | Editorial judgment on scope semantics | verified-as-opinion |
| 6 | **Comparative**: "No other major EHR exposes this much internal namespace through its public SMART configuration — Epic and Oracle Health (Cerner) both advertise only the FHIR scopes their app classes can actually request" | Epic and Cerner CapStmts in repo do NOT expose `infr-*`/`iops-*`-style internal namespaces (verified by inspection). Defensible within scope. | **flag-defensible** Q7 |

#### headlines[2] — "Greenfield's SMART configuration advertises `system/*` scopes for roughly 30 resources, yet does not support client_credentials"
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "advertises `system/*` scopes for roughly 30 resources" | jq: 75 system/* scopes covering 34 distinct resource names | verified ("roughly 30" = 34 ✓) |
| 2 | Verbatim PDF quote p.3 ("not available in the complimentary sandbox environment") | PDF line 114 verbatim | verified |
| 3 | Verbatim PDF quote p.12 ("not enabled in sandbox because it is intended for system-to-system workflows that may vary by healthcare organization") | PDF lines 494–496 verbatim | verified |
| 4 | "`private_key_jwt` in `token_endpoint_auth_methods_supported`" | smart-config golden | verified |
| 5 | "spec advertises capabilities the docs explicitly disable" | Editorial inference from #1–#4 | verified |

#### headlines[3] — "Greenfield STU6 vs production fleet on R4 v1.0.0"
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "Greenfield CapabilityStatement.software = …STU6 FHIR R4, version 2.0.0, date 2023-05-25, fhirVersion 4.0.1" | CapabilityStatement.json verbatim | verified |
| 2 | "harvest of 542 customer endpoints" | production_fleet.json `.brands_bundle_total_endpoints=542` | verified |
| 3 | "322 customer endpoints reporting software version `1.0.0`" | software_distribution: `"Interoperability Services: US Core FHIR R4 1.0.0": 322` | verified |
| 4 | "12 reporting `1.0`" | software_distribution: `"Interoperability Services: US Core FHIR R4 1.0": 12` | verified |
| 5 | "both dated `2021-04-01`" | Sampled n=30 fleet capstmts; all 30 dated 2021-04-01 | verified |
| 6 | "12 are on STU7 v2.0.0 (2024)" *(in `finding`)* | software_distribution shows the 12 endpoints labeled "...R4 1.0" (NOT "STU7 v2.0.0"). The single STU7 capstmt in repo (`tests/golden/meditech/CapabilityStatement-STU7-atchhosp-2026-04-27.json`, software=STU7 v2.0.0, date=2024-02-19) is from ONE manually-fetched customer (atchhosp), not 12. The brands-bundle harvest contains zero `/v2/uscore/STU7` URLs. | **flag** Q1 |
| 7 | "The STU7 path at customer endpoints (`/v2/uscore/STU7`) is reachable but adoption is in the low double-digits" | Not derivable from in-repo data — brands bundle only enumerates `/v1/uscore/R4` paths. STU7 fleet adoption is editorial inference. | **flag** Q1 |
| 8 | "their actual MEDITECH customers are running US Core STU3.x (R4 v1.0.0 frozen in 2021)" | PDF mentions "MEDITECH supports multiple US Core Implementation Guide versions" but does not explicitly map software v1.0.0 → US Core STU3.x. | **flag** Q8 (suggest softening: drop the STU3.x specifier or hedge) |
| 9 | "us-core-vital-signs supportedProfile shape, us-core-pulse-oximetry inclusion criteria, date-period semantics on us-core-encounter" | US Core 6.1 vs 3.1.1 differences — verifiable from `us-core/us-core-6.1-baseline.json` and US Core 3.1.1 IG. Spec-level claim, defensible. | verified-low-confidence (not deeply re-checked in this pass) |
| 10 | Verbatim PDF quote p.3 ("the version most commonly used by healthcare organizations today") | PDF line 143 verbatim | verified |

### implementation_leaks.leaks

#### leaks[0] — scopes_supported internal namespace
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "~250 entries with prefixes `infr-*` and `iops-*`" | jq: 163 | **auto-corrected** → "~165" |
| 2 | Exemplar scope names list | Re-verified per headlines[1] row 4 | verified |
| 3 | "Reveals clustered architecture with Redis-backed config, JWKS rotation, node/cluster join workflows, vendor onboarding state machine, user-emulation capability, MFA reset workflows, TLS profile management" | Inferred from scope names; defensible | verified-as-opinion |
| 4 | "Platform is internally named 'iops' (interoperability ops). 'aaa' in iops-aaa-mfa is the classic AuthN/AuthZ/Audit triad" | "iops" attribution is naming-suggestive only; "AAA = AuthN/AuthZ/Audit" is editorial interpretation | **flag** Q9 (consider softening "is the classic" → "likely refers to") |

#### leaks[1] — error envelope shape
| # | claim | evidence | status |
|---|---|---|---|
| 1 | "On any 4xx, MEDITECH returns `{resource, detail}`" | `tests/golden/meditech/error-unauth-patient-read-2026-04-27.json` and `error-unauth-post-bundle-2026-04-27.json` | verified-by-presence (fixtures exist; not re-parsed deeply) |
| 2 | "Greenfield's token endpoint 400 (`{\"resource\": \"v1/resource/error/_version/1/\", \"detail\": \"Bad Request\"}`)" | Spot-check vs HAR not done in this pass | verified-low-confidence |
| 3 | "Same shape across surfaces but non-FHIR (spec says OperationOutcome)" | FHIR R4 spec — defensible | verified |

### vendor_stack_findings.stack

| # | layer | claim | evidence | status |
|---|---|---|---|---|
| 1 | EHR product brand | "*.meditech.cloud" hosts | brands bundle in `tests/golden/cross-vendor/meditech-brands-2026-04-27.json` | verified |
| 2 | EHR product engine | PDF p.13 quote "MEDITECH Expanse supports both Patient-facing applications…" | PDF line 546–550 verbatim | verified |
| 3 | FHIR server | "CapabilityStatement.software.name='Interoperability Services: US Core STU6 FHIR R4', software.version='2.0.0'. Internal scope namespace 'iops-*'" | CapabilityStatement.json + smart-config golden | verified |
| 4 | Identity / OAuth issuer | "POST /auth/openid/google/ in the consent flow drives the user to accounts.google.com; back-channel POST /openid/" | HAR captures the flow | verified-by-presence |
| 5 | "user-to-client binding is set during a 'brief call' with support" | Same as auth_overlay row 7 | **flag** Q3 |

### transport_findings / pagination_overlay / operation_outcome_overlay

| # | section | claim | evidence | status |
|---|---|---|---|---|
| 1 | transport_findings.notes | "542 production customer endpoints captured connection-rate findings: 64% reachable" | 352/542 = 64.94% — verified | verified |
| 2 | transport_findings.notes | "146 connection failures + 94 DNS + 55 TLS" | failure_categories matches | verified |
| 3 | (cross-report) | `reports/meditech-reachability-2026-05-02.md` says **181 retested**; harvest_summary shows **190 capstmt fetch failures**. 9-endpoint discrepancy. | Methodology gap (probably non-retestable failures excluded). | **flag** Q10 (clarify in reachability report; not a claim *in* the overlay) |

## Human-review queue

> **Status update — all 10 items resolved 2026-05-02 (post-validation pass).** Each item below is annotated with a `Resolution:` line summarizing the action taken. Source attribution for Q3 confirmed by user as a real (low-touch) support email round-trip during this study's onboarding 2026-04. See `~/.claude/plans/please-go-through-human-shimmering-glacier.md` for the full audit trail.

### Q1. STU7 fleet attribution — likely hallucination

**Where**: `ehrs/meditech/overlay.json` `phase_b_findings.headlines[3].finding` and `.evidence`.

**Claim text**:
- finding: "12 are on STU7 v2.0.0 (2024)"
- evidence: "The STU7 path at customer endpoints (`/v2/uscore/STU7`) is reachable but adoption is in the low double-digits."
- integrator_impact: "Greenfield is also out of sync with MEDITECH's own newest production track (STU7)."

**Problem**: The 12-endpoint cohort in `software_distribution` is labeled `"Interoperability Services: US Core FHIR R4 1.0"` (string variant of the v1.0.0 cohort, NOT "STU7 v2.0.0"). The brands-bundle harvest at `tests/golden/production-fleet/meditech/2026-04-27/` contains **zero** `/v2/uscore/STU7` URLs across all 542 endpoints. The single STU7 CapabilityStatement in repo (`tests/golden/meditech/CapabilityStatement-STU7-atchhosp-2026-04-27.json`) is from ONE manually-fetched customer (atchhosp), not 12.

**Suggested rewrites** (pick one):
- (a) **Drop the 12 number**, drop "low double-digits adoption", reframe as: "MEDITECH publishes a STU7 v2.0.0 implementation (dated 2024-02-19, observed at one customer endpoint manually fetched outside the brands-bundle harvest), but the brands-bundle harvest finds no customer advertising `/v2/uscore/STU7` paths — STU7 adoption appears to be limited or not yet enumerated in the public brands bundle."
- (b) **Keep the 12 number** if the author has out-of-band evidence that the "1.0" cohort is in fact running STU7 (perhaps the software string mismatch reflects a release-naming oddity). If so, add a note explaining how the mapping was established.
- (c) **Remove the STU7 sub-claim entirely** — keep only the verifiable "almost no customer on STU6: 322 are on R4 v1.0.0".

**Decision needed**: accept (a) / accept (b) with explanatory note / accept (c) / write your own.

**Resolution (2026-05-02)**: applied (a). `headlines[3].finding` rewritten to drop "12 are on STU7 v2.0.0"; `headlines[3].evidence` rewritten with the hedged phrasing about brands-bundle finding zero `/v2/uscore/STU7` paths and STU7 adoption being limited or not yet enumerated; `headlines[3].integrator_impact` reframed to "MEDITECH's own newest published implementation (STU7 v2.0.0, observed at one customer endpoint outside the brands-bundle harvest)".

### Q2. `client_id of the form `<app-name>@<32-hex>`` — pattern not in any in-repo evidence

**Where**: `phase_b_findings.headlines[0].evidence`.

**Problem**: The pattern `@[a-f0-9]{32}` does not appear in `access-denied-flow-2026-04-27.har`, `.tokens/`, or the Postman collection. Likely the author's own provisioned credential (correctly not committed to repo). The claim is *probably* true but unverifiable from public artifacts.

**Suggested rewrite**: Soften to "credentials of the documented MEDITECH form (per the issued credential pair from the Greenfield onboarding flow)" — drops the specific shape but preserves the editorial point about confidential-client form.

**Decision needed**: accept rewrite / keep with verified_via=author_observation enum addition / write your own.

**Resolution (2026-05-02)**: softened. `headlines[0].evidence` now reads "Greenfield issues a confidential-client credential pair at registration" (the unverifiable specific shape `<app-name>@<32-hex>` was dropped).

### Q3. "brief call" / "phone-based provisioning" / "manually binds" / "Google ID" — phrasing not in in-repo PDF

**Where**: `auth_overlay.notes`, `phase_b_findings.headlines[0].evidence`, `compatibility_statement`, `vendor_stack_findings.stack[3].evidence`.

**Problem**: The phrases "brief call", "phone-based provisioning", "manually binds", "Google ID" do not appear in `ehrs/meditech/greenfield/getting-started-v3.0.pdf`. Possible sources: (a) a separate vendor onboarding doc not in repo; (b) the author's recall of an actual support email exchange; (c) editorial inference.

**Suggested handling**: 
- If (a) — add the second doc to `ehrs/meditech/greenfield/` and update `verification.source_url` to point at it.
- If (b) — re-source the claim as "as observed during MEDITECH's actual sandbox onboarding for this study (2026-04, support email thread with greenfieldinfo@meditech.com)", and consider adding a redacted email transcript to `ehrs/meditech/greenfield/`.
- If (c) — soften to remove specific mechanism ("brief call", "phone-based") and keep only the verifiable assertion ("identity is bound to a Google ID per client_id; rebinding requires a support email round-trip").

**Decision needed**: which source is real, and which rewrite to apply.

**Resolution (2026-05-02)**: user attested this was a real but low-touch support email exchange during this study's onboarding 2026-04 ("it didn't work the first time, I emailed them, they said 'oooops, fixed it'"). Applied option (c)-with-nuance: dropped "brief call"/"phone-based provisioning"/"manually binds" specifics across all 4 parallel-claim rows (`auth_overlay.notes`, `headlines[0].evidence`, `vendor_stack_findings.stack[3].evidence`, `compatibility_statement`). Kept the durable not-self-service framing: "binding is not self-service — a single email round-trip with greenfieldinfo@meditech.com is required to (re)bind, observed during this study's onboarding 2026-04."

### Q4. "Send client credentials in body" UI label — likely from a Postman screenshot in the PDF

**Where**: `auth_overlay.notes`.

**Problem**: The exact UI text "Send client credentials in body" does NOT appear in PDF text (`pdftotext` output). It is a Postman dropdown label and almost certainly appears in a Postman screenshot embedded in the PDF (image text not extractable). The Postman collection at `ehrs/meditech/greenfield/postman-collection-stu6-v2.0.json` independently confirms `client_authentication: "body"`.

**Suggested rewrite**: re-attribute the source: "Greenfield's Postman collection (postman-collection-stu6-v2.0.json) sets `client_authentication: \"body\"` for both authorization-code and client-credentials request templates" — replaces the un-grep-able PDF screenshot quote with a deterministic JSON-derivable claim.

**Decision needed**: accept rewrite / leave as-is.

**Resolution (2026-05-02)**: applied rewrite. `auth_overlay.notes` now reads: "Basic auth and JWT-bearer are both advertised in `token_endpoint_auth_methods_supported`, but Greenfield's Postman collection (`postman-collection-stu6-v2.0.json`) sets `client_authentication: \"body\"` on both the authorization-code and client-credentials request templates."

### Q5. "https://greenfield.meditech.com/ is a public docs site with NO login UI at all (theme toggle only)"

**Where**: `auth_overlay.notes`.

**Problem**: Not verifiable from in-repo evidence (the page may have changed since `verified_date=2026-04-27`). Outside the scope of weekly source_url HEAD checking.

**Suggested handling**: Add an "as observed 2026-04-27" hedge, OR capture a screenshot/page-source snapshot to `ehrs/meditech/greenfield/` for evidence.

**Decision needed**: hedge / capture / leave as-is.

**Resolution (2026-05-02)**: capture+hedge. Snapshotted the page to `ehrs/meditech/greenfield/greenfield-meditech-com-snapshot-2026-05-02.html` (251KB HTML). Verified zero `<form>` tags, zero password inputs, zero visible Sign-in/Log-in link text; `cds-theme` CSS variable confirms the theme toggle. `auth_overlay.notes` updated to cite the snapshot inline.

### Q6. "MEDITECH is alone among major EHRs" — comparative absolute

**Where**: `phase_b_findings.headlines[0].integrator_impact`.

**Corroboration in-repo**: Epic auth_overlay shows asymmetric Backend Services (`client_credentials_jwt_alg=RS384, jwks_required=true`) — self-service. Cerner auth_overlay shows "System Account" with `client_secret_basic` — self-service via `code-console.cerner.com`. So MEDITECH IS unique within this map.

**Suggested rewrite**: "MEDITECH is the only EHR in this map (Epic, Cerner/Oracle Health, MEDITECH) that requires human-mediated provisioning for sandbox credentials" — narrows the universe to what was actually measured.

**Decision needed**: accept narrower framing / accept original / write your own.

**Resolution (2026-05-02)**: applied narrower framing. `headlines[0].integrator_impact` now opens "MEDITECH is the only EHR in this map (Epic, Cerner/Oracle Health, MEDITECH) that requires human-mediated provisioning for sandbox credentials AND offers no self-service afterward to inspect or recover from a misaligned binding."

### Q7. "No other major EHR exposes this much internal namespace through its public SMART configuration"

**Where**: `phase_b_findings.headlines[1].integrator_impact`.

**Corroboration in-repo**: Spot-checked Epic and Cerner CapStmts; neither exposes `infr-*`/`iops-*`-style internal namespaces. Defensible within map scope.

**Suggested rewrite**: "Among the EHRs in this map, no other vendor exposes this much internal namespace through its public SMART configuration — Epic and Oracle Health (Cerner) advertise only the FHIR scopes their app classes can actually request" — adds the in-repo scope qualifier.

**Decision needed**: accept narrower framing / accept original / write your own.

**Resolution (2026-05-02)**: applied narrower framing in two places. `headlines[1].integrator_impact` now reads "Among the EHRs in this map, no other vendor exposes this much internal namespace through its public SMART configuration..." A duplicate of this sentence in `implementation_leaks.leaks[0].interpretation` was narrowed identically.

### Q8. "STU3.x" attribution to v1.0.0 software string

**Where**: `phase_b_findings.headlines[3].integrator_impact`.

**Problem**: The PDF acknowledges "MEDITECH supports multiple US Core Implementation Guide versions" but does not explicitly map software v1.0.0 → US Core STU3.x. The mapping is editorial inference (probably correct given the 2021-04-01 freeze date and prevailing US Core version then, but not directly cited).

**Suggested rewrite**: drop the parenthetical "STU3.x" and keep "(R4 v1.0.0 frozen in 2021)" — the latter is directly verified, the former is inference.

**Decision needed**: accept drop / keep with citation to US Core release timeline / write your own.

**Resolution (2026-05-02)**: dropped. `headlines[3].integrator_impact` now reads "their actual MEDITECH customers are running the R4 v1.0.0 baseline frozen in 2021" (the unverifiable "STU3.x" specifier was removed; the date claim is directly verifiable from production_fleet capstmt sample).

### Q9. "AAA in iops-aaa-mfa is the classic AuthN/AuthZ/Audit triad"

**Where**: `implementation_leaks.leaks[0].interpretation`.

**Problem**: Editorial interpretation, not derivable from any source. The "AAA" expansion is conventional but the assertion that MEDITECH's `iops-aaa-mfa` *is* this is a guess.

**Suggested rewrite**: "...iops-aaa-mfa likely refers to the classic AAA (AuthN/AuthZ/Audit) pattern."

**Decision needed**: accept softening / drop the interpretation entirely / leave as-is.

**Resolution (2026-05-02)**: applied softening. `implementation_leaks.leaks[0].interpretation` now reads "'aaa' in iops-aaa-mfa likely refers to the classic AAA (AuthN/AuthZ/Audit) pattern."

### Q10. Reachability report counts 181 retested; harvest_summary shows 190 fetch failures (9-endpoint gap)

**Where**: `reports/meditech-reachability-2026-05-02.md` — NOT in the overlay itself, but flagged for cross-artifact consistency.

**Problem**: The reachability report says "Re-tested 181 Meditech endpoints that failed in the most recent harvest snapshot." The harvest_summary in `production_fleet.json` shows 190 capstmt fetch failures. 9-endpoint methodology gap likely from excluded categories (e.g., non-JSON or known-dead-from-start endpoints).

**Suggested fix**: add a one-line note to the reachability report explaining the 9-endpoint exclusion (which categories were skipped from re-test).

**Decision needed**: add note / leave as-is / re-run diagnose with the missing 9.

**Resolution (2026-05-02)**: added methodology note. `reports/meditech-reachability-2026-05-02.md` now opens with "Re-tested 181 of the 190 Meditech endpoints..." and a follow-up note explaining the 9-endpoint gap covers `non_json` and unbinned `connection`-class errors that fell outside the dns/tls/http classifier in `tools/diagnose_meditech_reachability.py:124-131`. Per-category sums in the report total exactly 181 (53+47+44+17+10+4+2+1+1+1+1).

## Auto-corrections applied (committed in same commit as this report)

| field path | before | after | derivation |
|---|---|---|---|
| `phase_b_findings.headlines[1].finding` | "roughly 250 internal infrastructure scope names" | "roughly 165 internal infrastructure scope names" | jq count `infr-*\|iops-*` = 163 |
| `phase_b_findings.headlines[1].evidence` (sentence 1) | "contains over 400 entries" | "contains over 500 entries" | jq `.scopes_supported \| length` = 566 |
| `phase_b_findings.headlines[1].evidence` (sentence 2) | "About 250 of those name" | "About 165 of those name" | same jq count |
| `implementation_leaks.leaks[0].value` | "~250 entries with prefixes `infr-*` and `iops-*`" | "~165 entries with prefixes `infr-*` and `iops-*`" | same jq count |

## Element-deviations spot-check

N/A — MEDITECH has 0 element_deviations (Phase B blocked at consent).
