# METHODOLOGY — how we measure

This is the runbook. PLAN.md is strategy; this file is what an actual human (or Claude) follows step-by-step to add or update an EHR in the Map.

**Iron rule:** every claim about an EHR's behavior is sourced. CapabilityStatement claims are sourced *to the CapabilityStatement file itself* (verbatim, fetched + dated). Overlay claims (the things the CapStmt cannot tell you) carry an explicit citation: source URL + verbatim quote + verified date + verification method. PRs that violate this rule are rejected, no exceptions.

**Architecture (2026-04-26 revision):** there is no synthesized intermediate file. Two source-of-truth files per EHR:
- `ehrs/{ehr}/CapabilityStatement.json` — verbatim from `<sandbox>/metadata`. Authoritative spec source. Never edited.
- `ehrs/{ehr}/overlay.json` — only the things CapStmt cannot tell us (Phase B-only data: per-element value-set deviations, OperationOutcome shapes, pagination behavior, auth specifics).

The pipeline reads both at render time via `tools.synthesize.synthesize_ehr(ehr)`.

---

## Phase A — Fetch & archive the CapabilityStatement (10 minutes per EHR)

**Goal:** capture the EHR's authoritative spec claim — verbatim, dated, archived.

### A.1 — Run the fetcher

```bash
python -m tools.fetch_capability epic
python -m tools.fetch_capability cerner --base-url=https://fhir-open.cerner.com/r4/<tenant-id>
python -m tools.fetch_capability meditech --base-url=<sandbox URL via developer portal>
```

Sandbox `/metadata` URLs (no auth required for any of these as of 2026-04):

| EHR | Base URL |
|---|---|
| Epic | `https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4` |
| Cerner / Oracle Health | `https://fhir-open.cerner.com/r4/<tenant-id>` (need a published Oracle Health sandbox tenant id; default = `ec2458f2-1e24-41c8-b71b-0e701af7583d`) |
| MEDITECH | sandbox URL via developer portal |
| eCW | sandbox URL via developer portal |
| Veradigm / Allscripts | sandbox URL via developer portal |
| NextGen | sandbox URL via developer portal |

If a vendor URL changes, update `ehrs/{ehr}/overlay.json:capability_statement_url` and re-run the fetcher.

### A.2 — Inspect the CapStmt

The fetcher prints `software`, `fhirVersion`, and resource count. Sanity-check:
- `resourceType: "CapabilityStatement"` (not OperationOutcome / not HTML)
- `fhirVersion: "4.0.x"` (we don't yet support DSTU2 EHRs in v1)
- Resource count > 0
- `rest[0].security` declares OAuth or SMART-on-FHIR (or `mode: server` and explicit `Basic`)

If the CapStmt is malformed or missing fields, it's still authoritative — capture it as-is. The fact that vendor X's CapStmt is broken is itself a finding.

### A.3 — Update overlay metadata

The fetcher auto-updates `overlay.json:capability_statement_fetched_date`, `capability_statement_url`, and `ehr_version_validated` (from `software.name + software.version`).

You SHOULD manually verify and refine `overlay.json:ehr_version_validated` if the CapStmt's `software.version` is missing or vague (e.g., Cerner Open returns no software version — set the overlay's value manually like "Oracle Health Millennium (sandbox tenant ec2458f2)" with a note about the source).

### A.4 — Output

`ehrs/{ehr}/CapabilityStatement.json` (committed verbatim) + `tests/golden/{ehr}/CapabilityStatement-{date}.json` (date-stamped archive). The dated archive is the audit trail; quarterly re-runs add new dated copies, the primary `CapabilityStatement.json` always reflects the most recent fetch.

This produces a complete claimed-support view by itself: which US Core profiles are claimed, which interactions per resource, which search params per resource, declared auth services, declared OAuth URIs, FHIR version, software version. Run `python -m tools.synthesize {ehr}` to see.

---

## Phase B — Overlay measurement (1-3 days per EHR, depending on depth)

**Goal:** populate `overlay.json` with the things the CapStmt does not declare. ONLY measure things CapStmt cannot tell you — don't re-record what's already in CapStmt.

What CapStmt declares (don't repeat in overlay):
- Which profiles claimed supported (`rest[0].resource[].supportedProfile`)
- Which searches accepted per resource (`rest[0].resource[].searchParam`)
- Which interactions per resource (`rest[0].resource[].interaction[].code`)
- Auth service types (`rest[0].security.service`)
- SMART OAuth URIs (`rest[0].security.extension`)
- FHIR version, software version, generation date

What CapStmt does NOT declare (overlay is the only source):
- **Per-element value-set deviations** — e.g., "Epic emits Patient.name.use=official only, never usual" is NOT in CapStmt; CapStmt only says "supports us-core-patient profile"
- **OperationOutcome shape** — what error envelope vendor returns, what `code` system, what extensions
- **Pagination behavior** — default _count, max _count, link relations actually emitted, opaque vs parseable next URLs
- **Auth specifics** — JWT signing alg, JWKS hosting requirement, scope format (v1 vs v2)
- **Silent search-param failures** — params accepted by HTTP but with no filter effect (the worst kind of bug)
- **Token search restrictions** — e.g., "category accepts laboratory and vital-signs but rejects social-history with OperationOutcome"

### B.1 — Auth specifics pass

Required: SMART app registration (one-time per EHR) → client_id + JWKS → walk the OAuth flow.

For each EHR:
1. Register a SMART app at the EHR's developer portal (Epic: `fhir.epic.com/Developer/Apps`; Cerner: `code.oracle.com/health`; MEDITECH: developer portal)
2. Generate JWKS keypair (RS384 for Epic, RS256 for most others)
3. Run `client_credentials` flow → record JWT alg accepted, scope format used, token lifetime
4. Run `authorization_code` flow if supported
5. Update `overlay.json:auth_overlay` with: `client_credentials_jwt_alg`, `jwks_required`, `jwks_hosted_or_embedded`, `scopes_format`, `client_registration_url`, `notes`. Cite each as `verified_via: {ehr}_public_sandbox` with `verified_query` = the exact OAuth flow steps run.

### B.2 — OperationOutcome pass

Once you have a token, trigger known errors:
- `GET /Patient/does-not-exist-12345` → record OperationOutcome JSON
- `GET /Observation?bogus_param=x` → record OperationOutcome JSON
- `GET /Patient` (no params, may require search) → record
- `GET /Observation?category=invalid-category` → record
- Bad token → record

For each, capture into `overlay.json:operation_outcome_overlay.examples[]`:
- `trigger`: HTTP verb + URL + headers
- `response_body`: verbatim OperationOutcome JSON

Set `shape` to `fhir-standard` if responses match HL7 spec exactly, `fhir-standard-with-vendor-extensions` if extensions are present, `vendor-custom` if the envelope is non-FHIR.

### B.3 — Pagination pass

`GET /Observation?patient={canonical_patient_id}&category=laboratory` (likely returns many results). Walk pagination to the end. Record:
- Default `_count` (returned without explicit param)
- `_count=1`, `_count=max`, `_count=max+1` behavior (does it cap silently? error?)
- Link relations present (`self`, `next`, `previous`, `first`, `last`)
- Whether `next` URLs are stable (re-issue same query at later time → same next token?) or volatile

Update `overlay.json:pagination_overlay`.

### B.4 — Per-element value-set deviation pass

For each US Core P0 profile claimed in CapStmt, fetch the canonical sandbox patient's instances (Patient, Observations, Conditions, Encounters, etc.) and compare element values to US Core spec.

For each element where the EHR returns a value that's a *subset* of what US Core allows (or adds an extension US Core didn't define, or returns wrong cardinality), add an entry to `overlay.json:element_deviations`:

```json
{
  "profile_id": "us-core-patient",
  "path": "Patient.name.use",
  "deviation_category": "value-set-narrowed",
  "expected_per_us_core": "official | usual | old | nickname | anonymous | maiden",
  "observed_in_ehr": "official",
  "deviation": "Epic emits 'official' only; clients filtering by use='usual' will return zero results",
  "extension_urls_observed": [],
  "vendor_workaround": "Treat use='official' as the canonical name; do not depend on use='usual'",
  "notes": "",
  "verification": {
    "source_url": "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/Patient/eq081-VQEgP8drUUqCWzHfw3",
    "source_quote": "{ \"name\": [{ \"use\": \"official\", ...}] }",
    "verified_via": "epic_public_sandbox",
    "verified_query": "GET https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/Patient/eq081-VQEgP8drUUqCWzHfw3 Accept: application/fhir+json Authorization: Bearer ...",
    "verified_date": "2026-05-15"
  }
}
```

Save the raw response under `tests/golden/{ehr}/{Resource}-{id}-{date}.json` so the deviation row can be re-derived by anyone running validation.

### B.5 — Search-param-behavior pass

For each P0 resource, exercise edge cases of the search params CapStmt declares supported:
- Token searches with values OUTSIDE the documented value set (does the EHR return 400? empty bundle? all results?)
- Reference searches with non-canonical reference formats (does it accept `Patient/123`, `123`, both?)
- Date searches with various precisions (`date=2024-01`, `date=2024-01-15T00:00:00Z`, etc.)
- Searches with multiple values (`status=active,inactive`)

Where behavior diverges from FHIR spec or vendor docs, add to `overlay.json:search_param_observations[ResourceType].params_with_value_restrictions`.

Where a param is accepted (200 OK, results returned) but has no filtering effect, add to `params_silently_ignored` — these are the most insidious bugs.

### B.6 — Output

Updated `overlay.json` with verified Phase B data. Golden fixtures committed under `tests/golden/{ehr}/`. Run `python -m tools.validate {ehr}` to confirm:
- Schema valid
- No STUB cells in `element_deviations`
- Synthesis succeeds

---

## Phase C — Differential analysis (continuous, not a discrete pass)

In the no-intermediate-schema architecture, "Phase C" isn't a separate run — it's what `tools.synthesize` does at render time. The synthesizer:
1. Loads `CapabilityStatement.json` (claimed)
2. Loads `overlay.json` (measured)
3. Joins both against `us-core/us-core-6.1-baseline.json` (the spec baseline)
4. Produces per-profile, per-element view that downstream consumers render

Validators check that synthesis produces no errors (`tools.validate` includes a synthesis sanity check).

---

## Phase D — Cross-vendor synthesis (on demand)

`python -m tools.synthesize --all --json > /tmp/all-ehrs.json` produces the joined view across all `ehrs/*/`. Downstream consumers (the SEO renderer, the `/compare` tool, the mock.health mutate-on-read wrapper) consume this directly.

For comparison-rendering — "this US Core element across all 3 EHRs" — pivot the synthesized view: for each US Core profile, walk every EHR's profile entry and show their CapStmt-claimed support + overlay-observed deviations side-by-side. (Tooling for this lives in `tools/render_html.py` — TODO.)

---

## Phase E — Vendor confirmation (asynchronous, ongoing)

For each EHR, DM the vendor's dev-relations team:
- Epic: `epicfhir@epic.com` + LinkedIn DM their FHIR product manager
- Cerner / Oracle Health: developer portal contact
- MEDITECH: developer portal contact
- (Smaller vendors: developer portal contact)

### E.1 — The DM template (Marat-style)

```
Hi <name> — I built a structured map of how <EHR> implements US Core 6.1 / USCDI v3,
fetched from your published CapabilityStatement and verified against your public
sandbox. The repo is at <github URL>. Your <EHR> entry is at <direct file URL>.

Given how much detail this captures, I'd love to make sure we got it right. The
overlay.json file is the only place subjective claims live — your CapStmt is
ingested verbatim. Open to PRs against the overlay if you spot anything wrong;
source citation required, otherwise low-friction.

— nate
mock.health
```

### E.2 — PR review process

Vendor PRs land with `verified_via: vendor_pr` and authoritative weight on overlay claims (CapStmt is the vendor's own document so they "owned" it from the start). Maintainer reviews:
- Source citation present and resolves
- Verified date populated
- Diff against existing overlay is sensible
- Golden fixture updated if behavior is verifiable in sandbox

Merge with attribution in commit message.

### E.3 — Trust hierarchy when sources disagree

If vendor PR claims X but sandbox measures Y:
- Both preserved. Overlay's `notes` captures the vendor's claim; primary `support`/`deviation` reflects measurement.
- This honesty is the moat — vendor disagreement is itself a finding, not a problem to suppress.

---

## Phase F — Production-fleet harvest (v2, added 2026-04-27)

**Goal:** measure what's actually deployed across the vendor's customer fleet, not just the reference sandbox. Per-customer drift, software-version distribution, modal CapStmt-shape clusters, per-profile support rate at fleet scale.

This is anonymous: every customer's `/metadata` and `/.well-known/smart-configuration` are spec-mandated public per FHIR R4 §C.0.0 and SMART App Launch STU 2.2. No app registration, no auth, no Greenfield/EAP gating affects this layer.

### F.1 — Fetch the brands bundle

```bash
python -m tools.fetch_brands epic        # https://open.epic.com/Endpoints/R4
python -m tools.fetch_brands cerner      # Oracle Health GitHub (provider tier)
python -m tools.fetch_brands meditech    # https://fhir-apis.meditech.com/v1/brands
python -m tools.fetch_brands --all       # all known vendors
```

Each vendor publishes their brands & endpoints registry differently — Epic ships a `Bundle` of `Endpoint` only at `open.epic.com/Endpoints/R4`; MEDITECH publishes a SMART STU 2.2-conformant `Bundle` of `Organization` + `Endpoint` at `fhir-apis.meditech.com/v1/brands`; Oracle Health publishes via GitHub Pages in `oracle-samples/ignite-endpoints/`. The fetcher normalizes the storage location (`tests/golden/cross-vendor/{stem}-{date}.json`) but not the format.

### F.2 — Harvest CapStmt + SMART config per endpoint

```bash
python -m tools.harvest_production_capstmts epic
python -m tools.harvest_production_capstmts cerner
python -m tools.harvest_production_capstmts meditech
```

For every Endpoint resource in the bundle, the harvester pulls:
- `<address>/metadata` → `capability-statement.json` or `capability-statement.fetch-error.json`
- `<address>/.well-known/smart-configuration` → `smart-configuration.json` or `.fetch-error.json`

Politeness: `ThreadPoolExecutor(max_workers=20)`, per-host concurrency cap of 2 (multiple hospitals share regional infrastructure), 30s timeout, single retry-on-429 honoring `Retry-After`. Honest User-Agent: `mockhealth-map/0.1 (+https://mock.health; nate@mock.health) production-fleet-harvester` so hospital security teams who notice the traffic in logs can find the project.

Each endpoint gets its own subdir under `tests/golden/production-fleet/{ehr}/{captured_date}/{slug}/`. Re-running with `--force` refreshes; without `--force`, existing per-endpoint files are skipped (resumable).

### F.3 — Cluster + summarize

```bash
python -m tools.analyze_fleet_drift epic
python -m tools.analyze_fleet_drift cerner
python -m tools.analyze_fleet_drift meditech
```

For each vendor:
1. Hash each CapStmt's *shape* — sorted JSON of `rest[0].resource[*].{type, supportedProfile, searchParam.name list, interaction.code list, security.service codes}`. Two CapStmts with the same shape advertise an identical FHIR API surface.
2. Cluster by hash, label largest cluster A, next B, etc.
3. Tally `software.{name + version}`, `fhirVersion`, smart-config grant types/capabilities/scope buckets.
4. For every US Core 6.1 baseline profile, count how many reachable customers list it in any `supportedProfile` → fleet support rate.
5. Identify outliers: customers in non-modal clusters; record what they're missing or adding vs. modal.

Output: `ehrs/{ehr}/production_fleet.json` (~50KB, schema in `schema/production_fleet.schema.json`). Synthesizer + render_html consume it automatically — no further wiring needed.

### F.4 — Re-verification cadence (Phase F)

**Quarterly.** Brands bundles update infrequently and deployed customer software versions update on rolling vendor cycles. Daily refetching would be rude and produce no signal. Re-run `tools.fetch_brands {vendor}` then `harvest_production_capstmts {vendor}` then `analyze_fleet_drift {vendor}`. The harvester writes to a new `{captured_date}/` subdirectory; old snapshots stay for diff history.

### F.5 — Output

- `ehrs/{ehr}/production_fleet.json` — committed, ~50KB rolled-up summary
- `tests/golden/production-fleet/{ehr}/{captured_date}/` — per-endpoint raw evidence (CapStmts and SMART configs as fetched, error sidecars where unreachable)
- HTML pages under `reports/{quarter}/html/{ehr}/fleet/` once `render_html` runs

---

## Re-verification cadence

**Quarterly:** re-run `tools.fetch_capability` for all EHRs. The new CapStmt overwrites the primary `ehrs/{ehr}/CapabilityStatement.json`; the dated copy lands in `tests/golden/{ehr}/`. `git diff` on the primary file shows the spec delta. Re-run Phase B.4 (per-element deviation pass) only for resources where CapStmt diff or sandbox behavior changed. Phase F (production fleet) re-runs on the same quarterly cadence.

**Ad-hoc:** re-run for a single EHR when (a) vendor releases a major version, (b) customer reports a discrepancy, (c) an overlay row is older than 18 months (validator emits STALE warning).

---

## Tooling

| Tool | Purpose |
|---|---|
| `python -m tools.fetch_capability {ehr}` | Phase A — fetch CapStmt verbatim, archive dated copy, bump overlay metadata |
| `python -m tools.fetch_us_core_ig` | Phase 0 — download US Core IG package from packages.fhir.org into `us-core/ig-package/` |
| `python -m tools.build_baseline_from_ig` | Phase 0 — extract per-profile must_support + bindings + searches into `us-core/us-core-6.1-baseline.json` (replaces hand-curated baseline) |
| `python -m tools.enumerate_sandbox_patients {ehr}` | Phase 1 — characterize sandbox patient catalog and pick ≥3 patients per P0 probe |
| `python -m tools.measure_phase_b {ehr}` | Phase 2 — multi-patient four-axis sweep; writes overlay.element_deviations + multi_patient_coverage |
| `python -m tools.probe_search_bulk_refs {ehr}` | Phase 3 — search-param coverage, $export probe, reference resolution |
| `python -m tools.upload_us_core_to_hapi` | Phase 4 — upload IG package to a running HAPI FHIR server (DEFAULT tenant) |
| `python -m tools.cross_validate_with_hapi {ehr}` | Phase 4 — POST `$validate?profile=us-core-...` for each fixture; record agreement/disagreement |
| `python -m tools.run_inferno_us_core {ehr}` | Phase 5 — parse exported Inferno results JSON; deferred-note when not yet run |
| `python -m tools.probe_transport {ehr}` | Phase 6 — invalid-bearer + concurrent tokens + content-type negotiation + rate-limit probes |
| `python -m tools.synthesize {ehr}` | Read CapStmt + overlay, produce Map view (human-readable or `--json`) |
| `python -m tools.synthesize {ehr} --write-matrix=...` | Emit the conformance matrix JSON |
| `python -m tools.render_html {ehr}` | Generate SEO browseable HTML pages under `reports/{quarter}/html/{ehr}/` |
| `python -m tools.validate [{ehr}]` | Full validation: CapStmt sanity + overlay schema + synthesis succeeds + staleness check |

All Python 3.11+. Deps in `requirements.txt`. Run from repo root.

---

## v1 four-axis conformance methodology (added 2026-04-26)

The original Phase B above measured presence only — does each must-support path resolve
in the response? v1 extends to four orthogonal axes per (resource, must-support entry):

1. **Presence** — path resolves to ≥1 value (`tools/conformance/presence.py`)
2. **Cardinality** — count of values matches `min..max` (`tools/conformance/cardinality.py`)
3. **Value set** — coded values come from the bound VS when binding strength is `required`;
   observed codes are recorded for `extensible`/`preferred` to surface vendor narrowing
   (`tools/conformance/value_set.py`). LOINC/SNOMED subsets that can't be locally expanded
   are deferred to HAPI's `$validate-code` (Phase 4), recorded as `value-set-unverified-locally`.
4. **Format** — primitive values match FHIR R4 primitive-type regexes (`tools/conformance/format.py`).
   Skipped when the type list mixes primitive-with-regex types and primitive-without-regex
   (e.g. `boolean | dateTime`).

The orchestrator `tools/conformance/__init__.py:analyze()` runs all four axes and returns
a list of finding dicts. Each finding lands in `overlay.element_deviations` after
multi-patient aggregation:

- **Presence axis** dichotomy is collapsed into one row per (profile, path) annotated
  `vendor-implementation-gap` (every patient missing), `patient-data-gap` (some have it,
  some don't), or `matches-everywhere`.
- **Other axes** (`value-set-mismatch`, `cardinality-min-violated`, `format-violation`,
  etc.) get their own row each, with the patient list recorded in `multi_patient_evidence`.

This distinguishes "Epic doesn't emit X" from "this patient lacks X" — the central
epistemic gap the v0.1 Map could not close.

---

## End-to-end pipeline run

```bash
python -m tools.fetch_capability epic                         # Phase A — verbatim CapStmt
python -m tools.fetch_us_core_ig                              # Phase 0a — IG package
python -m tools.build_baseline_from_ig                        # Phase 0b — baseline from IG
python -m tools.enumerate_sandbox_patients epic               # Phase 1 — patient catalog
python -m tools.measure_phase_b epic                          # Phase 2 — multi-patient 4-axis
python -m tools.probe_search_bulk_refs epic                   # Phase 3 — search/bulk/refs
# Phase 4 requires HAPI with the US Core 6.1.0 IG preloaded — see tools/hapi/docker-compose.example.yml
docker compose -f tools/hapi/docker-compose.example.yml up -d
python -m tools.upload_us_core_to_hapi
python -m tools.cross_validate_with_hapi epic
# Phase 5 (optional):
python -m tools.run_inferno_us_core epic                       # writes deferred note unless inferno-results.json present
python -m tools.probe_transport epic                          # Phase 6 — transport
python -m tools.synthesize epic --write-matrix=reports/2026-q2/epic-conformance-matrix.json  # Phase 7
python -m tools.render_html epic                              # Phase 7 — HTML pages
python -m tools.validate epic                                 # Phase 8 — validate
```

---

## Reproducibility

Anyone (vendor, customer, contributor) should be able to:
1. Clone the repo
2. Run `python -m tools.fetch_capability epic` (re-fetches Epic CapStmt; produces a diff against committed copy if Epic changed anything)
3. Run `python -m tools.synthesize epic` (gets the Map view)
4. Run `python -m tools.validate epic` (passes)

If any step fails, the methodology is broken. Tests in `tests/test_methodology.py` (TODO) enforce this against committed golden CapStmts.
