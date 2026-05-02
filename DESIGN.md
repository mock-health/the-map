# DESIGN — How to add an EHR to the Map

This is the design doc for the multi-EHR Map architecture. It says what's shared,
what's per-EHR, where the seams are, and how to add a new vendor without breaking
the Epic build. Read this before adding Cerner, MEDITECH, eCW, NextGen,
Veradigm, or any other EHR.

For "what we measured for Epic" see `CHANGELOG.md` and `reports/2026-q2/epic-summary.md`.
For the runbook see `METHODOLOGY.md`.

---

## The two-axis model

Every EHR is described by **two source-of-truth files** + a **catalog of measurement
artifacts**. Nothing else is hand-curated; everything else is derived.

```
ehrs/{ehr}/
├── CapabilityStatement.json     # verbatim from sandbox /metadata; never edited
└── overlay.json                 # only what CapStmt cannot tell us
```

```
us-core/
├── ig-package/                  # downloaded from packages.fhir.org
└── us-core-6.1-baseline.json    # auto-extracted from ig-package; never hand-edited
```

```
ehrs/{ehr}/                      # measurement artifacts (auto-generated)
├── sandbox_patients.json        # patient-coverage matrix from enumerate_sandbox_patients
└── p0_patient_selection.json    # ≥3 patients per probe selection
tests/golden/{ehr}/phase-b-{date}/   # raw responses + sweep summary + HAPI validations
reports/{quarter}/                   # synthesized matrix + HTML
```

The synthesizer joins all three at render time. **No intermediate file** is the
core architectural decision — see `PLAN.md` "Architecture decision: no intermediate
schema (2026-04-26)" for the rationale.

---

## The seams: what's shared vs. what's per-EHR

Adding a new EHR means filling four small per-EHR slots; everything else is shared.

### Slot 1: OAuth config (`tools/auth_flows/__init__.py:EHR_CONFIG`)

The handshake module has a single dict keyed by EHR name. To add a new EHR (e.g. `<vendor>`):

```python
EHR_CONFIG = {
    "epic": { ... },
    "<vendor>": {
        "client_id_var": "<VENDOR>_NONPROD_CLIENT_ID",
        "private_key_var": "<VENDOR>_NONPROD_PRIVATE_KEY_PATH",  # may be unused if not RS-signed
        "kid_var": "<VENDOR>_NONPROD_JWKS_KID",                  # may be unused
        "token_url_var": "<VENDOR>_NONPROD_TOKEN_URL",
        "fhir_base_var": "<VENDOR>_NONPROD_FHIR_BASE",
        "default_scope": "system/Patient.read system/Observation.read ...",
        "canonical_patient_id": "<vendor's documented test patient>",
    },
}
```

If the EHR's auth flow is fundamentally different (e.g. authorization_code
with browser instead of client_credentials; Cerner is RS256 not RS384), add it
to `tools/auth_flows/{flow}.py` and have the `flow:` key in `EHR_CONFIG[ehr]`
dispatch to the right module.
Override slots in the config (`fhir_base_override`, `token_url_override`,
`authorize_url_override`, `aud_strip_suffix`, `use_pkce`, `redirect_uri`,
`callback_port`) cover most per-vendor wrinkles without forking the auth flow.

For hosted (non-localhost) redirect_uri flows, use `tools.begin_consent {ehr}` +
`tools.paste_callback {ehr} --url='<paste>'` instead of the blocking interactive
prompt — `state` and PKCE `code_verifier` are persisted to `.tokens/{ehr}.pending-*`
files so the two CLI invocations can be separated by the human's browser round-trip.

### Slot 2: Known patient roster (`tools/enumerate_sandbox_patients.py:KNOWN_PATIENT_ROSTERS`)

Some sandboxes refuse unfiltered Patient search (Epic does — see Phase B finding
"Patient search refuses to enumerate without demographics-or-_id"). For those,
hand-curate an ID list:

```python
KNOWN_PATIENT_ROSTERS = {
    "epic": ["eq081-VQEgP8drUUqCWzHfw3", ...],
    "cerner": ["12724066", ...],
}
```

If the sandbox allows blanket `Patient?_count=50`, the walker harvests
automatically; the roster can be empty. The two strategies coexist (see
`hydrate_known_roster` + `walk_patients` in the same file).

### Slot 3: P0 probe templates (`tools/measure_phase_b.py:P0_PROBES`)

P0 probes are mostly identical across EHRs because US Core 6.1 dictates the FHIR
query shape. But some vendors require additional filters or category codings. For
example, Epic's lab probe is `Observation?patient={id}&category=laboratory`;
Cerner's may need `&_count=200` to get reasonable defaults. Override per-EHR:

```python
P0_PROBES = {
    "Observation-lab": {
        "method": "GET",
        "path_template": "Observation?patient={patient_id}&category=laboratory",
        "us_core_profiles": ["us-core-observation-lab"],
        "per_ehr_overrides": {
            "cerner": "Observation?patient={patient_id}&category=laboratory&_count=200",
        },
    },
}
```

### Slot 4: Editorial findings (`ehrs/{ehr}/overlay.json:phase_b_findings.headlines[]`)

The 22 Epic headlines are hand-written editorial findings that summarize the
measurement output. Each new EHR will have its own analog set. **These are NOT
auto-generated** — they're the human-curated story the Map tells about each
vendor. Each headline carries its own source citation per the iron rule.

---

## The pipeline (8 phases × 1 driver per phase)

Each phase is a standalone tool. Each tool reads its inputs from disk, writes its
outputs to disk, and validates against the schema. **No tool depends on another's
in-process state.** This means you can run a single phase, inspect the output,
re-run with different config, etc., without re-running upstream phases.

| Phase | Tool | Input | Output |
|---|---|---|---|
| A | `tools.fetch_capability {ehr}` | sandbox `/metadata` | `ehrs/{ehr}/CapabilityStatement.json` + dated archive |
| 0a | `tools.fetch_us_core_ig` | packages.fhir.org | `us-core/ig-package/` |
| 0b | `tools.build_baseline_from_ig` | `us-core/ig-package/` | `us-core/us-core-6.1-baseline.json` |
| 1 | `tools.enumerate_sandbox_patients {ehr}` | OAuth + roster | `ehrs/{ehr}/{sandbox_patients,p0_patient_selection}.json` |
| 2 | `tools.measure_phase_b {ehr}` | OAuth + selection + baseline | overlay.element_deviations + multi_patient_coverage + golden fixtures |
| 3 | `tools.probe_search_bulk_refs {ehr}` | OAuth + CapStmt + selection | overlay.search_param_observations + bulk_export_overlay + reference_resolution_overlay |
| 4a | `tools.upload_us_core_to_hapi` | `us-core/ig-package/` | HAPI's database |
| 4b | `tools.cross_validate_with_hapi {ehr}` | golden fixtures + HAPI | overlay.cross_validation_disagreements + paired `*-hapi-validation.json` files |
| 5 | `tools.run_inferno_us_core {ehr}` | optional Inferno results JSON | overlay.inferno_cross_validation |
| 6 | `tools.probe_transport {ehr}` | OAuth | overlay.transport_findings |
| 7a | `tools.synthesize {ehr} --write-matrix=...` | CapStmt + overlay + baseline | conformance matrix JSON |
| 7b | `tools.render_html {ehr}` | conformance matrix | HTML page tree |
| 8 | `tools.validate [{ehr}]` | overlay schema + synthesis sanity | OK/FAIL |

Phases 0a/0b are EHR-independent — run once, used by every EHR.

Phase 4 requires a HAPI server. The `--skip-if-down` flag on
`cross_validate_with_hapi` writes a deferred note instead of failing, so the
pipeline survives without HAPI.

Phase 5 (Inferno) is always deferred-by-default; the runner reads
`inferno-results.json` if present, otherwise stamps a deferred note.

---

## The conformance analyzer (`tools/conformance/`)

Four orthogonal axis modules. Each is independently testable and replaceable.

```
tools/conformance/
├── __init__.py        # analyze() orchestrator
├── presence.py        # axis 1: path resolves to ≥1 value
├── cardinality.py     # axis 2: count matches min..max
├── value_set.py       # axis 3: required-binding membership
└── format.py          # axis 4: primitive regex
```

### Adding a new axis

If we want a new conformance check (e.g., "extension URL is a known US Core
extension"), add a new axis module:

1. Define `{axis}_finding(profile_id, must_support, resource, ehr, today, ...) → dict | None`.
2. Wire it into `analyze()` in `__init__.py` next to the existing axes.
3. Add the new `deviation_category` value to `schema/overlay.schema.json` enum.
4. The aggregator in `measure_phase_b.aggregate_multi_patient` already dispatches
   non-presence categories generically — no changes there.

### Replacing an axis

Each axis is a pure function over `(resource, must_support_entry)`. Swap the
implementation without touching the orchestrator. Useful when the value-set axis
gets upgraded from local-expansion-only to "ask HAPI's $validate-code for LOINC
membership."

### The presence-axis is special

Because presence is a binary across patients, the multi-patient aggregator treats
it specially: rows collapse to a single `vendor-implementation-gap` /
`patient-data-gap` / `matches-everywhere` summary instead of one row per patient.
Other axes emit one row per category with patient list. See
`measure_phase_b.aggregate_multi_patient` for the dichotomy.

---

## The baseline (`us-core/us-core-6.1-baseline.json`)

The baseline is the **expected** side. Every conformance check is against the
baseline; vendor responses are the **observed** side.

The baseline is **never hand-edited**. It is generated by
`tools.build_baseline_from_ig` from the official US Core IG package. The build
script:

1. Walks every `StructureDefinition-us-core-*.json` with
   `kind=resource && type!=Extension && derivation=constraint`.
2. For each profile, extracts every snapshot element with
   `mustSupport=true` OR carrying the `uscdi-requirement` extension flag.
3. Renders each element's path in analyzer-walkable form (extension slices become
   `Resource.extension(slug)`; non-extension slices keep the `:slice-name` marker).
4. Captures cardinality, type, target_profiles, binding (strength + valueSet URL +
   id), slice_name, and the source SD element.id for auditability.
5. Pulls per-resource search params + interactions from `CapabilityStatement-us-core-server.json`.
6. Writes `us-core/us-core-6.1-baseline.json` with priority + USCDI coverage tags.

When US Core ships 7.x:

```bash
.venv/bin/python -m tools.fetch_us_core_ig --version=7.0.0 --force
.venv/bin/python -m tools.build_baseline_from_ig --version=7.0.0
.venv/bin/python -m tools.validate
```

Diff `git diff us-core/us-core-6.1-baseline.json` to see what changed; expect
elements added/removed/re-bound. Re-run Phase 2 (multi-patient sweep) for any
EHR whose findings depend on the changed elements.

The two priority/coverage dicts (`PRIORITY`, `USCDI_COVERAGE`) inside
`build_baseline_from_ig.py` are the **only** hand-curated cross-reference. They
encode editorial-grade information that the IG package itself does not carry
(P0/P1/P2 launch priority, USCDI v3 data class mapping). When a profile is
renamed across IG versions, update those two dicts.

---

## The schema (`schema/overlay.schema.json`)

The schema is **permissive on top-level fields**, **strict on cited claims**.
That's the right tradeoff for a Map that grows new measurement axes per-EHR
without breaking the validator.

- `additionalProperties: true` (default) on top-level overlay — new probe sections
  can land without a schema update.
- `additionalProperties` on most nested objects so vendor-specific shapes can fit.
- Strict required-field enforcement on `verification` blocks — every cited claim
  MUST have `source_url`, `source_quote`, `verified_via`, `verified_date`. This is
  the iron rule made enforceable.
- Strict `deviation_category` enum so categories don't accidentally proliferate.
  When you add a new axis you MUST update the enum (see "Adding a new axis").

If a new probe needs more structured validation, add a `properties` block but
keep `additionalProperties: true` — the lesson from Epic was that a half-defined
shape costs more than no shape (existing data gets locked into a shape that
turns out to be wrong).

---

## The renderer (`tools/render_html.py`)

The HTML renderer reads the conformance matrix from `tools.synthesize` and
produces:

```
reports/{quarter}/html/
├── index.html                        # multi-EHR landing
└── {ehr}/
    ├── index.html                    # per-EHR landing
    └── {profile_id}/
        ├── index.html                # per-profile landing
        └── {element_path}.html       # per-element page
```

Element-page URL structure mirrors `mock.health/map/{ehr}/{resource}/{element}`,
so the static site can be deployed under that subdomain without rewrites.

The CSS is embedded inline (small, self-contained, no build step). Adding a new
EHR adds `{ehr}/index.html` + the per-profile + per-element subtree
automatically; no renderer changes.

To add a comparison view ("this element across all 3 EHRs"), add a new page
template that pivots the matrix: walk `synthesize_all()`, group by
`(profile, path)`, render side-by-side. The synthesizer already exposes the
right shape; the renderer just needs the new template.

---

## Validation (`tools.validate`)

The validator is the gate. It runs:

1. CapStmt sanity: resourceType + fhirVersion + non-empty resource list.
2. Overlay schema validation against `schema/overlay.schema.json`.
3. `overlay.ehr` matches its parent directory name.
4. No `STUB` source_quote in element_deviations (no row without evidence).
5. Synthesis sanity: `tools.synthesize.synthesize_ehr({ehr})` runs without error.
6. Staleness check: `verified_date` older than 18 months → warn.
7. Optional: URL reachability with `--check-urls`.

When you add an EHR, the validator catches structural errors immediately.
Run `python -m tools.validate {ehr}` between every phase. If validation fails,
stop and fix before continuing — never let an unvalidated overlay propagate.

---

## Adding a new EHR (concrete walkthrough)

If the new vendor's auth is authorization_code with a browser flow instead of
client_credentials, that's the single hardest delta from Epic.

1. **Phase A** — `python -m tools.fetch_capability <vendor>`. CapStmt drops in
   verbatim; no code changes needed.

2. **Auth** — If the flow is browser-based, use `tools/auth_flows/auth_code.py`
   (open localhost callback, capture authorization code, exchange for token).
   Set the `flow` key in `EHR_CONFIG['<vendor>']` to dispatch to the right
   module.

3. **Slot 2** — If the vendor publishes a documented test patient list, add
   their IDs to `KNOWN_PATIENT_ROSTERS["<vendor>"]`.

4. **Phase 1** — Run `python -m tools.enumerate_sandbox_patients <vendor>`. May
   need a `--no-walk` flag if the sandbox gates blanket Patient search.

5. **Phase 2** — Run the multi-patient sweep. The conformance analyzer needs no
   changes (US Core conformance is the same regardless of vendor).

6. **Phase 3** — Run search/bulk/refs. May discover the vendor uses different
   search-param defaults; record as findings.

7. **Phase 4 / 5 / 6 / 7 / 8** — All EHR-agnostic; just run them with
   `<vendor>` as the arg.

8. **Editorial findings** — Hand-write `ehrs/<vendor>/overlay.json:phase_b_findings.headlines`
   based on the sweep output. Use `epic-summary.md` as a model.

Total per-EHR effort estimate: 1-2 days for a vendor on auth_code (the long pole),
0.5-1 day for any subsequent EHR that's also client_credentials or auth_code.

---

## What NOT to do when adding an EHR

- **Don't fork tools per-EHR.** Every tool takes `{ehr}` as the first positional
  argument; per-vendor logic goes inside the tool keyed by the EHR name.
- **Don't hand-edit the baseline.** When a vendor implements something the IG
  doesn't flag mustSupport, that's a finding (record it as `extra-extension`),
  not a baseline change.
- **Don't suppress disagreements.** When HAPI flags an error our analyzer
  missed, that's a Map finding too. Surface it; don't silently widen the
  analyzer to suppress the disagreement.
- **Don't merge phase-b directories across dates.** Each sweep writes to
  `phase-b-{today}/`. Quarterly diff is the change-history mechanism. Older
  snapshots are the audit trail.
- **Don't skip the iron rule.** No row without a citation. The iron rule applies
  to vendor 3 the same as vendor 1. Validators enforce it; reviewers reinforce it.

---

## Open architectural questions

These are unresolved as of v1; resolve them before vendor 3.

1. **Multi-EHR comparison view shape.** The pivoted "this element across all 3
   EHRs" page template doesn't exist yet. Decide JSON shape + URL structure
   before adding it (does it live under `reports/{quarter}/html/_compare/` or
   under `mock.health/compare`?).

2. **Vendor-extension catalog.** Should there be a `vendor_extensions.json` per
   EHR cataloging every Epic-OID code system observed? Today these are buried in
   `value-set-narrowed` deviation rows. A first-class catalog would help the
   mutate-on-read wrapper inject vendor extensions on synthetic responses.

3. **Per-EHR auth-flow plumbing.** Today `EHR_CONFIG` assumes client_credentials.
   The cleanest extension is a `flow: "client_credentials" | "authorization_code"`
   key + a dispatch table. Don't refactor speculatively — wait until a real
   browser-flow vendor lands.

4. **Cross-vendor diff monitoring.** ~~Phase A is run quarterly; what mechanism
   alerts us when an EHR's CapStmt changed materially? Today it's git diff;
   tomorrow we may want a CI job that opens a PR with the diff.~~
   **RESOLVED (v1.0):** `.github/workflows/refresh-capstmts.yml` runs Phase A
   weekly across every EHR and opens a "CapStmt drift detected" PR with the
   diff when the upstream `/metadata` response moves.

5. **Baseline version lifecycle.** When US Core 7.0 ships, the baseline rebuilds
   from the new IG package, but per-EHR overlays may still cite paths that 7.0
   removed. Decide policy: hard-fail validate (force editorial review) or
   soft-warn (let stale rows linger).

6. **HAPI as a service vs. on-demand.** Phase 4 currently requires HAPI up locally.
   For CI / nightly re-verification, decide whether to run HAPI as a hosted
   service or spin it up per-job. Either way, the `tools.cross_validate_with_hapi
   --hapi-base=...` flag already supports both.
