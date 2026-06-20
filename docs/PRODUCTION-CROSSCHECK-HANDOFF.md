# Production cross-check — handoff (UNC Health Epic, patient-mediated)

**Goal:** validate the-map's sandbox-derived Epic overlay (`ehrs/epic/overlay.json`,
all `verified_via: epic_public_sandbox`) against **one real deployment** using your
own UNC Health MyChart data. UNC sits in **`epic-cluster-A` — the modal Epic shape**
(426/554 reachable endpoints), so this n=1 signal speaks to the most common Epic
deployment, not an outlier. First non-sandbox Epic signal the Map has ever had.

This is a **one-off validation/derisk pass**. It does not onboard a new vendor.

---

## PHI rule (non-negotiable)

This is your real health record.

- Raw resources + PHI-bearing outputs live ONLY under a **gitignored** path
  (`.phi/…`, already in `.gitignore`). Never committed, never POSTed to HAPI, never
  written into `tests/golden/` (those are synthetic).
- Only the **redacted report** (`reports/production-crosscheck-…md`) and any
  **hand-de-identified** overlay rows get committed.
- A `source_quote` in an overlay row must contain structural facts only (coding
  systems, codes, cardinalities, extension URLs) — no names, MRNs, dates, addresses,
  or note text.

---

## What's already built (this session, in `the-map`)

| File | Change |
|---|---|
| `schema/overlay.schema.json` | new `verified_via` enum value `production_patient_smart` |
| `tests/fixtures/verified_via_url_patterns.json` | registered the new value (no URL constraint, like `customer_evidence`) |
| `tools/auth_flows/__init__.py` | new `epic_unc` `EHR_CONFIG` entry (UNC's real OAuth/FHIR endpoints, public PKCE client, patient scopes) |
| `tools/auth_flows/auth_code.py` | public-client support (`token_endpoint_auth_method: "none"`, optional secret) |
| `tools/pull_patient_record.py` | **new** — authenticates + pulls your record to a gitignored dir |
| `tools/crosscheck_production.py` | **new** — runs the 4-axis analyzer, diffs vs the sandbox overlay, emits a redacted report + PHI-side candidate rows |
| `.gitignore` | `.phi/` |

All tests pass (`make test` → 149 passed) and `python -m tools.validate epic` → OK.

---

## Path A — live wire pull (primary evidence)

### A1. Register a patient-facing SMART app on Epic (the one human hurdle)

On <https://fhir.epic.com> → "Build Apps":
- App type: **Patients** (patient-facing, standalone launch).
- Client: **public / no secret** (PKCE). UNC advertises `client-public` + `launch-standalone` + S256, so this works.
- Redirect URI: `https://oauth.pstmn.io/v1/callback` (matches the configured paste-callback). *Or* a `http://localhost:PORT/callback` you control — if so, edit `epic_unc`'s `redirect_uri` + set `callback_port`.
- Scopes: the US Core `patient/*.read` set (see `default_scope` in the `epic_unc` config).
- Note the **client_id**.

> Friction note: production patient-app access against a specific org can require the app to be live/approved. If this stalls, skip to **Path B** for this pass and revisit — the cross-check tool is identical, only the evidence tier differs.

```bash
export EPIC_UNC_PATIENT_CLIENT_ID=<your client_id>   # or add to .env
```

### A2. Authenticate + pull (opens browser for MyChart consent on first run)

```bash
python -m tools.pull_patient_record --ehr epic_unc --out .phi/epic_unc/wire
# add --everything to try Patient/{id}/$everything first
```

Writes raw FHIR to `.phi/epic_unc/wire/` (gitignored). The token caches to `.tokens/epic_unc.json`.

### A3. Cross-check

```bash
python -m tools.crosscheck_production --source wire \
  --in .phi/epic_unc/wire \
  --phi-out .phi/epic_unc/crosscheck \
  --report reports/production-crosscheck-epic-unc-$(date +%F).md
```

Produces:
- `reports/…md` — **redacted**, safe to commit (verdict buckets: confirmed deviations / confirmed matches / divergent / novel deviations / absent types / untested).
- `.phi/epic_unc/crosscheck/candidate_rows.json` — PHI-side, overlay-shaped rows tagged `production_patient_smart`, each with a `TODO-DEIDENTIFY` `source_quote` and a `_crosscheck_intent` (confidence-upgrade-existing / new-row / review-divergence).

---

## Path B — ehi-to-fhir (corroboration / coverage breadth)

The EHI export is the easy path (no app registration) but the FHIR **shape is
reconstructed** by the converter, not UNC's wire — so it's corroboration only and is
**never** promoted to overlay rows.

1. In MyChart: request your **EHI export** (the full TSV dump). Save it locally under `.phi/`.
2. Run Josh Mandel's converter (TypeScript/Bun): <https://github.com/jmandel/ehi-to-fhir> → produces `out/bundle.json` (+ `out-crosswalk/`).
3. Report-only cross-check:

```bash
python -m tools.crosscheck_production --source ehi \
  --in .phi/epic_unc/ehi-to-fhir/out \
  --phi-out .phi/epic_unc/crosscheck-ehi \
  --report reports/production-crosscheck-epic-unc-ehi-$(date +%F).md
```

---

## Review + (optional) append to the overlay

1. Open `.phi/epic_unc/crosscheck/candidate_rows.json` (wire tier only).
2. For each row worth keeping (`new-row`, or `confidence-upgrade-existing`):
   - Write a **de-identified** `source_quote` (structural fact only).
   - Drop the helper keys `_crosscheck_intent` / `_sandbox_categories`.
   - Scope `multi_patient_evidence` honestly to the single patient.
3. Append to `ehrs/epic/overlay.json#element_deviations`.
4. `python -m tools.validate epic` must pass (iron rule: every row sourced + dated +
   `verified_via=production_patient_smart`).
5. Commit the redacted report + the reviewed rows. **Never** commit anything under `.phi/`.

---

## Durable follow-up (kata)

This pass is one-off. The durable version — promote `epic_unc` into a first-class
config, generalize `crosscheck_production.py` into a repeatable `measure_production`
for future patient-access sources, and decide whether the overlay schema's
"OBSERVED in sandbox responses" description should broaden — is filed as a kata.
