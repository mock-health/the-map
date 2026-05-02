# The Map — empirical EHR conformance, not folklore

> **"Every instance of Epic is different"** is a conversation-stopper disguised as an insight. The variation isn't in the software — it's in the organizations running it. Transparent, structured documentation of how each instance is configured would make those differences manageable instead of mythological. ([Brendan Keeler, *A Collective Cognitive Dissonance*](https://healthapiguy.substack.com/p/a-collective-cognitive-dissonance))

This repo is that documentation. Per-vendor, per-deployment, per-element. With citations, source quotes, and verified dates. Not vibes.

> **Browse the rendered map at [mock.health/map](https://mock.health/map).** This GitHub repo is the open data, schema, and Python pipeline that produces it.

---

## Headlines (v1.0 — 2026-Q2)

Real findings from the data shipped in this commit. Every number links to the row that proves it.

- **[2,474 production endpoints harvested](https://mock.health/map/fleet)** anonymously across [Epic (481)](https://mock.health/map/epic/fleet), [Oracle Health / Cerner (1,451)](https://mock.health/map/cerner/fleet), and [MEDITECH (542)](https://mock.health/map/meditech/fleet) — three regulatory mandates make this public: [ONC HTI-1 §170.404](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-D/part-170#170.404), [FHIR R4 §C.0.0](http://hl7.org/fhir/R4/http.html#capabilities), and [SMART App Launch STU 2.2](https://hl7.org/fhir/smart-app-launch/STU2.2/brands.html).
- **[~31% of Epic customers run a non-modal CapStmt shape](https://mock.health/map/epic/fleet#clusters).** 9 distinct shape clusters across 481 endpoints; the modal cluster covers 300 endpoints. The other 181 are advertising a different FHIR API surface than Epic's vendor sandbox suggests.
- **[189 element deviations recorded for Epic](https://mock.health/map/epic)** across 13 P0 US Core profiles in a 7-patient × 4-axis (presence + cardinality + value-set + format) sweep.
- **[HAPI agreed with our analyzer 31/38 (82%)](https://mock.health/map/epic#cross-validation)** on Epic conformance findings. The 7 disagreements are documented per-row — terminology coverage gaps and R5-extension-on-R4 namespace drift, not analyzer bugs.
- **[MEDITECH Greenfield SMART standalone-launch works](https://mock.health/map/meditech/greenfield#auth)** with `openid fhirUser launch/patient offline_access patient/*.read` and **no `aud` parameter**. This is non-obvious; it's why we measured.
- **The iron rule:** every cited claim has `source_url`, `source_quote`, `verified_via`, and `verified_date`. Schema-enforced. CI-gated. No row without evidence.

## What this is not

A guess. A vendor marketing pamphlet. An aspirational standards document. A scrape of vendor blog posts. Every cell has a verified date and a source quote. When the data goes stale, every page in the rendered site shows a yellow banner. When a vendor pushes a CapStmt update, [`refresh-capstmts.yml`](./.github/workflows/refresh-capstmts.yml) runs Phase A weekly and opens a "drift detected" PR with the diff. We make this stick.

---

## What one row looks like

Here is a real, unedited entry from [`ehrs/epic/overlay.json`](./ehrs/epic/overlay.json) — one row out of 189, picked at random:

```json
{
  "row_id": "d327784aed12",
  "profile_id": "us-core-condition-problems-health-concerns",
  "path": "Condition.code",
  "deviation_category": "value-set-narrowed",
  "expected_per_us_core": "binding extensible → us-core-condition-code",
  "observed_in_ehr": "2 of 4 codings outside the bound VS; sample: [('http://snomed.info/sct', '68154008'), ('urn:oid:2.16.840.1.113883.3.247.1.1', '41151')]",
  "deviation": "With extensible binding, vendor MAY emit codes outside the bound VS, but doing so signals vendor-specific value-set narrowing/extension. Observed in 3/3 patient(s).",
  "verification": {
    "source_url": "(see paired golden fixture)",
    "source_quote": "out-of-set: [('http://snomed.info/sct', '68154008'), ('urn:oid:2.16.840.1.113883.3.247.1.1', '41151')]; in-set: [('http://hl7.org/fhir/sid/icd-10-cm', 'R05'), ('http://hl7.org/fhir/sid/icd-9-cm', '786.2')]",
    "verified_via": "epic_public_sandbox",
    "verified_date": "2026-04-26"
  }
}
```

That `urn:oid:2.16.840.1.113883.3.247.1.1` is Epic's internal code system OID — a real artifact of Epic's deployment, observed in three sandbox patients, captured in a paired golden fixture. The `row_id` is content-addressed — it never moves, never collides, never reshuffles when new rows are added. **Cite it directly:** `the-map row_id d327784aed12` resolves forever.

## Cite a finding

Every `element_deviations[]` row carries a stable `row_id` derived deterministically from `(profile_id, path, deviation_category)`. To anchor a blog post, paper, or vendor ticket to a specific finding:

```
The Map: row_id d327784aed12  (Epic Condition.code value-set narrowing)
https://github.com/mock-health/the-map/blob/main/ehrs/epic/overlay.json
https://mock.health/map/epic/Condition/code
```

If a row is later re-categorized, its old `row_id` lands in the new row's `previous_row_ids[]` so external links still resolve. See [`tools/row_id.py`](./tools/row_id.py) for the hash function.

## Verify a finding in 60 seconds

Doubt the data? Here is the entire receipt path. Three commands, anonymous, no credentials, ~60 seconds:

```bash
git clone https://github.com/mock-health/the-map.git
cd the-map && python -m venv .venv && . .venv/bin/activate
pip install -e .[dev]

# Re-fetch Epic's CapabilityStatement live from the sandbox
python -m tools.fetch_capability epic

# Diff against the committed copy. Empty diff = the Map's source matches reality.
git diff ehrs/epic/CapabilityStatement.json
```

For a fleet-shape claim ("31% of Epic customers run a non-modal CapStmt"):

```bash
python -m tools.fetch_brands epic
python -m tools.harvest_production_capstmts epic --max-endpoints=20
python -m tools.analyze_fleet_drift epic --print
```

The whole catalog can be re-derived from these primitives. Phase A and Phase F are spec-mandated public per [FHIR R4 §C.0.0](http://hl7.org/fhir/R4/http.html#capabilities) and [SMART STU 2.2 brands](https://hl7.org/fhir/smart-app-launch/STU2.2/brands.html); no vendor relationship required.

---

## What "the Map" is

A structured catalog of how each major EHR — Epic, Oracle Health (Cerner), MEDITECH — actually implements [USCDI v3](https://www.healthit.gov/isa/united-states-core-data-interoperability-uscdi) / [US Core 6.1](http://hl7.org/fhir/us/core/STU6.1/), including the documented deviations from spec, vendor-specific extensions, search-param subsets, OperationOutcome shapes, auth-flow specifics, and pagination quirks.

USCDI v3 says "every certified EHR must support these data classes." US Core 6.1 says "here's how to expose them via FHIR R4." In practice, every EHR deviates. Vendors skip MUST-SUPPORT elements, add proprietary extensions, narrow value sets, support a subset of search params, and emit different error shapes for the same condition. Until now there was no canonical "for-each-EHR, here's the deviation" reference. This repo is that reference.

## What's measured

For each EHR × USCDI v3 data class × US Core element:

- Whether the element is supported (yes / partial / no / unknown / vendor-extension)
- What the actual emitted values look like vs. the US Core baseline
- Vendor-specific extensions added beyond spec
- Recommended client workaround for any deviation
- Source citation: vendor doc URL, verbatim quote, verified date, verification method

Plus per-EHR: SMART auth variant, supported OAuth flows, search params accepted/rejected per resource, OperationOutcome shape, pagination behavior. Plus per-vendor production-fleet rollups: how many of the live customer endpoints actually advertise each profile, what the modal CapStmt shape looks like, the long tail.

## Methodology

Phase A is anonymous CapStmt fetch. Phase F is anonymous brands-bundle harvest across the vendor's whole customer fleet. Phases 1-6 require sandbox credentials and produce per-element evidence. Phases 7-8 synthesize and validate. Full runbook: [`METHODOLOGY.md`](./METHODOLOGY.md). Architecture: [`DESIGN.md`](./DESIGN.md). Data contract for downstream consumers: [`EXPORT.md`](./EXPORT.md).

The two source-of-truth files per EHR are `CapabilityStatement.json` (verbatim from sandbox `/metadata`, never edited) and `overlay.json` (only the things CapStmt cannot tell you). The pipeline reads both at render time. **No intermediate file** — an intermediate goes stale; this can't.

## Production-fleet evidence (Git LFS)

`tests/golden/production-fleet/**` holds the per-endpoint CapStmt + smart-config harvest that backs every fleet-shape claim. ~380 MB across 2,474 endpoints. Tracked via Git LFS so a default `git clone` stays fast (~80 MB):

```bash
git clone https://github.com/mock-health/the-map.git           # 80 MB
cd the-map && git lfs pull                                     # 380 MB
git lfs pull --include="tests/golden/production-fleet/epic/**"  # one vendor
```

## Contributing

Yes, please. The Map is most credible when vendors and integrators are correcting it. See [`CONTRIBUTING.md`](./CONTRIBUTING.md). Two contributor paths:

- **Phase A doc-only path** — no sandbox creds. Cite vendor public docs to add or correct a row. Iron rule applies: `source_url`, `source_quote`, `verified_via`, `verified_date`.
- **Phase 1+ sandbox path** — sign up for the relevant vendor's developer program, run the sweep tools locally, open a PR with the updated overlay + golden fixtures.

Vendor dev-relations teams get fast-track review on their own rows.

## License

- **Map content** (the JSON files under `ehrs/`, `us-core/`, `uscdi/`, `schema/`, the prose docs): [CC BY 4.0](./LICENSE-CONTENT). Cite as: *"The Map (US Core Deviation Catalog) by mock.health, licensed CC BY 4.0 — https://github.com/mock-health/the-map"*.
- **Tooling** (the Python under `tools/` and `tests/`): [Apache 2.0](./LICENSE-CODE).

## Disclaimer

This project is not affiliated with or endorsed by any of the EHR vendors mapped. Trademarks are property of their respective owners and used here for identification only. Compatibility claims reference each vendor's published FHIR profile; we do not claim to perfectly mimic any specific EHR's production behavior, which varies by site configuration. Rapid takedown pathway for any vendor concern: open an issue or email security@mock.health.

---

<details>
<summary><b>Repo layout</b> (click to expand)</summary>

```
the-map/
├── README.md                            # you are here
├── DESIGN.md                            # multi-EHR architecture
├── METHODOLOGY.md                       # the runbook (Phases A-F)
├── EXPORT.md                            # data contract for downstream consumers
├── CONTRIBUTING.md                      # how to add or correct an EHR
├── CHANGELOG.md                         # release notes
├── LICENSE-CODE                         # Apache 2.0 (the Python pipeline)
├── LICENSE-CONTENT                      # CC BY 4.0 (the data)
├── pyproject.toml                       # deps + ruff + mypy + pytest config
├── Makefile                             # `make help` for tasks
├── schema/
│   ├── overlay.schema.json              # JSON Schema for ehrs/{ehr}/overlay.json
│   └── production_fleet.schema.json     # JSON Schema for ehrs/{ehr}/production_fleet.json
├── ehrs/                                # source-of-truth files per EHR
│   ├── epic/
│   │   ├── CapabilityStatement.json     # verbatim from sandbox /metadata; never edited
│   │   ├── overlay.json                 # only what CapStmt cannot tell us
│   │   └── production_fleet.json        # rolled-up production-fleet snapshot
│   ├── cerner/                          # Oracle Health — provider (fhir-ehr) + patient (fhir-myrecord) host fronts unioned; see overlay.access_scope_notes
│   └── meditech/
│       └── greenfield/                  # MEDITECH's vendor-managed sandbox
├── uscdi/
│   └── uscdi-v3-baseline.json           # USCDI v3 22 data classes
├── us-core/
│   └── us-core-6.1-baseline.json        # US Core 6.1 MUST-SUPPORT (auto-extracted from official IG)
├── tools/                               # Python pipeline (one driver per phase)
│   ├── _env.py                          # shared .env loader
│   ├── row_id.py                        # stable content-addressed row identifiers
│   ├── fetch_capability.py              # Phase A
│   ├── fetch_brands.py                  # Phase F.1
│   ├── harvest_production_capstmts.py   # Phase F.2
│   ├── analyze_fleet_drift.py           # Phase F.3
│   ├── enumerate_sandbox_patients.py    # Phase 1
│   ├── measure_phase_b.py               # Phase 2
│   ├── probe_search_bulk_refs.py        # Phase 3
│   ├── upload_us_core_to_hapi.py        # Phase 4 prep
│   ├── cross_validate_with_hapi.py      # Phase 4
│   ├── run_inferno_us_core.py           # Phase 5
│   ├── probe_transport.py               # Phase 6
│   ├── synthesize.py                    # Phase 7a (matrix)
│   ├── render_html.py                   # Phase 7b (HTML)
│   ├── validate.py                      # Phase 8
│   ├── conformance/                     # presence + cardinality + value_set + format
│   ├── auth_flows/                      # client_credentials + auth_code
│   └── hapi/docker-compose.example.yml  # vendor-neutral HAPI for Phase 4
├── tests/
│   ├── test_env_loading.py              # P0: env loader regression
│   ├── test_validate_iron_rule.py       # P0: schema iron-rule adversarial
│   ├── test_render_paths.py             # P0: URL stability + XSS escape + staleness banner
│   └── golden/                          # captured fixtures + production-fleet harvests
│       └── production-fleet/            # 2,474 endpoints; 380 MB; tracked via Git LFS
└── reports/                             # quarterly snapshots + cross-vendor views
```

</details>

<details>
<summary><b>Full pipeline run</b> (for contributors and verifiers)</summary>

```bash
# Setup (Python 3.11+; Phase A and Phase F run with no .env at all)
python -m venv .venv && . .venv/bin/activate
pip install -e .[dev]

# Phase A — anonymous /metadata fetch (no creds)
python -m tools.fetch_capability epic
python -m tools.fetch_capability meditech

# Phase F — anonymous brands-bundle harvest (no creds)
python -m tools.fetch_brands --all
python -m tools.harvest_production_capstmts epic
python -m tools.analyze_fleet_drift epic --print

# Phase 1+ — sandbox sweeps (sandbox creds required; see CONTRIBUTING.md)
cp .env.example .env  # then fill in EPIC_NONPROD_*, CERNER_NONPROD_*, etc.
python -m tools.enumerate_sandbox_patients epic
python -m tools.measure_phase_b epic
python -m tools.probe_search_bulk_refs epic

# Phase 4 — HAPI cross-validation
docker compose -f tools/hapi/docker-compose.example.yml up -d
python -m tools.upload_us_core_to_hapi
python -m tools.cross_validate_with_hapi epic

# Synthesize → render → validate
python -m tools.synthesize epic --write-matrix=reports/2026-q2/epic-conformance-matrix.json
python -m tools.render_html epic
python -m tools.validate
```

`make help` lists every developer task.

</details>

Built for [mock.health](https://mock.health).
