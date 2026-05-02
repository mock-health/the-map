# EXPORT — The data contract for downstream consumers

The Map is consumed by the public-facing site at [`mock.health/map`](https://mock.health/map) (the rendered, browseable surface), as well as anyone who wants to build on top of the data — vendor analytics teams, conformance researchers, partner integrations.

This file specifies **exactly what gets exported**, where it goes, and the stability promises that come with it. Everything else in this repository is implementation detail: pipeline source, fixtures, test code, internal docs.

## Running the export

```bash
make export
```

Produces `dist/data/the-map/` with a stable, file-tree-stable, schema-validated subset of the repo. The Makefile target also writes `dist/data/the-map/MANIFEST.json` listing every file so consumers can detect drift.

## What's in the export

```
dist/data/the-map/
├── ehrs/
│   ├── epic/
│   │   ├── CapabilityStatement.json    # verbatim sandbox /metadata, dated
│   │   ├── overlay.json                # measured deviations from US Core 6.1
│   │   ├── production_fleet.json       # rolled-up production-fleet snapshot (when present)
│   │   ├── sandbox_patients.json       # per-resource patient coverage matrix
│   │   └── p0_patient_selection.json   # the multi-patient sweep selection
│   ├── cerner/                          # Oracle Health (provider + patient host fronts unioned; see overlay.access_scope_notes)
│   ├── meditech/
│   └── meditech/greenfield/
├── us-core/
│   └── us-core-6.1-baseline.json       # auto-extracted from official US Core 6.1.0 IG package
├── uscdi/
│   └── uscdi-v3-baseline.json          # USCDI v3 22 data classes
├── schema/
│   ├── overlay.schema.json             # JSON Schema for ehrs/{ehr}/overlay.json
│   └── production_fleet.schema.json    # JSON Schema for ehrs/{ehr}/production_fleet.json
└── MANIFEST.json                       # generated; lists every exported file
```

Total: roughly 3 MB at v1.0. Scales linearly with vendor count; under 20 MB even at 10× coverage.

## What's deliberately NOT in the export

- `tools/` — Python pipeline source. Consumers don't need to run our pipeline; they consume its output. (Anyone who *wants* to run the pipeline pulls the full repo.)
- `tests/` — fixtures and test code. The 380 MB `tests/golden/production-fleet/` is the iron-rule audit trail (Git LFS); it's accessible from the source repo, not the export.
- `reports/` — HTML render output is regenerable from the data; the live site is `mock.health/map`.
- `*.md` — narrative docs are repo-internal. The consumer side has its own narrative surface.
- `.tokens/`, `.env`, `.venv/`, `.gstack/` — secrets and dev state, gitignored.

## Stability promises

The export contract is **stable across patch and minor releases** (v1.0.x, v1.x.0). Concretely:

- File paths under `ehrs/{ehr}/`, `us-core/`, `uscdi/`, `schema/` will not move within a major version.
- New EHRs land as new `ehrs/{slug}/` directories; existing slugs are not renamed.
- New top-level fields may be added to `overlay.json` and `production_fleet.json` (the schema is `additionalProperties: true` on top-level overlay by design — see `DESIGN.md`); existing top-level fields are not removed within a major.
- New `deviation_category` enum values may be added; existing values are not removed.
- `verification` block fields (`source_url`, `source_quote`, `verified_via`, `verified_date`) are required for every cited claim. This is the iron rule and it is permanent.

Major-version bumps (v2.0) may restructure file paths and rename slugs. The CHANGELOG will name every breaking change with a migration recipe.

## Versioning

Each release is tagged on the source repo (`v1.0`, `v1.1`, ...) and includes a generated `MANIFEST.json` listing every exported file. fhir-studio's sync action pins to a tag, not to `main`, so a partial update mid-write doesn't half-replace data.

If you're consuming the export programmatically:

```python
import json, pathlib
manifest = json.loads(pathlib.Path("data/the-map/MANIFEST.json").read_text())
print(manifest["export_version"], len(manifest["export_files"]))
```

`export_version` increments on breaking shape changes (path layout, manifest schema). `export_files` is a sorted list — diff two manifests to see what was added, removed, or moved.

## How to consume

Pick whichever pattern fits your build:

1. **Vendored copy + sync action** (this is what `mock.health/map` uses). A scheduled GitHub Action clones the source repo at a tag, runs `make export`, and copies `dist/data/the-map/` into your tree. PR with the diff. Reviewable, reproducible, offline-buildable.

2. **Build-time fetch from a release asset.** Cut a release on github.com/mock-health/the-map, attach `data-the-map-v1.0.tar.gz` as an artifact, fetch + extract in your build. Trades reviewability for setup ease.

3. **Direct read from the source clone.** If you're already cloning this repo (e.g., for the pipeline), point your build at `ehrs/`, `us-core/`, `schema/` directly. No export step. Simpler but tightly couples your repo's freshness to your clone's freshness.

The Map's CC-BY-4.0 license applies to the data; Apache-2.0 to the tooling. Attribute as: *"The Map (US Core Deviation Catalog) by Mock Health, Inc., licensed CC BY 4.0 — https://github.com/mock-health/the-map"*.

## Iron rule, exported

Every cited claim in any exported `overlay.json` carries a `verification` block:

```json
{
  "verification": {
    "source_url": "https://fhir.epic.com/Specifications",
    "source_quote": "Patient.extension.us-core-birthsex is supported but not emitted unless...",
    "verified_via": "epic_public_sandbox",
    "verified_date": "2026-04-26"
  }
}
```

Schema enforcement on the source is strict; CI rejects PRs that violate. When you consume the data, you can rely on every claim being citable. This is the whole point.
