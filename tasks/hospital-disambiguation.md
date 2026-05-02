# Hospital disambiguation enrichment

## Why

The fhir-studio `brand_index.json` (built from this repo's `tests/golden/cross-vendor/*.json` brands bundles) powers the Studio-tier hospital lookup at `/map/lookup`. Each row currently shows hospital name + cluster + endpoint URL. That's enough for a unique name like "Boston Children's Hospital", but breaks down for:

- Common names: dozens of "St. Mary's Medical Center", several "Memorial Hospital".
- Epic-hosted endpoints: hostname is `epicproxy.etXXXX.epichosted.com` — useless for identifying the actual customer hospital.
- Cerner tenant URLs: `fhir-ehr.cerner.com/r4/<uuid>` — opaque.

Cerner and Meditech bundles already include `Organization.address[0].{city, state}` and (Meditech only) `Organization.telecom[system="url"]`. The fhir-studio build script picks those up. **Epic publishes neither** — only `Endpoint.name` and the hosted endpoint URL.

## Asks

1. **Per-hospital website lookup for Epic.** Possible sources: open.epic.com customer directory pages (each Endpoint is linked from a customer page that has a real website), Wikipedia, NPI registry, healthsystemtracker.org. Output: `data/hospital-overrides/epic-websites.json` keyed by Endpoint.id or address.
2. **Common-name disambiguation.** When the same hospital name appears in multiple states/cities, surface enough metadata that the lookup UI can show "St. Mary's — Rochester, MN" vs. "St. Mary's — Madison, WI". Cerner/Meditech already get this via `Organization.address`; Epic needs the same.
3. **Optional: NPI cross-reference.** A canonical hospital identifier would let downstream consumers join brand-bundle data with HCRIS, HQI, or claims datasets.

## Constraints

- Brand bundles are spec-defined and we don't control their content. This task is about **enrichment**, not amending the bundles themselves.
- Output should be diff-friendly JSON (one record per line if needed) — review-able when re-harvested.
- Re-runnable: a new harvest shouldn't lose hand-curated overrides.

## Consumers

`fhir-studio/scripts/build_brand_index.py` should grow an `--enrichment-dir` flag that overlays the override JSON onto the bundle-derived rows before emitting `brand_index.json`. Order: enrichment fields fill gaps but never overwrite a value the bundle already provides.

Filed 2026-05-01 from the fhir-studio `the-map` branch.
