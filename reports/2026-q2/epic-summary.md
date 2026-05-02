# Epic v1 Map — Editorial Summary (2026-04-26)

A categorized digest of the 22 Phase B headlines for downstream consumers (Brendan
Keeler co-piece, mock.health/compare tool, mutate-on-read wrapper). Source-of-truth
for every line below: `ehrs/epic/overlay.json` + paired golden fixture files in
`tests/golden/epic/phase-b-2026-04-26/`.

## Spec-conformance gaps

Real US Core 6.1 conformance violations (would fail Inferno's gold-standard tests):

- **R5 extensions on R4 DocumentReferences** — `http://hl7.org/fhir/5.0/StructureDefinition/extension-DocumentReference.attester` shows up on Camila Lopez's documents. R4 endpoint should not emit R5-namespaced extensions; HAPI rejects.
- **DocumentReference.type uses 100% Epic-proprietary OID codings** — bound LOINC VS is `required`; OID-only codings violate. 3/3 fixtures flagged.
- **Observation.category:us-core slice carries vendor-proprietary OID alongside the required `laboratory` code** — slice satisfies the required binding (laboratory IS present) but receivers must scan all coding[] entries, not just coding[0].
- **CDC race/ethnicity OIDs un-validatable** — `urn:oid:2.16.840.1.113883.6.238` for ombCategory race coding can't be membership-checked without external CDC terminology service. Vendors using the OID literally are technically conformant but un-validatable with stock tooling.

## Vendor-specific extensions

- **Patient.extension(us-core-tribal-affiliation)** — never emitted across 3 swept patients (vendor-implementation gap)
- **Patient.extension(us-core-genderIdentity)** — same: not emitted by any test patient
- **Patient.name.suffix, Patient.name.period** — not emitted by any test patient
- **Condition.extension(condition-assertedDate), Condition.abatement[x]** — not emitted across the swept patient set
- **Observation.dataAbsentReason** — never emitted on labs

## Implementation leaks (vendor-OID + standard-code dual coding)

These confirm Epic's hybrid coding pattern: standard code + Epic OID code carried alongside.

- Observation.code on labs: LOINC + `urn:oid:1.2.840.114350.1.13.0.1.7.2.768282` (Epic lab procedure dictionary)
- Observation.code on vitals: LOINC + `urn:oid:1.2.840.114350.1.13.0.1.7.2.707679`
- Observation.category:us-core: standard `laboratory` + Epic `urn:oid:1.2.840.114350.1.13.0.1.7.10.798268.30` value `Lab`
- Encounter.type: 100% Epic OIDs (no standard codes)
- Procedure.code: Epic CPT-substitute OID `urn:oid:1.2.840.114350.1.13.0.1.7.2.696580`

The Map's mutate-on-read wrapper should mirror this dual-coding pattern.

## Transport oddities

- **Default content-type fallback is XML**, not JSON, when Accept is missing or unparseable. Single most likely cause of silent integration failures targeting Epic.
- **Bundle.total reports page size, not result count** when `_count` is set. Use `_summary=count` for accurate total.
- **Empty-result Bundles include an OperationOutcome `mode=outcome` entry**, not an empty `entry` array. Filter by `entry.search.mode != 'outcome'` AND `resourceType != 'OperationOutcome'`.
- **Patient search refuses to enumerate** without demographics-or-_id; `birthdate` alone returns 0 matches without erroring.
- **Bulk `$export` returns IIS HTML 404**, not a FHIR OperationOutcome. URL never reaches the FHIR layer.
- **Unknown FHIR resource types return IIS HTML 404** (same routing pattern; v0.1 finding).
- **Vital-signs Observation page returns 1000 entries on first page** — large page size by FHIR norms.
- **Reference fields are 100% relative-form**; resolution rate 100% over 30 samples.

## Integrator-trap behaviors

- **Silent-param-ignore is 80% prevalent across P0 resources**, not just Patient. AllergyIntolerance, Condition, DiagnosticReport, DocumentReference, Encounter, Immunization, MedicationRequest, and Procedure all silently drop bogus params and return data that looks filtered but isn't.
- **OperationOutcome uses Epic-proprietary coding system OID** `1.2.840.114350.1.13.0.1.7.2.657369` for fine-grained error categorization (codes like 59008, 59100, 59159).
- **Silently-ignored Patient search params return HTTP 400 with severity=information** — a single response carries multiple OperationOutcome.issue entries with different severities; first-issue-only readers misdiagnose.
- **Vital-signs LOINC codes fall outside the official us-core-vital-signs subset**. Match by system='http://loinc.org' rather than against the bound subset.
- **Patient ombCategory race uses CDC OID 2.16.840.1.113883.6.238**; HAPI without external terminology cannot validate membership.

## Confirming-no-surprise findings

- Invalid bearer token returns RFC 6750-conformant 401 + WWW-Authenticate header.
- Concurrent token issuance returns 5 distinct tokens; no caching/reuse.
- No rate limiting observed at ~27 RPS (sub-30-RPS workloads safe on sandbox).

## Patient-data gaps (not vendor-implementation gaps)

Important distinction: these aren't claims about Epic; they're about which sandbox
patients have which data. Customer integrations may see different patterns.

- **`Condition?category=health-concern` returns 0 entries for every test patient** — vendor-implementation gap on the sandbox; real customer instances may differ.
- **Lab data exists only on Camila Lopez (5 LOINC observations) and Warren McGinnis** — Derrick Lin (canonical) has zero. Multi-patient catalog is mandatory for honest conformance claims.

## Cross-validation result

HAPI's $validate (preloaded with US Core 6.1.0) agreed with our four-axis analyzer
on **31/38 (82%)** cross-checked fixtures. The 7 disagreements concentrate around
terminology coverage (CDC OID, Epic OIDs) and version-namespace drift (R5 extensions
on R4) — both real Map findings, not analyzer bugs.

Inferno cross-validation deferred — `tools.run_inferno_us_core` is ready when an
Inferno instance is available (see `tools/run_inferno_us_core.py` docstring for setup).
