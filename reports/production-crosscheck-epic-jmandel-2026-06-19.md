# Production cross-check — Epic (wire tier, community_report)

**Date:** 2026-06-19
**Source:** jmandel/ehi-to-fhir fhir-target/ — de-identified capture of Epic's live FHIR API (1 patient, 1 undisclosed Epic org)
**Iron-rule citation:** https://github.com/jmandel/ehi-to-fhir/tree/e86e00ca322174c520d4e682457a20dd913033cb/fhir-target
**Evidence tier:** genuine production Epic FHIR (live API), de-identified + published by a third party
**Provenance:** `fhir-target/` is jmandel/ehi-to-fhir's **live-API reference target** (the directory the EHI→FHIR converter is scored *against*), i.e. genuine Epic FHIR API bytes — not the converter's reconstructed output. That is why this is a `wire` cross-check, not an `ehi` one.
**Scope:** n=1 patient (`jmandel-epic`), n=1 site. Resources analyzed: 621 (AllergyIntolerance×4, CarePlan×4, CareTeam×1, Condition×53, Coverage×1, DiagnosticReport×9, DocumentReference×51, Encounter×34, Goal×1, Immunization×19, Location×6, Medication×18, MedicationRequest×18, Observation×357, OperationOutcome×1, Organization×5, Patient×1, Practitioner×29, Specimen×9).

This report contains structural facts only (profile / path / deviation category /
verdict). No values, identifiers, dates, names, or note text. The PHI-bearing raw
findings and candidate rows live under the gitignored --phi-out path.

## Method

Resources were run through the-map's four-axis conformance analyzer
(`tools/conformance/`: presence, cardinality, value-set, format) against
`us-core/us-core-6.1-baseline.json` — the same analyzer `tools/measure_phase_b.py`
runs on sandbox golden fixtures — then diffed against the sandbox-derived
`ehrs/epic/overlay.json` by `row_id` (= sha256(profile_id|path|deviation_category)).

## Verdict summary

| bucket | count | meaning |
|---|---|---|
| **confirmed deviations** | 41 | production REPRODUCED a sandbox deviation (row_id hit) → strongest signal; confidence upgrade |
| **confirmed matches** | 134 | production matched spec where the sandbox also matched → background agreement |
| **divergent** | 9 | same path, *different* behavior than the sandbox recorded → sandbox claim may be sandbox-specific |
| **novel deviations** | 95 | a deviation the sandbox never recorded → potential new row |
| **absent types** | 8 | whole resource types not in this record (n=1 coverage, not a finding) |
| **untested** | 14 | sandbox rows this single record never exercised |

_Benign matches at paths the sandbox doesn't track (agreement, not listed): 302._

## Confirmed deviations (production reproduced the sandbox finding)

> Caveat: rows whose category is `patient-data-gap-n1` are single-patient coverage
> gaps that happened to hash to a sandbox `missing` row — they are NOT reproduced
> vendor deviations. The headline "confirmed" count is inflated by these until the
> bucketing is fixed to exclude n=1 absences (tracked follow-up). Only `value-set-*`,
> `nonstandard-system`, and similar non-absence categories here are genuine reproductions.

| profile | path | category |
|---|---|---|
| `us-core-allergyintolerance` | `AllergyIntolerance.clinicalStatus` | value-set-unverified-locally |
| `us-core-allergyintolerance` | `AllergyIntolerance.verificationStatus` | value-set-unverified-locally |
| `us-core-condition-encounter-diagnosis` | `Condition.clinicalStatus` | value-set-unverified-locally |
| `us-core-condition-encounter-diagnosis` | `Condition.code` | value-set-narrowed |
| `us-core-condition-encounter-diagnosis` | `Condition.extension(condition-assertedDate)` | patient-data-gap-n1 |
| `us-core-condition-encounter-diagnosis` | `Condition.verificationStatus` | value-set-unverified-locally |
| `us-core-condition-problems-health-concerns` | `Condition.category:screening-assessment` | value-set-mismatch |
| `us-core-condition-problems-health-concerns` | `Condition.clinicalStatus` | value-set-unverified-locally |
| `us-core-condition-problems-health-concerns` | `Condition.code` | value-set-narrowed |
| `us-core-condition-problems-health-concerns` | `Condition.extension(condition-assertedDate)` | patient-data-gap-n1 |
| `us-core-condition-problems-health-concerns` | `Condition.verificationStatus` | value-set-unverified-locally |
| `us-core-diagnosticreport-lab` | `DiagnosticReport.status` | value-set-unverified-locally |
| `us-core-documentreference` | `DocumentReference.content.attachment.contentType` | value-set-unverified-locally |
| `us-core-documentreference` | `DocumentReference.content.attachment.data` | patient-data-gap-n1 |
| `us-core-documentreference` | `DocumentReference.status` | value-set-unverified-locally |
| `us-core-documentreference` | `DocumentReference.type` | value-set-mismatch |
| `us-core-encounter` | `Encounter.reasonReference` | patient-data-gap-n1 |
| `us-core-encounter` | `Encounter.status` | value-set-unverified-locally |
| `us-core-encounter` | `Encounter.type` | value-set-narrowed |
| `us-core-immunization` | `Immunization.status` | value-set-unverified-locally |
| `us-core-immunization` | `Immunization.statusReason` | patient-data-gap-n1 |
| `us-core-medicationrequest` | `MedicationRequest.category:us-core` | value-set-unverified-locally |
| `us-core-medicationrequest` | `MedicationRequest.intent` | value-set-unverified-locally |
| `us-core-medicationrequest` | `MedicationRequest.reasonReference` | patient-data-gap-n1 |
| `us-core-medicationrequest` | `MedicationRequest.reported[x]` | patient-data-gap-n1 |
| `us-core-medicationrequest` | `MedicationRequest.status` | value-set-unverified-locally |
| `us-core-observation-lab` | `Observation.category:us-core` | value-set-mismatch |
| `us-core-observation-lab` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-observation-lab` | `Observation.status` | value-set-unverified-locally |
| `us-core-patient` | `Patient.address.use` | value-set-unverified-locally |
| `us-core-patient` | `Patient.extension(us-core-tribal-affiliation)` | patient-data-gap-n1 |
| `us-core-patient` | `Patient.gender` | value-set-unverified-locally |
| `us-core-patient` | `Patient.name.period` | patient-data-gap-n1 |
| `us-core-patient` | `Patient.name.suffix` | patient-data-gap-n1 |
| `us-core-patient` | `Patient.name.use` | value-set-unverified-locally |
| `us-core-patient` | `Patient.telecom.system` | value-set-unverified-locally |
| `us-core-patient` | `Patient.telecom.use` | value-set-unverified-locally |
| `us-core-vital-signs` | `Observation.code` | value-set-narrowed |
| `us-core-vital-signs` | `Observation.component.code` | value-set-narrowed |
| `us-core-vital-signs` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-vital-signs` | `Observation.status` | value-set-unverified-locally |

## Divergent (production differs from what the sandbox recorded)

| profile | path | category | sandbox_categories |
|---|---|---|---|
| `us-core-condition-encounter-diagnosis` | `Condition.abatement[x]` | matches-everywhere | missing |
| `us-core-condition-problems-health-concerns` | `Condition.abatement[x]` | matches-everywhere | missing |
| `us-core-encounter` | `Encounter.hospitalization.dischargeDisposition` | matches-everywhere | missing |
| `us-core-encounter` | `Encounter.hospitalization.dischargeDisposition` | value-set-narrowed | missing |
| `us-core-encounter` | `Encounter.serviceProvider` | patient-data-gap-n1 | matches |
| `us-core-medicationrequest` | `MedicationRequest.reasonCode` | matches-everywhere | missing |
| `us-core-medicationrequest` | `MedicationRequest.reasonCode` | value-set-narrowed | missing |
| `us-core-patient` | `Patient.extension(us-core-genderIdentity)` | matches-everywhere | missing |
| `us-core-vital-signs` | `Observation.dataAbsentReason` | patient-data-gap-n1 | matches |

## Novel deviations (only seen in production)

| profile | path | category |
|---|---|---|
| `us-core-blood-pressure` | `Observation.code` | value-set-narrowed |
| `us-core-blood-pressure` | `Observation.component.code` | value-set-narrowed |
| `us-core-blood-pressure` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-blood-pressure` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-blood-pressure` | `Observation.status` | value-set-unverified-locally |
| `us-core-bmi` | `Observation.code` | value-set-narrowed |
| `us-core-bmi` | `Observation.component.code` | value-set-narrowed |
| `us-core-bmi` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-bmi` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-bmi` | `Observation.status` | value-set-unverified-locally |
| `us-core-body-height` | `Observation.code` | value-set-narrowed |
| `us-core-body-height` | `Observation.component.code` | value-set-narrowed |
| `us-core-body-height` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-height` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-height` | `Observation.status` | value-set-unverified-locally |
| `us-core-body-height` | `Observation.value[x].code` | value-set-unverified-locally |
| `us-core-body-temperature` | `Observation.code` | value-set-narrowed |
| `us-core-body-temperature` | `Observation.component.code` | value-set-narrowed |
| `us-core-body-temperature` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-temperature` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-temperature` | `Observation.status` | value-set-unverified-locally |
| `us-core-body-temperature` | `Observation.value[x].code` | value-set-unverified-locally |
| `us-core-body-weight` | `Observation.code` | value-set-narrowed |
| `us-core-body-weight` | `Observation.component.code` | value-set-narrowed |
| `us-core-body-weight` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-weight` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-body-weight` | `Observation.status` | value-set-unverified-locally |
| `us-core-body-weight` | `Observation.value[x].code` | value-set-unverified-locally |
| `us-core-careplan` | `CarePlan.intent` | value-set-unverified-locally |
| `us-core-careplan` | `CarePlan.status` | value-set-unverified-locally |
| `us-core-careteam` | `CareTeam.status` | value-set-unverified-locally |
| `us-core-coverage` | `Coverage.status` | value-set-unverified-locally |
| `us-core-diagnosticreport-note` | `DiagnosticReport.category:us-core` | value-set-mismatch |
| `us-core-diagnosticreport-note` | `DiagnosticReport.media` | patient-data-gap-n1 |
| `us-core-diagnosticreport-note` | `DiagnosticReport.media.link` | patient-data-gap-n1 |
| `us-core-diagnosticreport-note` | `DiagnosticReport.presentedForm` | patient-data-gap-n1 |
| `us-core-diagnosticreport-note` | `DiagnosticReport.status` | value-set-unverified-locally |
| `us-core-goal` | `Goal.lifecycleStatus` | value-set-unverified-locally |
| `us-core-goal` | `Goal.target` | patient-data-gap-n1 |
| `us-core-goal` | `Goal.target.due[x]` | patient-data-gap-n1 |
| `us-core-head-circumference` | `Observation.code` | value-set-narrowed |
| `us-core-head-circumference` | `Observation.component.code` | value-set-narrowed |
| `us-core-head-circumference` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-head-circumference` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-head-circumference` | `Observation.status` | value-set-unverified-locally |
| `us-core-head-circumference` | `Observation.value[x].code` | value-set-unverified-locally |
| `us-core-heart-rate` | `Observation.code` | value-set-narrowed |
| `us-core-heart-rate` | `Observation.component.code` | value-set-narrowed |
| `us-core-heart-rate` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-heart-rate` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-heart-rate` | `Observation.status` | value-set-unverified-locally |
| `us-core-location` | `Location.address` | patient-data-gap-n1 |
| `us-core-location` | `Location.address.city` | patient-data-gap-n1 |
| `us-core-location` | `Location.address.line` | patient-data-gap-n1 |
| `us-core-location` | `Location.address.postalCode` | patient-data-gap-n1 |
| `us-core-location` | `Location.address.state` | patient-data-gap-n1 |
| `us-core-location` | `Location.managingOrganization` | patient-data-gap-n1 |
| `us-core-location` | `Location.status` | patient-data-gap-n1 |
| `us-core-location` | `Location.telecom` | patient-data-gap-n1 |
| `us-core-observation-clinical-result` | `Observation.category:us-core` | value-set-mismatch |
| `us-core-observation-clinical-result` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-observation-clinical-result` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-occupation` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-pregnancyintent` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-pregnancyintent` | `Observation.value[x]:valueCodeableConcept` | value-set-narrowed |
| `us-core-observation-pregnancystatus` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-pregnancystatus` | `Observation.value[x]:valueCodeableConcept` | value-set-narrowed |
| `us-core-observation-screening-assessment` | `Observation.category:screening-assessment` | value-set-mismatch |
| `us-core-observation-screening-assessment` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-observation-screening-assessment` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-sexual-orientation` | `Observation.status` | value-set-unverified-locally |
| `us-core-observation-sexual-orientation` | `Observation.value[x]:valueCodeableConcept` | value-set-narrowed |
| `us-core-organization` | `Organization.address.country` | patient-data-gap-n1 |
| `us-core-organization` | `Organization.telecom.system` | value-set-unverified-locally |
| `us-core-practitioner` | `Practitioner.address` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.address.city` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.address.country` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.address.line` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.address.postalCode` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.address.state` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.telecom` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.telecom.system` | patient-data-gap-n1 |
| `us-core-practitioner` | `Practitioner.telecom.value` | patient-data-gap-n1 |
| `us-core-pulse-oximetry` | `Observation.code` | value-set-narrowed |
| `us-core-pulse-oximetry` | `Observation.component.code` | value-set-narrowed |
| `us-core-pulse-oximetry` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-pulse-oximetry` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-pulse-oximetry` | `Observation.status` | value-set-unverified-locally |
| `us-core-respiratory-rate` | `Observation.code` | value-set-narrowed |
| `us-core-respiratory-rate` | `Observation.component.code` | value-set-narrowed |
| `us-core-respiratory-rate` | `Observation.component.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-respiratory-rate` | `Observation.dataAbsentReason` | patient-data-gap-n1 |
| `us-core-respiratory-rate` | `Observation.status` | value-set-unverified-locally |
| `us-core-simple-observation` | `Observation.status` | value-set-unverified-locally |
| `us-core-smokingstatus` | `Observation.code` | value-set-narrowed |

## Absent resource types (n=1 coverage)

| profile | path | category |
|---|---|---|
| `us-core-implantable-device` | `Device` | patient-data-gap-n1 |
| `us-core-medicationdispense` | `MedicationDispense` | patient-data-gap-n1 |
| `us-core-practitionerrole` | `PractitionerRole` | patient-data-gap-n1 |
| `us-core-procedure` | `Procedure` | patient-data-gap-n1 |
| `us-core-provenance` | `Provenance` | patient-data-gap-n1 |
| `us-core-questionnaireresponse` | `QuestionnaireResponse` | patient-data-gap-n1 |
| `us-core-relatedperson` | `RelatedPerson` | patient-data-gap-n1 |
| `us-core-servicerequest` | `ServiceRequest` | patient-data-gap-n1 |

## Honest limitations

- **n=1 patient, n=1 site.** No generalization beyond this single record on 2026-06-19.
  Absent resource types are patient-data gaps, not vendor gaps (see n=1 correction).
- **Patient scopes ≠ system scopes.** A patient-access launch may expose a narrower
  slice than the system-scope sandbox sweep; some untested rows reflect scope, not absence.
- **De-identified third-party capture.** Direct-identifier *values* (MRN, etc.) were redacted by the publisher, so format-axis findings on identifier values may be redaction artifacts — presence, coding-system, and cardinality findings are unaffected. The site is an undisclosed Epic org with no CapabilityStatement shipped, so its cluster is unknown; this validates Epic broadly, not a named deployment. Only the confirmed-deviation and divergent buckets are trustworthy without triage.


## Next step

Review `<phi-out>/candidate_rows.json`, fill a de-identified `source_quote` for each row worth keeping, then append confirmed/novel rows to `ehrs/epic/overlay.json` with verified_via=community_report. Re-run `python -m tools.validate epic`.
