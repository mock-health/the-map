---
name: Vendor correction
about: Correct a row about how a specific EHR implements US Core / USCDI
title: "[correction] {ehr}/{profile}/{element}: <one-line summary>"
labels: vendor-correction
---

<!--
Use this template when an existing row in the Map is wrong, undercited, or missing.

Iron rule: corrections need a source. The row you're correcting will be updated only with verifiable evidence (vendor doc URL + verbatim quote + verification date).
-->

## What's wrong

EHR: <!-- e.g. epic, cerner, meditech -->
Profile: <!-- e.g. us-core-patient -->
Element path: <!-- e.g. Patient.extension.us-core-birthsex -->

Current claim in the overlay:
> <!-- paste the relevant excerpt from ehrs/{ehr}/overlay.json -->

What's wrong about it:
<!-- one paragraph -->

## What it should say

Proposed corrected claim:
> <!-- paste your proposed JSON or describe the change -->

## Evidence (the iron rule)

- **source_url:** <!-- vendor doc URL, must be reachable -->
- **source_quote:** <!-- verbatim quote from the source — be exact -->
- **verified_via:** <!-- vendor_official_docs / epic_public_sandbox / customer_evidence / community_report / vendor_pr -->
- **verified_date:** <!-- YYYY-MM-DD when you verified the source -->

## Context

<!-- Anything else that helps a reviewer understand the change. If you're a vendor dev-relations team correcting your own row, please say so — we fast-track those. -->
