# Per-vendor pagination link semantics — 2026-05-15

Captured offline by `tools/probe_pagination.py` walking the golden Bundle fixtures under `tests/golden/{epic,cerner,meditech}/`. Every claim below cites a captured response — no live sandbox calls were made to produce this report. Unblocks KATA-EPIC-PAGINATION-01 (#117) and KATA-EPIC-PAGINATION-02 (#118), and surfaces the corresponding deltas for Cerner and MEDITECH so a downstream emulator can faithfully reproduce them.

## Comparison table

| Dimension | Epic | Cerner | MEDITECH |
| --- | --- | --- | --- |
| Fixtures probed | 38 | 23 | 15 |
| Fixtures with rel=next | 2 | 11 | 4 |
| link relations seen | next, self | next, self | next, self |
| next URL format | opaque-base64 | vendor-custom | vendor-custom |
| default page size (when paginating) | 1000 | 10 | 20 |
| max single-page entries | 1000 | 1623 | 44 |
| vital-signs page-size override | 1000 | 50 | 20 |
| Bundle.total semantics | reflects_page_size_when_count_set | absent_in_most_captures | reflects_returned_match_count |
| empty bundle shape | mode_outcome_entry | unobserved | unobserved |
| token stability | stable_within_session_offline_observation | ttl_unknown_no_timestamp_in_token | ttl_unknown_continue_cursor_is_resource_typed |
| self-link canonicalization | echo_exact=21, echo_normalized=17 | echo_exact=23 | echo_normalized=15 |
| per-resource page-size overrides | _empty_ | MedicationRequest=25, Observation=50 | _empty_ |

## Per-vendor detail

### EPIC

- Probed `38` Bundle fixtures; `2` carry `rel=next`.
- **Bundle.total**: `reflects_page_size_when_count_set` — page_count_5: bundle_total=5 entry_count=5; page_count_1000: bundle_total=1000 entry_count=1000 — Bundle.total tracks the page payload, not the unbounded match set.
  - Evidence: `tests/golden/epic/phase-b-2026-04-26/observation-vital-signs-paginated-with-total-2026-05-02.json`
- **Token stability**: `stable_within_session_offline_observation` — next_token_stable_across_pages: true (walked 2 pages; URLs did not duplicate or drift). TTL unmeasured offline.
  - Evidence: `ehrs/epic/overlay.json:pagination_overlay.next_token_stable_across_pages`
- **Vital-signs page-size override**: `1000` entries on a single page (compare to vendor default).
- **Largest single-page no-rel=next bundles** (per resource): {'AllergyIntolerance': 3, 'Condition': 7, 'DiagnosticReport': 4, 'DocumentReference': 70, 'Encounter': 10, 'Immunization': 7, 'MedicationRequest': 9, 'Observation': 388, 'Procedure': 2}
- **Sample next URLs** (truncated to 5):
  - `https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/Observation?sessionID=16-AB6401F741B911F1BE0E005056BECC4A`
  - `https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4/Observation?sessionID=16-E90EE22641B311F1BE0E005056BECC4A`

### CERNER

- Probed `23` Bundle fixtures; `11` carry `rel=next`.
- **Bundle.total**: `absent_in_most_captures` — 21/23 captured Cerner bundles omit Bundle.total entirely (no `total` key); the remaining 2 include a total that matches match_entry_count (e.g. immunization-12742399.json: total=24, matches=24).
  - Evidence: `tests/golden/cerner/phase-b-2026-04-27/allergyintolerance-12724066.json`
- **Token stability**: `ttl_unknown_no_timestamp_in_token` — Cerner next URL embeds an opaque pageContext (UUID/base64). No timestamp component, no documented TTL in measured fixtures.
  - Evidence: `tests/golden/cerner/phase-b-2026-04-27/documentreference-12724066.json`
- **Vital-signs page-size override**: `50` entries on a single page (compare to vendor default).
- **Per-resource page-size overrides**: {'MedicationRequest': 25, 'Observation': 50}
- **Largest single-page no-rel=next bundles** (per resource): {'AllergyIntolerance': 115, 'Condition': 1623, 'DiagnosticReport': 4, 'Encounter': 1, 'Immunization': 24, 'Observation': 1, 'Procedure': 535}
- **Sample next URLs** (truncated to 5):
  - `https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/DocumentReference?patient=12724066&-pageContext=eJxljb0KwjAURt_lriaQ22qtBRfdlOKgIJ1Kmh-rhKQ0CSql7251Kjh-H-dwBhAu2gAFMgJSaR7NNAYQsfeuhwL2h3SB1_K8e96rx_FUJZdyCwRa7murXhMb-qgIdPymauFs-H2gJcsEckZXuMnpskGk-TrNKMeEIWoldNbASMCHyZN11769dGIettGYeUZz4_86X2gcPwRdQIc%3D&-pageDirection=NEXT`
  - `https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/DocumentReference?patient=12742399&-pageContext=eJxljcsKwjAUBf_lrhNoTFttwY2Cu-pC9-U2DyvEpORBldJ_t7oquDyHGWYC4ZKNULOMgFQak1nGBCL54DzUcDxjx57N9TA-7qO_nPit2QOBHkNr1Wtho0-KwIB31Qpn4-8DxjjfFl1OZck7muuM012lCyq05llZVSg3CDOBEBdPtkP_DtKJddgmY9YZjSb8db7QPH8Acn9BTA%3D%3D&-pageDirection=NEXT`
  - `https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/DocumentReference?patient=12752183&-pageContext=eJxljb0KwjAURt_lriaQtDbVgotOCp0UxKncpolVQlKaBH9K393qVHD8Ps7hDCBdtAEKzgg0SmM00xhAxt67HgrYHd53fi6P28dtv7ikkp_KDRBo0VdWPSc29FER6PCqKuls-H2gVinjGUtohhrpUoic1onOqFgzRMnzTNQpjAR8mLym6tqXb5ych200Zp7RaPxf5wuN4wcZPUCS&-pageDirection=NEXT`
  - `https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/Encounter?patient=12724066&-pageContext=af16655e-fbec-4a4a-a281-2814a69222a9&-pageDirection=NEXT`
  - `https://fhir-open.cerner.com/r4/ec2458f2-1e24-41c8-b71b-0e701af7583d/Encounter?patient=12742399&-pageContext=2ddcc8d7-9270-4fe0-afc0-b3ee2511977b&-pageDirection=NEXT`

### MEDITECH

- Probed `15` Bundle fixtures; `4` carry `rel=next`.
- **Bundle.total**: `reflects_returned_match_count` — All 15 captured bundles with a Bundle.total have total==match_entry_count (e.g. tests/golden/meditech/phase-b-2026-04-28/allergyintolerance-bundle.json: total=1, matches=1).
  - Evidence: `tests/golden/meditech/phase-b-2026-04-28/allergyintolerance-bundle.json`
- **Token stability**: `ttl_unknown_continue_cursor_is_resource_typed` — MEDITECH next URL uses `continue=<RESOURCETYPE-prefix>.<UUID>` (e.g. VS./LA./B76C...). No timestamp, no documented TTL.
  - Evidence: `tests/golden/meditech/phase-b-2026-04-28/diagnosticreport-bundle.json`
- **Vital-signs page-size override**: `20` entries on a single page (compare to vendor default).
- **Largest single-page no-rel=next bundles** (per resource): {'AllergyIntolerance': 1, 'CarePlan': 2, 'CareTeam': 1, 'Condition': 14, 'DocumentReference': 7, 'Encounter': 9, 'Goal': 1, 'Immunization': 2, 'MedicationRequest': 16, 'Observation': 2, 'ServiceRequest': 44}
- **Sample next URLs** (truncated to 5):
  - `https://greenfield-prod-apis.meditech.com:443/v2/uscore/STU6/DiagnosticReport?patient=9bec5771-4796-5189-a54e-352c571a44c8&continue=F21A3E95-324B-48B8-A99C-BD0C3CB87BDC`
  - `https://greenfield-prod-apis.meditech.com:443/v2/uscore/STU6/Observation?patient=9bec5771-4796-5189-a54e-352c571a44c8&category=laboratory&continue=LA.0878558F-C87D-4F89-B9C3-533FE13040BD`
  - `https://greenfield-prod-apis.meditech.com:443/v2/uscore/STU6/Observation?patient=9bec5771-4796-5189-a54e-352c571a44c8&category=vital-signs&continue=VS.4EF43488-23EC-411D-8348-56BE9075308D`
  - `https://greenfield-prod-apis.meditech.com:443/v2/uscore/STU6/Procedure?patient=9bec5771-4796-5189-a54e-352c571a44c8&continue=B76C6745-B501-4D63-8DE2-12CE8A81F0A3`

## Known gaps

- **Epic**: only the vital-signs probe ever crossed Epic's page-size cap in our captured corpus, so the empirical `default_page_size` is biased toward the 1000-entry vital-signs ceiling. Non-vital-signs resources (DocumentReference, Observation-lab, etc.) returned ≤70 entries on a single page with no `rel=next` — meaning either they exhausted the match set or sit under a lower per-resource cap we cannot resolve from one-shot fixtures.
- **Cerner**: empty-result Bundle shape unobserved in our corpus (every captured Cerner search returned ≥1 match). KATA-OO-02 captures the OperationOutcome shape on auth/preflight errors; an explicit empty-result capture remains TODO.
- **MEDITECH**: pagination corpus is small (single canonical patient under Phase B 2026-04-28 capture). Token expiry / TTL not measurable offline; would require re-issuing a previously-captured `continue=` cursor against the live sandbox — outside this kata's offline scope. MEDITECH server-side self-link normalization (`:443` appended to host) is consistent across all 15 captures.
- **MEDITECH**: empty-result Bundle shape unobserved (every captured MEDITECH search returned ≥1 match for the bound Greenfield patient). Capture pending under kata #133 (MEDITECH Phase B consent gate).

---

_Generated by `tools/probe_pagination.py`. Re-run with `python -m tools.probe_pagination` after any new golden capture refresh. KATA-CAPTURE-BUNDLE-LINK-SEMANTICS (#136)._
