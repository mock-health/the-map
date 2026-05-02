# Contributing to The Map

Thank you for considering a contribution. The Map gets more credible every time a vendor or integrator corrects it.

There are **two contributor paths**, depending on whether you have sandbox credentials:

1. **Phase A — doc-only path.** No credentials. You cite vendor public docs to add or correct a row.
2. **Phase 1+ — sandbox path.** Sign up for the vendor's developer program, run the sweep tools locally, open a PR with the updated overlay + golden fixtures.

Both are first-class. Most vendor PRs land via the doc-only path; integrators reproducing a deviation in their own integration usually use the sandbox path.

## The iron rule

**Every cited claim has `source_url`, `source_quote`, `verified_via`, and `verified_date`.** No exceptions.

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

Schema-enforced. CI-gated. Don't try to land a row without one.

`verified_via` is an enum:

- `vendor_official_docs` — citing a vendor's published documentation
- `epic_public_sandbox` / `cerner_public_sandbox` / `meditech_public_sandbox` / `ecw_public_sandbox` / `veradigm_public_sandbox` / `nextgen_public_sandbox` — observed in a vendor's anonymous sandbox
- `customer_evidence` — observed in a customer's production FHIR endpoint (with the customer's permission)
- `community_report` — community contribution; lower-trust until corroborated
- `vendor_pr` — PR submitted by the vendor's developer-relations team

If your contribution doesn't fit any of these, open an issue first.

## Setup

```bash
git clone https://github.com/mock-health/the-map.git
cd the-map
python -m venv .venv && . .venv/bin/activate
pip install -e .[dev]
make validate     # confirm everything works
```

If you need the production-fleet evidence (380 MB across 2,474 endpoints), run `git lfs pull` after cloning. Most contributors don't need it — the rolled-up summaries in `ehrs/{ehr}/production_fleet.json` are sufficient.

## Path 1: Phase A doc-only contribution

Use this when you spot a row that's wrong, missing, or undercited, and you can fix it from public vendor documentation alone.

1. Find the row in `ehrs/{ehr}/overlay.json` (or add a new one to `element_deviations[]`).
2. Update it with the correct claim and a complete `verification` block.
3. Run `make validate` — schema check passes.
4. Run `python -m tools.synthesize {ehr}` and `python -m tools.render_html {ehr}` to confirm the change renders sanely.
5. Open a PR. CI will re-validate; reviewer takes a look; we merge.

**Fast track:** if you work on the vendor's developer-relations team and you're correcting your own row, mention it in the PR description. We prioritize vendor-PR review.

## Path 2: Phase 1+ sandbox contribution

Use this when you can sign up for a vendor's sandbox and reproduce a deviation in a multi-patient sweep. This produces stronger evidence than docs alone — golden fixtures that future PRs can cite.

### Per-vendor sandbox program signup

Each vendor offers a free developer sandbox; sign up for the ones you want to test against:

- **Epic on FHIR** — https://fhir.epic.com/. JWT-signed Backend Services (`client_credentials`, RS384). Sandbox patients include Derrick Lin, Camila Lopez, Jason Argonaut.
- **Cerner Code (Oracle Health)** — https://code.cerner.com/. Open tier (`fhir-open.cerner.com`) anonymous; secure tier (`fhir-ehr.cerner.com`) needs `client_secret_basic`. Sandbox tenant: `ec2458f2-1e24-41c8-b71b-0e701af7583d`.
- **MEDITECH Greenfield** — https://developer.meditech.com/. SMART standalone-launch. Greenfield is MEDITECH's vendor-managed sandbox; consent ties to a Google account registered with Greenfield.

Once you have credentials, copy `.env.example` to `.env` and fill in the relevant variables for the vendor(s) you signed up for. **Phase A and Phase F still run with no `.env` at all** — you only need credentials for Phase 1+.

### The pipeline

```bash
# Re-fetch the CapStmt in case it drifted
python -m tools.fetch_capability epic

# Build a multi-patient catalog
python -m tools.enumerate_sandbox_patients epic

# Multi-patient × four-axis sweep (the meat)
python -m tools.measure_phase_b epic

# Search-param coverage, $export, reference resolution
python -m tools.probe_search_bulk_refs epic

# Optional: HAPI cross-validation
docker compose -f tools/hapi/docker-compose.example.yml up -d
python -m tools.upload_us_core_to_hapi
python -m tools.cross_validate_with_hapi epic

# Optional: Inferno
python -m tools.run_inferno_us_core epic   # see the script's docstring for setup

# Phase 6: token-lifecycle and transport-edge probes
python -m tools.probe_transport epic

# Synthesize → render → validate
python -m tools.synthesize epic
python -m tools.render_html epic
python -m tools.validate
```

The pipeline writes:

- `ehrs/{ehr}/overlay.json` — the claims (element deviations, multi-patient evidence, OperationOutcome shapes, pagination, etc.)
- `tests/golden/{ehr}/phase-b-{date}/` — verbatim captured responses (the audit trail; cite these as `source_url` for sandbox-evidence claims)
- `reports/2026-q2/html/{ehr}/` — local rendered HTML for browsing your changes before pushing

Open a PR with the updated `overlay.json` + the new golden fixtures.

### Adding a new EHR

See [`DESIGN.md`](./DESIGN.md) "Adding a new EHR (concrete walkthrough)." Short version:

1. Add the EHR to `tools/auth_flows/__init__.py:EHR_CONFIG`
2. If the auth flow is new (not `client_credentials` or `auth_code`), add a module to `tools/auth_flows/`
3. Add the EHR's documented test patient roster to `tools/enumerate_sandbox_patients.py:KNOWN_PATIENT_ROSTERS`
4. Run the pipeline; everything else is EHR-agnostic
5. Hand-write `ehrs/{new-ehr}/overlay.json:phase_b_findings.headlines` based on the sweep output

We're explicitly looking for: eCW, NextGen, Veradigm. Contact security@mock.health if you're a vendor team interested in claiming your row.

## Refreshing an EHR's data

When a vendor pushes a CapStmt update, the rendered map will eventually flag a yellow staleness banner (after 90 days; red after 365 days). The fix is to re-run Phase A and re-validate:

```bash
python -m tools.fetch_capability {ehr}     # writes new ehrs/{ehr}/CapabilityStatement.json
python -m tools.validate {ehr}              # confirm overlay claims still apply
```

`.github/workflows/refresh-capstmts.yml` does this weekly across every EHR and opens a "drift detected" PR automatically. You can run it manually too: GitHub → Actions → refresh-capstmts → Run workflow.

If a deviation row's underlying CapStmt element changed shape (e.g., the vendor now emits the field), update the overlay row's `deviation_category` and add a fresh `verification` block.

## Code style

```bash
make lint        # ruff check
make typecheck   # mypy (advisory in v1.0; not blocking)
make test        # pytest
```

Pre-commit, `make lint && make test` should pass. CI runs the same on every PR.

## Code of conduct

This project follows the [Contributor Covenant 2.1](./CODE_OF_CONDUCT.md). In short: don't be a jerk; be patient with people learning FHIR; vendor critique is welcome but personal attacks are not.

## Security

If you find a credential leak, an XSS in the rendered HTML, or a way for a malicious overlay PR to compromise downstream consumers, please follow [`SECURITY.md`](./SECURITY.md) — don't open a public issue.

## Questions

Open a GitHub Discussion or email hello@mock.health.
