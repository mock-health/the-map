<!--
Thanks for opening a PR!

Before submitting, please confirm:

- [ ] You're following the iron rule: every cited claim in your overlay change has source_url, source_quote, verified_via, and verified_date.
- [ ] You ran `make validate` locally and it passes.
- [ ] If your change affects synthesis or rendering, you ran `make render EHR=<ehr>` and visually inspected the output.
- [ ] If you added or modified a test fixture, the fixture is under tests/golden/{ehr}/ with a dated subdirectory.
- [ ] CI will run ruff + mypy + pytest + tools.validate + smoke render. Wait for it to go green before requesting review.
-->

## What does this PR do?

<!-- One-sentence summary. -->

## Why?

<!-- The motivation. If this corrects a vendor row, link to the vendor doc that disagrees with the previous claim. -->

## Type of change

- [ ] **Phase A doc-only** — citing vendor public docs to add or correct a row
- [ ] **Phase 1+ sandbox** — observed in a vendor's sandbox; includes new golden fixtures
- [ ] **New EHR** — adding a vendor not previously mapped
- [ ] **Tooling / pipeline** — code changes under `tools/` or `tests/`
- [ ] **Docs** — README, METHODOLOGY, DESIGN, CONTRIBUTING, etc.
- [ ] **Other** — explain below

## Iron-rule check (overlay changes only)

For each row added or modified, paste the `verification` block here:

```json
{
  "source_url": "...",
  "source_quote": "...",
  "verified_via": "...",
  "verified_date": "YYYY-MM-DD"
}
```

(Or N/A if this PR doesn't touch any cited claim.)

## Vendor dev-relations PR?

If you work on a vendor's dev-relations team and this PR corrects your own row, mention it here:

- Vendor: <!-- e.g. Epic, Oracle Health, MEDITECH -->
- Your role: <!-- e.g. FHIR API product manager -->

We'll fast-track review.

## Anything else?
