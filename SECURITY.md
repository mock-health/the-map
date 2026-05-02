# Security Policy

## Reporting a vulnerability

The Map is a public data + tooling artefact. The most likely security issues we worry about:

- **Credential leakage** — a `.env`, OAuth refresh token, private key, or API key accidentally committed to the repo or to one of its golden fixtures.
- **XSS in rendered HTML** — a malicious overlay row's `source_quote` rendered without escape, allowing script injection on `mock.health/map/{ehr}/...`.
- **Path traversal in vendor PRs** — an overlay row whose `path` field contains `../` and gets used as a filesystem path by any tool.
- **Schema bypass** — a way to land a row that violates the iron rule (missing `verification`, `STUB` source quote) without CI catching it.
- **LFS content abuse** — a PR that adds attacker-controlled binaries to `tests/golden/production-fleet/` via Git LFS.
- **Supply chain** — a malicious dependency or workflow change that runs in CI with elevated permissions.

If you find any of these, please report privately. Do not open a public GitHub issue.

**Email: security@mock.health**

Include:

- A description of the issue and its potential impact
- Steps to reproduce (or a minimal proof of concept)
- Whether you've disclosed publicly anywhere else (and we'll respect a coordinated disclosure timeline)

## Response timeline

We aim to respond to reports within **48 hours** with an acknowledgement, and to triage and propose a fix within **7 days** for the categories above. Coordinated disclosure follows a standard **90-day** window — we'll work with you on disclosure timing once we've confirmed the issue.

## Scope

In scope:

- This GitHub repository (`mock-health/the-map`)
- The Python pipeline under `tools/`
- The schemas under `schema/`
- The CI workflows under `.github/workflows/`
- The published exported data (`make export` output)

Out of scope here (report to the appropriate vendor or to mock.health directly):

- Bugs or vulnerabilities in the EHR vendors' actual FHIR endpoints (report those to the vendor's developer-relations team)
- Vulnerabilities in `mock.health/map` itself or other mock.health properties (email security@mock.health)
- Vulnerabilities in upstream dependencies (we'll forward; you should also report to the upstream)

## Recognition

We're happy to credit security researchers who report responsibly. If you'd like a mention in the CHANGELOG when a fix lands, say so in your report.
