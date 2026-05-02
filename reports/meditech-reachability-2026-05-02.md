# Meditech reachability diagnostic — 2026-04-27

Re-tested **181** of the 190 Meditech endpoints that failed in the most recent
harvest snapshot. Each was probed with two User-Agents (default + browser-style)
to surface vendor-side scraping blocks vs genuine endpoint death.

> **Methodology note**: The 9-endpoint gap (190 fetch failures in `harvest_summary` vs. 181 retested here) covers endpoints whose original failure category did not produce a dns/tls/http classifier signal usable by the User-Agent comparison — primarily `non_json` responses and a handful of `connection`-class errors that fell outside the dns/tls/http buckets in `tools/diagnose_meditech_reachability.py:124-131`. Per-category sums in this report total exactly 181 (53+47+44+17+10+4+2+1+1+1+1).

## Default User-Agent results

| failure category | count | share |
|---|---:|---:|
| `tls_timeout` | 53 | 29% |
| `dns_dead` | 47 | 26% |
| `tcp_refused` | 44 | 24% |
| `http_400` | 17 | 9% |
| `http_403` | 10 | 6% |
| `http_503` | 4 | 2% |
| `http_error_ConnectionError` | 2 | 1% |
| `http_525` | 1 | 1% |
| `http_500` | 1 | 1% |
| `http_526` | 1 | 1% |
| `http_error_ReadTimeout` | 1 | 1% |

## Browser User-Agent results

| failure category | count | share |
|---|---:|---:|
| `tls_timeout` | 53 | 29% |
| `dns_dead` | 47 | 26% |
| `tcp_refused` | 44 | 24% |
| `http_400` | 17 | 9% |
| `http_403` | 10 | 6% |
| `http_503` | 4 | 2% |
| `http_error_ConnectionError` | 2 | 1% |
| `http_525` | 1 | 1% |
| `http_500` | 1 | 1% |
| `http_526` | 1 | 1% |
| `http_error_ReadTimeout` | 1 | 1% |

## Top failing host domains

| host | failed endpoints |
|---|---:|
| `hca.cloud` | 10 |
| `catholichealth.net` | 9 |
| `christushealth.org` | 7 |
| `trinity-health.org` | 7 |
| `steward.org` | 6 |

## Recommendation

- **dns_dead / tcp_refused / tls_handshake_failure** dominating ⇒ the failures
  are *infrastructure*, not us. Document in CHANGELOG as a known limitation;
  the endpoints are unreachable from anywhere.
- **alive_now > 0** ⇒ some failures were transient at harvest time. Consider
  adding a single retry-after-60s pass to `harvest_production_capstmts.py`.
- **default vs browser UA divergence > 5%** ⇒ vendor (or hospital infra) is
  filtering on User-Agent. Consider switching to a browser-style UA.
- **One host with >10% of failures** ⇒ vendor-side outage; flag the cluster.

Source: `tools/diagnose_meditech_reachability.py` against
`tests/golden/production-fleet/meditech/2026-04-27/`.
