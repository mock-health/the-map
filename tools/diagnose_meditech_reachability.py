"""Re-test Meditech endpoints that failed in the most recent harvest.

Why: ehrs/meditech/production_fleet.json shows 65% capstmt-fetch reachability
(352/542). Failures are dominated by connection / DNS / TLS errors. Before
launch we want evidence that this is genuine vendor-side fragility (some
hospitals' VPN-fronted FHIR endpoints are intermittently down), not an artifact
of our scraping (User-Agent blocked, throttle too aggressive).

Output: reports/meditech-reachability-{captured_date}.md with a classification
table and a recommendation. Network-heavy; runs against the live failures list.

Usage:
    python -m tools.diagnose_meditech_reachability
    python -m tools.diagnose_meditech_reachability --max-endpoints 20
"""
from __future__ import annotations

import argparse
import json
import socket
import ssl
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
FLEET_PATH = REPO_ROOT / "ehrs" / "meditech" / "production_fleet.json"
HARVEST_DIR = REPO_ROOT / "tests" / "golden" / "production-fleet" / "meditech"

DEFAULT_UA = "the-map-meditech-diagnostic/1.0 (+https://github.com/mock-health/the-map)"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.5 Safari/605.1.15"
)


def _latest_harvest_dir() -> Path | None:
    if not HARVEST_DIR.is_dir():
        return None
    candidates = sorted([p for p in HARVEST_DIR.iterdir() if p.is_dir()])
    return candidates[-1] if candidates else None


def _failed_endpoints_from_harvest() -> list[dict]:
    """Walk the most recent harvest snapshot for fetch-error sidecars.
    Returns [{address, slug, error_kind, status, reason}, ...]."""
    snap = _latest_harvest_dir()
    if snap is None:
        return []
    failed: list[dict] = []
    for slug_dir in snap.iterdir():
        if not slug_dir.is_dir():
            continue
        err = slug_dir / "capability-statement.fetch-error.json"
        if not err.exists():
            continue
        try:
            payload = json.loads(err.read_text())
        except json.JSONDecodeError:
            continue
        failed.append({
            "slug": slug_dir.name,
            "address": payload.get("address") or payload.get("url") or "",
            "error_kind": payload.get("category") or payload.get("kind") or "unknown",
            "status": payload.get("status"),
            "reason": payload.get("error") or payload.get("reason") or "",
        })
    return failed


def _classify(address: str, *, ua: str, timeout: int) -> dict:
    """Return classification for a single endpoint."""
    parsed = urlparse(address)
    host = parsed.netloc.split(":")[0] or ""
    out = {"address": address, "host": host, "dns": None, "tls": None, "http": None, "headers_used": ua}
    # DNS
    try:
        socket.getaddrinfo(host, None)
        out["dns"] = "ok"
    except socket.gaierror as e:
        out["dns"] = f"NXDOMAIN ({e})"
        return out

    # TLS handshake
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, parsed.port or 443), timeout=timeout) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                out["tls"] = ssock.version() or "ok"
    except TimeoutError:
        out["tls"] = "timeout"
        return out
    except (ConnectionRefusedError, OSError) as e:
        out["tls"] = f"refused ({type(e).__name__})"
        return out
    except ssl.SSLError as e:
        out["tls"] = f"tls-error ({e.reason or e})"
        return out

    # HTTP /metadata
    try:
        import requests
    except ImportError:
        out["http"] = "requests not installed"
        return out
    try:
        url = address.rstrip("/") + "/metadata"
        r = requests.get(
            url,
            headers={"User-Agent": ua, "Accept": "application/fhir+json"},
            timeout=timeout,
            allow_redirects=True,
        )
        out["http"] = r.status_code
    except Exception as e:
        out["http"] = f"{type(e).__name__}"
    return out


def _judge(c: dict) -> str:
    if c["dns"] != "ok":
        return "dns_dead"
    if isinstance(c["tls"], str) and c["tls"].startswith(("timeout",)):
        return "tls_timeout"
    if isinstance(c["tls"], str) and c["tls"].startswith(("refused",)):
        return "tcp_refused"
    if isinstance(c["tls"], str) and c["tls"].startswith(("tls-error",)):
        return "tls_handshake_failure"
    if isinstance(c["http"], int):
        if c["http"] == 200:
            return "alive_now"  # transient at harvest time
        if 200 <= c["http"] < 400:
            return f"alive_status_{c['http']}"
        return f"http_{c['http']}"
    return f"http_error_{c['http']}"


def _render_report(records: list[dict], *, captured_date: str) -> str:
    by_judgment_default = Counter(_judge(r["default"]) for r in records)
    by_judgment_browser = Counter(_judge(r["browser"]) for r in records)

    def _table(c: Counter) -> str:
        rows = sorted(c.items(), key=lambda x: -x[1])
        return "\n".join(f"| `{k}` | {v} | {v / len(records):.0%} |" for k, v in rows)

    # Concentration by registered domain (eTLD+1) — vendor-side blocks (WAF, IP
    # allowlist) typically share a parent domain like `exp.hca.cloud` even when
    # each endpoint has a unique subdomain.
    def _registered_domain(host: str) -> str:
        parts = host.split(".")
        if len(parts) <= 2:
            return host
        # heuristic: keep last 3 labels for known multi-label TLDs, else last 2.
        if parts[-2] in {"co", "com", "net", "org", "ac", "gov"} and len(parts[-1]) == 2:
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])

    domain_counts: dict[str, int] = defaultdict(int)
    for r in records:
        domain_counts[_registered_domain(r["host"])] += 1
    top_domains = sorted(domain_counts.items(), key=lambda x: -x[1])[:5]

    return f"""# Meditech reachability diagnostic — {captured_date}

Re-tested **{len(records)}** Meditech endpoints that failed in the most recent
harvest snapshot. Each was probed with two User-Agents (default + browser-style)
to surface vendor-side scraping blocks vs genuine endpoint death.

## Default User-Agent results

| failure category | count | share |
|---|---:|---:|
{_table(by_judgment_default)}

## Browser User-Agent results

| failure category | count | share |
|---|---:|---:|
{_table(by_judgment_browser)}

## Top failing host domains

| host | failed endpoints |
|---|---:|
{chr(10).join(f"| `{h}` | {n} |" for h, n in top_domains)}

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
`tests/golden/production-fleet/meditech/{captured_date}/`.
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-endpoints", type=int, default=None,
                    help="Cap number of endpoints to test (default: all)")
    ap.add_argument("--timeout", type=int, default=10)
    ap.add_argument("--max-workers", type=int, default=10)
    ap.add_argument("--out-dir", type=Path, default=REPO_ROOT / "reports")
    args = ap.parse_args()

    if not FLEET_PATH.exists():
        sys.stderr.write(f"ERROR: {FLEET_PATH} missing\n")
        return 1
    fleet = json.loads(FLEET_PATH.read_text())
    captured_date = fleet.get("captured_date", "unknown")

    failed = _failed_endpoints_from_harvest()
    if args.max_endpoints:
        failed = failed[: args.max_endpoints]
    if not failed:
        sys.stderr.write("No failed-endpoint sidecars found; nothing to diagnose.\n")
        return 0

    sys.stderr.write(f"Re-testing {len(failed)} failed Meditech endpoints (2× UA)...\n")

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {}
        for f in failed:
            addr = f["address"]
            futures[ex.submit(_classify, addr, ua=DEFAULT_UA, timeout=args.timeout)] = ("default", addr, f)
            futures[ex.submit(_classify, addr, ua=BROWSER_UA, timeout=args.timeout)] = ("browser", addr, f)
        bucket: dict[str, dict] = {}
        for fut in as_completed(futures):
            kind, addr, original = futures[fut]
            res = fut.result()
            bucket.setdefault(addr, {"address": addr, "host": res["host"], "original": original})[kind] = res
        results = list(bucket.values())

    # Drop endpoints that didn't get both probes (shouldn't happen but be safe).
    results = [r for r in results if "default" in r and "browser" in r]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    today = time.strftime("%Y-%m-%d")
    out = args.out_dir / f"meditech-reachability-{today}.md"
    out.write_text(_render_report(results, captured_date=captured_date))
    sys.stderr.write(f"Wrote {out.relative_to(REPO_ROOT)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
