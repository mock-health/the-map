"""Bulk harvest CapabilityStatement + .well-known/smart-configuration from every
endpoint in a vendor's brands bundle. v2 of The Map's Phase A — sandbox CapStmts
told us what one reference implementation does; this tells us what the deployed
fleet actually exposes.

For each Endpoint resource in the brands bundle:
  GET <address>/metadata                          → capability-statement.json
  GET <address>/.well-known/smart-configuration   → smart-configuration.json
  (both spec-mandated public per FHIR R4 §C.0.0 + SMART STU 2.2)

Failures are RECORDED (`fetch-error.json`), never retried beyond a single 429-honor.
Per-host concurrency is capped (default 2) so we don't hammer any one hospital's
infra. Total politeness: ≤10 RPS sustained, honest User-Agent identifying the
project + contact email, no authenticated probing.

Output structure (one snapshot per `captured_date`):
    tests/golden/production-fleet/{ehr}/{captured_date}/
      _summary.json                       # reachability + status histograms
      _input-brands-bundle.json           # exact bundle file used as input
      <endpoint_slug>/
        capability-statement.json   OR   fetch-error.json
        smart-configuration.json    OR   fetch-error.json

Usage:
    python -m tools.harvest_production_capstmts epic
    python -m tools.harvest_production_capstmts cerner --concurrency 30
    python -m tools.harvest_production_capstmts epic --limit 25 --force
    python -m tools.harvest_production_capstmts epic --brands-file=tests/golden/cross-vendor/epic-r4-endpoints-2026-04-27.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
GOLDEN_CROSS_VENDOR = REPO_ROOT / "tests" / "golden" / "cross-vendor"
GOLDEN_FLEET = REPO_ROOT / "tests" / "golden" / "production-fleet"

USER_AGENT = (
    "mockhealth-map/0.1 (+https://mock.health; nate@mock.health) "
    "production-fleet-harvester"
)
TIMEOUT_SECONDS = 30
DEFAULT_CONCURRENCY = 20
DEFAULT_PER_HOST_CAP = 2

# Map EHR identifier → brands-bundle filename stem (matches tools.fetch_brands).
# Note: "cerner-patient" is an INTERNAL harvest-input alias for Oracle Health's
# patient-launch tier (fhir-myrecord.cerner.com). It is NOT a public ehr slug —
# there is no ehrs/cerner-patient/ directory. The patient harvest under
# tests/golden/production-fleet/cerner-patient/{date}/ is unioned into the
# cerner ehr at analysis time. See tools.analyze_fleet_drift.MULTI_BUNDLE_VENDORS.
EHR_TO_BRANDS_STEM = {
    "epic": "epic-r4-endpoints",
    "cerner": "oracle-health-provider-r4-endpoints",
    "cerner-patient": "oracle-health-patient-r4-endpoints",
    "meditech": "meditech-brands",
}


# ─────────────────── per-host throttling ─────────────────────────────────────
class PerHostThrottle:
    """Caps concurrent requests per hostname. Some Epic/Oracle Health customer
    endpoints share infrastructure (e.g., a regional Haiku tenant hosting many
    hospitals); without this cap a polite-overall 20-worker pool can still hit
    the same host 20-deep."""

    def __init__(self, per_host: int) -> None:
        self.per_host = per_host
        self._lock = threading.Lock()
        self._sems: dict[str, threading.Semaphore] = {}

    def acquire(self, url: str) -> threading.Semaphore:
        host = (urlparse(url).hostname or "").lower()
        with self._lock:
            sem = self._sems.get(host)
            if sem is None:
                sem = threading.Semaphore(self.per_host)
                self._sems[host] = sem
        sem.acquire()
        return sem


# ─────────────────── slug from endpoint address ──────────────────────────────
def endpoint_slug(address: str, *, max_len: int = 96) -> str:
    """Filename-safe slug derived from the endpoint URL. Stable: same address
    always yields the same slug so quarterly re-runs overwrite cleanly."""
    p = urlparse(address)
    raw = f"{p.hostname or 'unknown'}{p.path or ''}"
    raw = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw).strip("_")
    if len(raw) > max_len:
        raw = raw[:max_len].rstrip("_")
    return raw or "unknown"


# ─────────────────── single-endpoint fetch ───────────────────────────────────
def _fetch_with_retry(url: str, *, accept: str, throttle: PerHostThrottle) -> tuple[int | None, dict | str | None, dict]:
    """One fetch with at most one retry on HTTP 429 honoring Retry-After.
    Returns (status_code or None, parsed_body or raw_text or None, meta).
    meta carries timing + error classification."""
    sem = throttle.acquire(url)
    try:
        attempt = 0
        while True:
            attempt += 1
            t0 = time.monotonic()
            try:
                r = requests.get(
                    url,
                    headers={"Accept": accept, "User-Agent": USER_AGENT},
                    timeout=TIMEOUT_SECONDS,
                    allow_redirects=True,
                )
            except requests.exceptions.SSLError as e:
                return None, None, {"kind": "tls", "error": str(e)[:300], "elapsed_ms": int((time.monotonic() - t0) * 1000)}
            except requests.exceptions.ConnectionError as e:
                # Distinguish DNS from other connection failures
                cause = str(e)
                kind = "dns" if "Name or service not known" in cause or "NameResolutionError" in cause or "Could not resolve host" in cause else "connection"
                return None, None, {"kind": kind, "error": cause[:300], "elapsed_ms": int((time.monotonic() - t0) * 1000)}
            except requests.exceptions.ReadTimeout:
                return None, None, {"kind": "timeout", "error": f"read timeout after {TIMEOUT_SECONDS}s", "elapsed_ms": int((time.monotonic() - t0) * 1000)}
            except requests.exceptions.RequestException as e:
                return None, None, {"kind": "other", "error": f"{type(e).__name__}: {str(e)[:300]}", "elapsed_ms": int((time.monotonic() - t0) * 1000)}

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            meta = {"kind": "http", "elapsed_ms": elapsed_ms, "final_url": r.url}
            if r.status_code == 429 and attempt == 1:
                retry_after = r.headers.get("Retry-After", "1")
                try:
                    delay = max(1.0, min(float(retry_after), 30.0))
                except ValueError:
                    delay = 5.0
                time.sleep(delay)
                continue
            ct = (r.headers.get("Content-Type") or "").lower()
            if r.status_code >= 400:
                return r.status_code, r.text[:1000], {**meta, "kind": "http_error", "content_type": ct}
            if "json" in ct or r.text.lstrip().startswith("{"):
                try:
                    return r.status_code, r.json(), {**meta, "content_type": ct}
                except json.JSONDecodeError as e:
                    return r.status_code, r.text[:1000], {**meta, "kind": "parse", "error": str(e), "content_type": ct}
            return r.status_code, r.text[:1000], {**meta, "kind": "non_json", "content_type": ct}
    finally:
        sem.release()


def fetch_one_endpoint(address: str, out_dir: Path, throttle: PerHostThrottle, *, force: bool) -> dict:
    """Fetch /metadata + /.well-known/smart-configuration for one endpoint.
    Writes both results (or fetch-error.json sidecars) into out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    base = address.rstrip("/")
    record = {
        "endpoint_address": address,
        "endpoint_slug": out_dir.name,
        "host": urlparse(address).hostname,
        "capstmt": None,
        "smart_config": None,
    }

    # Fetch CapStmt
    cap_path = out_dir / "capability-statement.json"
    err_path = out_dir / "capability-statement.fetch-error.json"
    if force or not (cap_path.exists() or err_path.exists()):
        if cap_path.exists():
            cap_path.unlink()
        if err_path.exists():
            err_path.unlink()
        status, body, meta = _fetch_with_retry(f"{base}/metadata", accept="application/fhir+json", throttle=throttle)
        if status is not None and 200 <= status < 300 and isinstance(body, dict) and body.get("resourceType") == "CapabilityStatement":
            cap_path.write_text(json.dumps(body, indent=2) + "\n")
            record["capstmt"] = {"status": status, "elapsed_ms": meta.get("elapsed_ms"), "ok": True}
        else:
            err = {
                "url": f"{base}/metadata",
                "http_status": status,
                "category": _classify_failure(status, body, meta),
                "meta": meta,
                "body_preview": body if isinstance(body, str) else (json.dumps(body)[:500] if body else None),
            }
            err_path.write_text(json.dumps(err, indent=2) + "\n")
            record["capstmt"] = {"status": status, "elapsed_ms": meta.get("elapsed_ms"), "ok": False, "category": err["category"]}
    else:
        record["capstmt"] = {"ok": cap_path.exists(), "skipped": True}

    # Fetch SMART config
    sm_path = out_dir / "smart-configuration.json"
    sm_err = out_dir / "smart-configuration.fetch-error.json"
    if force or not (sm_path.exists() or sm_err.exists()):
        if sm_path.exists():
            sm_path.unlink()
        if sm_err.exists():
            sm_err.unlink()
        status, body, meta = _fetch_with_retry(f"{base}/.well-known/smart-configuration", accept="application/json", throttle=throttle)
        if status is not None and 200 <= status < 300 and isinstance(body, dict):
            sm_path.write_text(json.dumps(body, indent=2) + "\n")
            record["smart_config"] = {"status": status, "elapsed_ms": meta.get("elapsed_ms"), "ok": True}
        else:
            err = {
                "url": f"{base}/.well-known/smart-configuration",
                "http_status": status,
                "category": _classify_failure(status, body, meta),
                "meta": meta,
                "body_preview": body if isinstance(body, str) else (json.dumps(body)[:500] if body else None),
            }
            sm_err.write_text(json.dumps(err, indent=2) + "\n")
            record["smart_config"] = {"status": status, "elapsed_ms": meta.get("elapsed_ms"), "ok": False, "category": err["category"]}
    else:
        record["smart_config"] = {"ok": sm_path.exists(), "skipped": True}

    return record


def _classify_failure(status: int | None, body, meta: dict) -> str:
    kind = (meta or {}).get("kind", "")
    if kind in {"dns", "tls", "timeout", "connection", "parse", "non_json"}:
        return kind
    if status is None:
        return kind or "other"
    if 400 <= status < 500:
        return f"http_4xx_{status}"
    if 500 <= status < 600:
        return f"http_5xx_{status}"
    return "other"


# ─────────────────── brands-bundle loading ───────────────────────────────────
def latest_brands_file(stem: str) -> Path | None:
    """Pick the most recent dated archive for `stem` under tests/golden/cross-vendor/."""
    if not GOLDEN_CROSS_VENDOR.exists():
        return None
    candidates = sorted(GOLDEN_CROSS_VENDOR.glob(f"{stem}-*.json"))
    return candidates[-1] if candidates else None


def load_endpoints(bundle_path: Path) -> tuple[list[dict], dict]:
    """Returns (endpoint_list, source_metadata).
    Each endpoint is {"address": str, "id": str, "managing_org": str|None}."""
    bundle = json.loads(bundle_path.read_text())
    endpoints: list[dict] = []
    for entry in bundle.get("entry", []):
        r = entry.get("resource", {}) if isinstance(entry, dict) else {}
        if r.get("resourceType") != "Endpoint":
            continue
        addr = r.get("address")
        if not addr or not addr.startswith(("http://", "https://")):
            continue
        # name may live in resource.name (Epic) or be derived from managingOrganization (others)
        name = r.get("name")
        endpoints.append({
            "address": addr,
            "id": r.get("id") or "",
            "name": name,
            "managing_organization": (r.get("managingOrganization") or {}).get("reference"),
        })
    src = {
        "bundle_path": str(bundle_path.relative_to(REPO_ROOT)),
        "bundle_total": bundle.get("total"),
        "bundle_entries": len(bundle.get("entry", [])),
    }
    return endpoints, src


# ─────────────────── orchestrator ────────────────────────────────────────────
def harvest(ehr: str, *, brands_file: Path | None, concurrency: int, per_host: int, limit: int | None, force: bool, captured_date: str | None) -> dict:
    if brands_file is None:
        stem = EHR_TO_BRANDS_STEM.get(ehr)
        if not stem:
            sys.exit(f"ERROR: no brands-bundle stem registered for ehr={ehr!r}; pass --brands-file=PATH")
        brands_file = latest_brands_file(stem)
        if brands_file is None:
            sys.exit(f"ERROR: no archive matching '{stem}-*.json' under {GOLDEN_CROSS_VENDOR.relative_to(REPO_ROOT)}/. Run `python -m tools.fetch_brands {ehr}` first.")

    endpoints, src = load_endpoints(brands_file)
    if not endpoints:
        sys.exit(f"ERROR: no Endpoint resources in {brands_file}")

    if limit:
        endpoints = endpoints[:limit]

    captured_date = captured_date or datetime.date.today().isoformat()
    out_root = GOLDEN_FLEET / ehr / captured_date
    out_root.mkdir(parents=True, exist_ok=True)

    # Snapshot the input bundle so the harvest is self-describing
    input_copy = out_root / "_input-brands-bundle.json"
    if force or not input_copy.exists():
        shutil.copy2(brands_file, input_copy)

    print(f"\nHarvesting {len(endpoints)} endpoints from {brands_file.relative_to(REPO_ROOT)}")
    print(f"  output: {out_root.relative_to(REPO_ROOT)}/")
    print(f"  concurrency: {concurrency} workers, max {per_host} per host")
    print(f"  user-agent: {USER_AGENT}")

    throttle = PerHostThrottle(per_host)
    t0 = time.monotonic()
    results: list[dict] = []

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(fetch_one_endpoint, ep["address"], out_root / endpoint_slug(ep["address"]), throttle, force=force): ep
            for ep in endpoints
        }
        for i, fut in enumerate(as_completed(futures), 1):
            ep = futures[fut]
            try:
                rec = fut.result()
                rec["managing_organization"] = ep.get("managing_organization")
                rec["endpoint_name"] = ep.get("name")
                results.append(rec)
            except Exception as e:
                results.append({
                    "endpoint_address": ep["address"],
                    "endpoint_slug": endpoint_slug(ep["address"]),
                    "host": urlparse(ep["address"]).hostname,
                    "exception": f"{type(e).__name__}: {e}",
                })
            if i % 25 == 0 or i == len(endpoints):
                ok_cap = sum(1 for r in results if (r.get("capstmt") or {}).get("ok"))
                ok_sm = sum(1 for r in results if (r.get("smart_config") or {}).get("ok"))
                print(f"  [{i:>4}/{len(endpoints)}]  cap_ok={ok_cap}  smart_ok={ok_sm}  elapsed={int(time.monotonic() - t0)}s")

    elapsed = time.monotonic() - t0

    # Aggregate summary
    summary = _build_summary(ehr, captured_date, brands_file, src, results, elapsed=elapsed, concurrency=concurrency, per_host=per_host)
    (out_root / "_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"\nDone. {summary['endpoints_attempted']} endpoints attempted in {int(elapsed)}s")
    print(f"  capstmt OK: {summary['capstmt_fetched_ok']}/{summary['endpoints_attempted']}  ({100 * summary['capstmt_fetched_ok'] // max(1, summary['endpoints_attempted'])}%)")
    print(f"  smart_config OK: {summary['smart_config_fetched_ok']}/{summary['endpoints_attempted']}  ({100 * summary['smart_config_fetched_ok'] // max(1, summary['endpoints_attempted'])}%)")
    if summary["failure_categories"]:
        print(f"  failure categories: {summary['failure_categories']}")
    print(f"  summary: {(out_root / '_summary.json').relative_to(REPO_ROOT)}")
    return summary


def _build_summary(ehr: str, captured_date: str, brands_file: Path, src: dict, results: list[dict], *, elapsed: float, concurrency: int, per_host: int) -> dict:
    cap_ok = sum(1 for r in results if (r.get("capstmt") or {}).get("ok"))
    sm_ok = sum(1 for r in results if (r.get("smart_config") or {}).get("ok"))
    failures: dict[str, int] = {}
    for r in results:
        for key in ("capstmt", "smart_config"):
            sub = r.get(key) or {}
            if not sub.get("ok") and sub.get("category"):
                failures[sub["category"]] = failures.get(sub["category"], 0) + 1
    return {
        "ehr": ehr,
        "captured_date": captured_date,
        "brands_bundle_source_url_or_path": src["bundle_path"],
        "brands_bundle_total_endpoints": len(results),
        "endpoints_attempted": len(results),
        "capstmt_fetched_ok": cap_ok,
        "capstmt_fetch_failed": len(results) - cap_ok,
        "smart_config_fetched_ok": sm_ok,
        "smart_config_fetch_failed": len(results) - sm_ok,
        "failure_categories": dict(sorted(failures.items(), key=lambda kv: -kv[1])),
        "wall_clock_seconds": round(elapsed, 1),
        "concurrency": concurrency,
        "per_host_cap": per_host,
        "per_endpoint": [
            {
                "address": r["endpoint_address"],
                "slug": r["endpoint_slug"],
                "host": r["host"],
                "managing_organization": r.get("managing_organization"),
                "name": r.get("endpoint_name"),
                "capstmt_ok": (r.get("capstmt") or {}).get("ok", False),
                "capstmt_status": (r.get("capstmt") or {}).get("status"),
                "capstmt_category": (r.get("capstmt") or {}).get("category"),
                "smart_config_ok": (r.get("smart_config") or {}).get("ok", False),
                "smart_config_status": (r.get("smart_config") or {}).get("status"),
                "smart_config_category": (r.get("smart_config") or {}).get("category"),
            }
            for r in sorted(results, key=lambda x: x.get("endpoint_address", ""))
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ehr", help="EHR identifier (epic, cerner, meditech)")
    ap.add_argument("--brands-file", help="Path to a brands-bundle JSON. Defaults to most recent dated archive under tests/golden/cross-vendor/.")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    ap.add_argument("--per-host", type=int, default=DEFAULT_PER_HOST_CAP)
    ap.add_argument("--limit", type=int, help="Limit to first N endpoints (for testing)")
    ap.add_argument("--force", action="store_true", help="Refetch endpoints even if output already exists")
    ap.add_argument("--captured-date", help="Override the captured_date directory (default: today). Useful for resuming an interrupted harvest.")
    args = ap.parse_args()

    bf = Path(args.brands_file) if args.brands_file else None
    if bf and not bf.is_absolute():
        bf = REPO_ROOT / bf
    if bf and not bf.exists():
        sys.exit(f"ERROR: --brands-file not found: {bf}")

    harvest(
        args.ehr,
        brands_file=bf,
        concurrency=args.concurrency,
        per_host=args.per_host,
        limit=args.limit,
        force=args.force,
        captured_date=args.captured_date,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
