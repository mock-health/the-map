"""HEAD-check every source_url across all ehrs/*/overlay.json.

Runs out-of-band (weekly cron, not per-PR) because it's network-dependent and
external sites time out for legitimate reasons. Output is a JSON report on
stdout plus a human-readable summary on stderr; exit code is non-zero only when
permanent failures are detected (4xx/5xx, NXDOMAIN). Transient failures
(timeout, connection reset) are reported as WARN and don't fail the run.

Usage:
    python tools/check_source_urls.py
    python tools/check_source_urls.py --vendor epic
    python tools/check_source_urls.py --max-workers 5

Skip targets:
  - Sentinel "(see paired golden fixture)" — not a URL.
  - URLs starting with tests/golden/ or ehrs/ — local repo paths used by the
    production_fleet schema.
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent
EHRS_DIR = REPO_ROOT / "ehrs"

SENTINELS = {"(see paired golden fixture)"}
LOCAL_PATH_PREFIXES = ("tests/golden/", "ehrs/")
ALLOWED_REDIRECT_CODES = {301, 302, 303, 307, 308}
USER_AGENT = "the-map-source-url-check/1.0 (+https://github.com/mock-health/the-map)"


def _walk_overlay_urls(ov_path: Path):
    ov = json.loads(ov_path.read_text())
    ehr = ov.get("ehr") or ov_path.parent.name

    def _emit(label: str, v):
        if isinstance(v, dict) and "source_url" in v:
            yield (ehr, label, v.get("source_url", ""), v.get("verified_via", ""))

    for top in ("auth_overlay", "operation_outcome_overlay", "pagination_overlay",
                "phase_b_findings", "multi_patient_coverage", "access_scope_notes"):
        block = ov.get(top)
        if isinstance(block, dict):
            yield from _emit(top, block.get("verification"))
    for i, dev in enumerate(ov.get("element_deviations", []) or []):
        yield from _emit(f"element_deviations[{i}]", dev.get("verification"))
    spo = ov.get("search_param_observations") or {}
    if isinstance(spo, dict):
        for resource_type, block in spo.items():
            if isinstance(block, dict):
                yield from _emit(f"search_param_observations.{resource_type}", block.get("verification"))


def _is_skippable(url: str) -> bool:
    if url in SENTINELS:
        return True
    if any(url.startswith(p) for p in LOCAL_PATH_PREFIXES):
        return True
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return True
    return False


# Sandbox & customer-evidence verifications point to FHIR API query URLs, not
# doc pages. Their authority comes from being a reproducible query the user can
# re-run with their own credentials — not from being publicly browsable. We
# only flag them as broken on outright link rot (404/410/NXDOMAIN); auth
# challenges (401/403) just mean "URL exists and a real handler responded."
EVIDENCE_QUERY_VIA = {
    "epic_public_sandbox",
    "cerner_public_sandbox",
    "ecw_public_sandbox",
    "veradigm_public_sandbox",
    "nextgen_public_sandbox",
    "meditech_public_sandbox",
    "customer_evidence",
}
# For doc-page verifications, ANY 4xx is link rot worth flagging.
TREAT_AS_OK = {200, 201, 202, 203, 204, 301, 302, 303, 307, 308}


def _check_one(url: str, timeout: int = 15, *, verified_via: str = "") -> dict:
    """Returns {url, status, ok, kind} where kind is 'ok'|'broken'|'transient'.

    Doc-page citations (vendor_official_docs, vendor_pr, community_report) are
    judged by HTTP status: <400 = OK, anything else = broken. Evidence-bearing
    query URLs (*_public_sandbox, customer_evidence) are only broken on hard
    link rot (404, 410, NXDOMAIN); auth gates like 401/403 prove the URL is alive.
    For FHIR sandbox bases, a 404 at the bare root is *expected* — FHIR servers
    discover capability under ${base}/metadata, not at the root — so we re-probe
    /metadata before declaring rot.
    """
    is_query = verified_via in EVIDENCE_QUERY_VIA
    try:
        import requests
    except ImportError:
        return {"url": url, "status": None, "ok": False, "kind": "broken",
                "reason": "requests not installed (pip install requests)"}

    def _probe_status(target: str) -> int | None:
        r = requests.head(target, headers={"User-Agent": USER_AGENT}, timeout=timeout, allow_redirects=True)
        if r.status_code == 405:
            r = requests.get(target, headers={"User-Agent": USER_AGENT, "Accept": "application/fhir+json"},
                             timeout=timeout, allow_redirects=True, stream=True)
            r.close()
        return r.status_code

    try:
        sc = _probe_status(url)
        if sc in TREAT_AS_OK:
            return {"url": url, "status": sc, "ok": True, "kind": "ok"}
        # Auth-gated responses on evidence query URLs: URL exists, auth just required.
        if is_query and sc in {401, 403}:
            return {"url": url, "status": sc, "ok": True, "kind": "ok",
                    "reason": f"HTTP {sc} (auth required; URL is alive)"}
        # FHIR convention: a bare base URL legitimately 404s; capability lives
        # at ${base}/metadata. For sandbox-evidence URLs, re-probe /metadata and
        # accept any non-rot response (2xx/3xx, 401/403 auth-gates, 405/406 from
        # HEAD content-negotiation) as proof that the FHIR server is alive.
        if is_query and sc == 404 and not url.rstrip("/").endswith("/metadata"):
            try:
                meta_sc = _probe_status(url.rstrip("/") + "/metadata")
                if meta_sc in TREAT_AS_OK or meta_sc in {401, 403, 405, 406}:
                    return {"url": url, "status": sc, "ok": True, "kind": "ok",
                            "reason": f"HTTP 404 at bare base; /metadata responded {meta_sc} (FHIR convention)"}
            except Exception:
                pass  # Fall through to broken below.
        # Hard link rot for everyone:
        if sc in {404, 410}:
            return {"url": url, "status": sc, "ok": False, "kind": "broken", "reason": f"HTTP {sc}"}
        # 5xx is treated as transient (vendor outages happen).
        if 500 <= sc < 600:
            return {"url": url, "status": sc, "ok": False, "kind": "transient", "reason": f"HTTP {sc}"}
        # Other 4xx on doc-page URLs is broken; on query URLs it's transient.
        if 400 <= sc < 500:
            kind = "transient" if is_query else "broken"
            return {"url": url, "status": sc, "ok": False, "kind": kind, "reason": f"HTTP {sc}"}
        # Unknown success-range status (>=200): accept.
        return {"url": url, "status": sc, "ok": True, "kind": "ok"}
    except requests.exceptions.Timeout:
        return {"url": url, "status": None, "ok": False, "kind": "transient", "reason": "timeout"}
    except requests.exceptions.ConnectionError as e:
        msg = str(e)
        kind = "broken" if "Name or service not known" in msg or "NXDOMAIN" in msg else "transient"
        return {"url": url, "status": None, "ok": False, "kind": kind, "reason": msg[:160]}
    except Exception as e:
        return {"url": url, "status": None, "ok": False, "kind": "transient", "reason": f"{type(e).__name__}: {e}"[:160]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vendor", help="Limit to a single vendor (e.g., epic)")
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--timeout", type=int, default=15)
    args = ap.parse_args()

    vendors = [args.vendor] if args.vendor else sorted(p.name for p in EHRS_DIR.iterdir() if p.is_dir())

    citations: list[dict] = []
    for vendor in vendors:
        ov_path = EHRS_DIR / vendor / "overlay.json"
        if not ov_path.exists():
            sys.stderr.write(f"WARN: {ov_path} missing; skipping {vendor}\n")
            continue
        for ehr, label, url, via in _walk_overlay_urls(ov_path):
            citations.append({"ehr": ehr, "label": label, "url": url, "verified_via": via})

    # De-dup by URL — many citations point to the same vendor doc page.
    by_url: dict[str, list[dict]] = {}
    for c in citations:
        if _is_skippable(c["url"]):
            continue
        by_url.setdefault(c["url"], []).append(c)

    sys.stderr.write(f"Checking {len(by_url)} unique URLs across {len(vendors)} vendor(s)...\n")

    # The verified_via for an URL determines how strictly we judge non-2xx
    # responses. If a URL is cited from multiple verifications with different
    # verified_via values, fall back to the strictest (doc-page semantics) — ANY
    # alive doc-page citation makes that URL doc-judged.
    via_for_url: dict[str, str] = {}
    for u, cites in by_url.items():
        all_via = {c["verified_via"] for c in cites}
        # If ALL citations are evidence-query verifications, treat as query URL;
        # otherwise treat as doc URL (the strictest judgment wins).
        via_for_url[u] = next(iter(all_via)) if all_via.issubset(EVIDENCE_QUERY_VIA) else "vendor_official_docs"

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {
            ex.submit(_check_one, u, args.timeout, verified_via=via_for_url[u]): u
            for u in by_url
        }
        for f in as_completed(futures):
            res = f.result()
            results[res["url"]] = res

    broken = [r for r in results.values() if r["kind"] == "broken"]
    transient = [r for r in results.values() if r["kind"] == "transient"]
    ok = [r for r in results.values() if r["kind"] == "ok"]

    summary = {
        "total_unique_urls": len(by_url),
        "ok": len(ok),
        "broken": len(broken),
        "transient": len(transient),
        "broken_details": [
            {"url": r["url"], "reason": r["reason"], "citations": [
                {"ehr": c["ehr"], "label": c["label"], "verified_via": c["verified_via"]}
                for c in by_url[r["url"]]
            ]}
            for r in broken
        ],
        "transient_details": [
            {"url": r["url"], "reason": r["reason"]} for r in transient
        ],
    }
    print(json.dumps(summary, indent=2))

    sys.stderr.write(f"\nOK: {len(ok)}  Broken: {len(broken)}  Transient: {len(transient)}\n")
    if broken:
        sys.stderr.write("\nBROKEN URLS (open a citation refresh issue):\n")
        for r in broken:
            sys.stderr.write(f"  {r['url']}  -- {r['reason']}\n")
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
