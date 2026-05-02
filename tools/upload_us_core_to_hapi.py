"""Upload the US Core 6.1.0 IG package contents into a running HAPI FHIR server
so HAPI's $validate endpoint can validate against the profiles.

This is the runtime alternative to mounting the IG into HAPI's application.yaml.
It POSTs every StructureDefinition, ValueSet, CodeSystem, SearchParameter, and
CapabilityStatement from `us-core/ig-package/package/` into HAPI's REST API.

Idempotent: uploads use PUT with the resource's `id`, so re-running upserts.

Usage:
    python -m tools.upload_us_core_to_hapi
    python -m tools.upload_us_core_to_hapi --hapi-base=http://localhost:8090/fhir
"""
import argparse
import json
import sys
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
IG_DIR = REPO_ROOT / "us-core" / "ig-package" / "package"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) IG-uploader"

UPLOAD_TYPES = {
    "StructureDefinition",
    "ValueSet",
    "CodeSystem",
    "SearchParameter",
    "OperationDefinition",
    "ImplementationGuide",
    "CapabilityStatement",
}


def upload_one(hapi_base: str, resource: dict) -> tuple[int, str]:
    rt = resource.get("resourceType")
    rid = resource.get("id")
    if not rt or not rid:
        return 0, "missing resourceType or id"
    url = f"{hapi_base.rstrip('/')}/{rt}/{rid}"
    r = requests.put(
        url,
        json=resource,
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "User-Agent": USER_AGENT,
        },
        timeout=60,
    )
    return r.status_code, r.text[:200]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    # The example HAPI compose runs in tenant-partitioning mode; the FHIR base
    # must include the tenant segment ('DEFAULT' is the auto-created default).
    ap.add_argument("--hapi-base", default="http://localhost:8090/fhir/DEFAULT")
    ap.add_argument("--types", default=",".join(sorted(UPLOAD_TYPES)))
    args = ap.parse_args()

    types = set(args.types.split(","))
    files = sorted(IG_DIR.glob("*.json"))
    print(f"== Uploading IG to HAPI at {args.hapi_base} ==")
    print(f"  types: {sorted(types)}")
    print(f"  source: {IG_DIR.relative_to(REPO_ROOT)}/ ({len(files)} files)")

    counts = {"ok": 0, "fail": 0, "skip": 0}
    failures = []
    for f in files:
        try:
            res = json.loads(f.read_text())
        except json.JSONDecodeError:
            counts["skip"] += 1
            continue
        rt = res.get("resourceType")
        if rt not in types:
            counts["skip"] += 1
            continue
        status, body_first = upload_one(args.hapi_base, res)
        if 200 <= status < 300:
            counts["ok"] += 1
        else:
            counts["fail"] += 1
            failures.append((rt, res.get("id"), status, body_first))

    print(f"  uploaded ok: {counts['ok']}, failed: {counts['fail']}, skipped: {counts['skip']}")
    for rt, rid, status, body in failures[:10]:
        print(f"    FAIL {rt}/{rid} HTTP {status}: {body[:160]}")
    return 0 if counts["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
