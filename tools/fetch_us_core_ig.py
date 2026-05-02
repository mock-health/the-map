"""Download the official US Core 6.1.0 IG package from packages.fhir.org.

The Map's `must_support` baseline is built FROM this package — not by hand.
Per PLAN_EPIC.md decision #1: the IG package is the source of truth.

Output:
    us-core/ig-package/package/StructureDefinition-*.json
    us-core/ig-package/package/ValueSet-*.json
    us-core/ig-package/package/CodeSystem-*.json
    us-core/ig-package/package/package.json   (manifest with version + dep list)

Usage:
    python -m tools.fetch_us_core_ig
    python -m tools.fetch_us_core_ig --version=6.1.0
    python -m tools.fetch_us_core_ig --force         # re-download even if present
"""
import argparse
import io
import json
import sys
import tarfile
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
IG_DIR = REPO_ROOT / "us-core" / "ig-package"
PACKAGE_NAME = "hl7.fhir.us.core"
DEFAULT_VERSION = "6.1.0"
REGISTRY_BASE = "https://packages.fhir.org"
USER_AGENT = "mockhealth-map/0.1 (+https://mock.health) ig-package-fetcher"


def fetch_package(name: str, version: str) -> bytes:
    url = f"{REGISTRY_BASE}/{name}/{version}"
    r = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept": "application/gzip"}, timeout=60)
    r.raise_for_status()
    return r.content


def extract_package(tarball: bytes, dest: Path) -> int:
    dest.mkdir(parents=True, exist_ok=True)
    count = 0
    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            # Sanitize: refuse anything that escapes the dest dir
            target = (dest / member.name).resolve()
            if not str(target).startswith(str(dest.resolve())):
                raise RuntimeError(f"refusing path traversal: {member.name}")
            target.parent.mkdir(parents=True, exist_ok=True)
            data = tf.extractfile(member)
            if data is None:
                continue
            target.write_bytes(data.read())
            count += 1
    return count


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--version", default=DEFAULT_VERSION)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    manifest = IG_DIR / "package" / "package.json"
    if manifest.exists() and not args.force:
        existing = json.loads(manifest.read_text())
        if existing.get("version") == args.version and existing.get("name") == PACKAGE_NAME:
            sds = sorted((IG_DIR / "package").glob("StructureDefinition-*.json"))
            print(f"already present: {PACKAGE_NAME} {args.version} ({len(sds)} StructureDefinitions). Use --force to re-fetch.")
            return 0

    print(f"fetching {PACKAGE_NAME}@{args.version} from {REGISTRY_BASE}")
    tarball = fetch_package(PACKAGE_NAME, args.version)
    print(f"  {len(tarball):,} bytes")

    if IG_DIR.exists() and args.force:
        # Wipe stale package files before re-extract; keep the dir itself
        for p in IG_DIR.rglob("*"):
            if p.is_file():
                p.unlink()

    n = extract_package(tarball, IG_DIR)
    print(f"  extracted {n} files to {IG_DIR.relative_to(REPO_ROOT)}/")

    if not manifest.exists():
        sys.exit(f"ERROR: extracted package missing manifest at {manifest}")
    info = json.loads(manifest.read_text())
    print(f"  package: {info.get('name')} {info.get('version')} (canonical={info.get('canonical')})")
    sds = sorted((IG_DIR / "package").glob("StructureDefinition-*.json"))
    vss = sorted((IG_DIR / "package").glob("ValueSet-*.json"))
    css = sorted((IG_DIR / "package").glob("CodeSystem-*.json"))
    print(f"  StructureDefinitions={len(sds)}  ValueSets={len(vss)}  CodeSystems={len(css)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
