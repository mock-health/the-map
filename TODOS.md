# TODOS

## CMS Data Pipeline

### Regression test for `discover_input` tier preference (QIES > zip > iQIES)

**What:** Add a unit test in `tests/test_pos_index_build.py` that creates a temp
dated dir with a QIES CSV, a legacy zip, and an iQIES CSV all sibling-by-sibling
(with mtimes manipulated to make iQIES newest), then asserts
`discover_input(None)` returns the QIES CSV.

**Why:** Commit `56220bc` fixed a real production bug — hospitals-mode silently
read iQIES and produced an empty (0-row) hospitals catalog, attributed to
`POS_File_iQIES_Q1_2026.csv`. The fix tiers QIES > zip > iQIES, but no test
guards against the tier order regressing. Future contributors changing
`DEFAULT_GLOB_DIRS` or `discover_input` won't see this fail.

**Context:** Code at `tools/build_pos_hospital_index.py:182-200`. The previous
implementation was `max(candidates, key=lambda p: p.stat().st_mtime)`. Test
should also cover the case where only iQIES exists in the dated dir (hospitals
mode should still return it as the fallback rather than raising — verify or
fix as needed).

**Effort:** S
**Priority:** P1
**Depends on:** None

### Regression test for iQIES projection (`project_row_iqies` + `_detect_format`)

**What:** Add a unit test that runs `build_index` over a small in-memory iQIES
CSV (UPPER snake_case columns, e.g. `prvdr_num`, `fac_name`, `cmplnc_strt_dt`,
`trmntn_exprtn_dt`) and asserts the projected rows have the expected `ccn`,
`name`, `address`, etc. Cover the termination filter
(`trmntn_exprtn_dt <= today_iso` drops the row).

**Why:** The iQIES integration adds ~56k post-acute facilities to the providers
catalog. Right now the entire iQIES code path
(`tools/build_pos_hospital_index.py:project_row_iqies` + `_detect_format`)
has zero unit coverage. A future schema change in the iQIES extract would only
be caught by `verify_overlay_refresh.py` after a full pipeline rebuild.

**Effort:** S
**Priority:** P1
**Depends on:** None

### Codify the rebuild-overlays idempotency invariant

**What:** Add an integration test (or `Makefile` target with a CI hook) that
runs `make rebuild-overlays` twice and diffs the generated overlay JSON files,
asserting byte-identical output modulo `generated_at` timestamps.

**Why:** Reproducibility is the whole point of this branch. The user proved it
manually three times during the iQIES integration; if it regresses
(non-deterministic dict ordering, drifting input file selection, timestamp
leaking into row payloads), it should fail loudly in CI, not on a future
contributor's clean-room rebuild attempt.

**Context:** The verify harness at `tools/verify_overlay_refresh.py` compares
the current rebuild against committed HEAD, but that's a different question —
it doesn't catch nondeterminism within a single contributor's machine.

**Effort:** M
**Priority:** P1
**Depends on:** None

### Provenance filename consistency: teach `read_provenance` about per-source

**What:** Either (a) accept a `filename=` parameter on `read_provenance` and
`latest_dated_subdir` mirroring `archive_provenance`, or (b) change the
existence check to glob `.provenance*.json` so per-source provenance files
(`.provenance-qies.json`, `.provenance-iqies.json`) are discoverable.

**Why:** Today `read_provenance(<cms-pos-dated-dir>)` always returns `None`
and `latest_dated_subdir(<cms-pos-root>)` always falls back to
"newest-by-mtime" because the POS fetcher writes per-source files but the
readers only check the default name. No current caller hits the inconsistency,
but the API is a landmine for future code.

**Context:** `tools/_fetch.py` writers vs readers. Pair with adding a test that
pins the chosen behavior so it can't silently drift back.

**Effort:** S
**Priority:** P2
**Depends on:** None

### Pull duplicated fetcher boilerplate into `_fetch.py`

**What:** The four fetchers (`fetch_cms_npd.py`, `fetch_cms_nppes.py`,
`fetch_cms_pos.py`, `fetch_cms_pecos.py`) each repeat a ~6-line `--dir`
argparse + `storage_root(...)` resolution block, and a ~5-line `out_dir`
resolution block. Hoist as `add_storage_dir_arg(ap, dataset, env_var)` and
`resolve_storage_dir(args, dataset, env_var)` in `tools/_fetch.py`.

**Why:** This is the consolidation `_fetch.py` was created for — it stopped
one layer too early. Adding a fifth source becomes one helper call instead of
copy-paste; bug fixes apply once.

**Effort:** S
**Priority:** P2
**Depends on:** None

### Make HTTP timeouts uniform and named

**What:** Add `METADATA_TIMEOUT_SECS`, `HEAD_TIMEOUT_SECS`,
`STREAM_TIMEOUT_SECS`, `GIT_TIMEOUT_SECS` constants in `tools/_fetch.py` and
swap the bare literals (15, 30, 300, 2) in fetchers for the named constants.

**Why:** `fetch_cms_npd.py` uses 30s for HEAD; `fetch_cms_nppes.py` uses 15s.
Probably accidental; the named constants make the choice intentional and
discoverable.

**Effort:** S
**Priority:** P3
**Depends on:** None

### Normalize env-var naming: `THE_MAP_NPD_DIR` (drop `CMS_` infix)

**What:** Rename `THE_MAP_CMS_NPD_DIR` to `THE_MAP_NPD_DIR` to match
`THE_MAP_NPPES_DIR` / `THE_MAP_POS_DIR` / `THE_MAP_PECOS_DIR`. Keep the old
name as a deprecation alias for one release.

**Why:** Long-tail support: a contributor exporting one of the four expects
the other three to follow the same convention. Inconsistent envvar names mean
"why doesn't this work?" debugging cycles.

**Effort:** S
**Priority:** P3
**Depends on:** None

### Sample-of-20 NPI/CCN spot-check in `verify_overlay_refresh.py`

**What:** Extend `diff_overlay` to additionally pick 20 random endpoints per
overlay and compare their NPI/CCN values between committed and rebuilt
versions, flagging any mismatch with a sample diff. Cap at INFO severity
(not a structural break — facility renames and NPI re-issuances are normal
drift).

**Why:** The plan called for this; today the harness checks schema parity and
row-count only. A subtle resolver regression that re-mapped half the endpoints
to wrong NPIs would pass schema + count but be a real correctness loss.

**Effort:** M
**Priority:** P2
**Depends on:** None

### Strip `THE_MAP_*` env vars in test isolation

**What:** Extend the autouse `_isolate_env` fixture in `tests/conftest.py` to
`monkeypatch.delenv` any key starting with `THE_MAP_`, not just the explicit
list it currently has.

**Why:** A contributor who exports `THE_MAP_POS_DIR` / `THE_MAP_NPPES_DIR` /
`THE_MAP_POS_CSV` in their shell sees real-tool behavior tests pick up that
directory instead of the temp one. Currently the only protection is
"developers don't usually set these." That's the kind of flake the autouse
exists to prevent.

**Effort:** S
**Priority:** P2
**Depends on:** None

### Test `archive_provenance` `filename=` and `extra=` kwargs

**What:** Add two tests in `tests/test_fetch_lib.py`: one that calls
`archive_provenance(..., filename='.provenance-qies.json')` and again with
`.provenance-iqies.json` in the same dir, asserting both files exist with
distinct contents. Another that passes `extra={...}` and verifies the keys
land at the JSON top level alongside `dataset`/`source_url`/etc., and that
`extra` cannot overwrite reserved keys (if that's the intended contract).

**Why:** Both kwargs are load-bearing for POS (per-source provenance) and used
by every fetcher (`extra` for the DCAT title and dataset identifier). Untested
today.

**Effort:** S
**Priority:** P2
**Depends on:** None

### DCAT downloadURL host allowlist (defense-in-depth)

**What:** In `tools/_fetch.py:discover_dcat_csv_distribution`, enforce that
`downloadURL.startswith("https://")` and that the netloc is in a small
allowlist (`data.cms.gov`, `download.cms.gov`, `*.s3.amazonaws.com` for
signed-S3 redirects).

**Why:** Today a compromised data.cms.gov DCAT response could point
`downloadURL` at an arbitrary host. Very low real-world risk — offline dev
tooling running on a laptop, the result is just a file under `data/raw/` —
but the allowlist costs nothing and bounds the worst case.

**Effort:** S
**Priority:** P3
**Depends on:** None

### Pre-tokenize POS facility names in `augment_pos_via_nppes`

**What:** In `tools/augment_pos_via_nppes.py` around the `pos_idx`-building
loop (~line 297), stash `h["_name_tokens"] = name_tokens(h["name"])` so the
inner candidate-scoring loop at line 209 doesn't recompute tokens per
fleet-endpoint × candidate.

**Why:** Bounded micro-optimization — the whole pipeline runs in seconds
either way — but it's the only non-trivial repeated work the performance
specialist flagged. Cheap to fix when next touching the file.

**Effort:** S
**Priority:** P3
**Depends on:** None

### Stale docstrings in `verify_overlay_refresh.py`

**What:** `OverlayDiff.status` field comment (line 63) lists `"missing-committed"`
(never produced) and omits `"missing-both"` and `"drift-outside-tolerance"`
(both produced). Update to enumerate the actual status values.

**What (2):** `_count_rows` docstring says `summary.{count_key}` but the
summary-fallback loop ignores `count_key` and iterates a fixed tuple. Either
include `count_key` in the search or fix the docstring.

**Why:** Future contributors cross-referencing the docstring against the
breakage gate will be misled.

**Effort:** S
**Priority:** P3
**Depends on:** None

### Remove dead clause in `archive_provenance`

**What:** In `tools/_fetch.py:225`, the filter
`not (p.name.startswith("provenance") and p.name.endswith(".json"))` is dead —
every provenance file is dot-prefixed (`.provenance.json`,
`.provenance-{source}.json`) and the preceding `not p.name.startswith(".")`
already excludes them.

**Why:** Dead code rots. Either delete the clause or rename to
`startswith(".provenance")` to match the actual pattern.

**Effort:** S
**Priority:** P4
**Depends on:** None

### Hoist `import re` and `import datetime` to module top

**What:** `tools/_fetch.py:334` has `import re` inside
`discover_dcat_csv_distribution`. `tools/build_pos_hospital_index.py:430,533`
has `import datetime` inside function bodies. Move to module-level imports.

**Effort:** S
**Priority:** P4
**Depends on:** None

## Completed
