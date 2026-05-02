"""Unit tests for tools/llm_disambiguate.py.

Tests prompt rendering, candidates_hash determinism, cache key invalidation,
the candidate-validation defense layer, and row-selection logic. Does NOT
hit the Anthropic API — the network call is the only piece not exercised
here. Real API behavior is verified manually via --dry-run + small --limit.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import llm_disambiguate as ld

# --- candidates_hash --------------------------------------------------

def test_candidates_hash_is_deterministic() -> None:
    cands = [
        {"ccn": "100001", "name": "MERCY", "city": "X", "state": "MO", "zip5": "65801"},
        {"ccn": "100002", "name": "BAPTIST", "city": "Y", "state": "MO", "zip5": "65802"},
    ]
    assert ld.candidates_hash(cands) == ld.candidates_hash(list(cands))


def test_candidates_hash_changes_on_reorder() -> None:
    """Reordering the candidate list must change the hash — different
    ordering can change the model's choice (it picks 'the first plausible'
    on ties), so a re-sort should invalidate the cache."""
    a = [
        {"ccn": "1", "name": "A", "city": "X", "state": "MO", "zip5": "1"},
        {"ccn": "2", "name": "B", "city": "Y", "state": "MO", "zip5": "2"},
    ]
    b = list(reversed(a))
    assert ld.candidates_hash(a) != ld.candidates_hash(b)


def test_candidates_hash_changes_on_new_candidate() -> None:
    base = [{"ccn": "1", "name": "A", "city": "X", "state": "MO", "zip5": "1"}]
    extended = base + [{"ccn": "2", "name": "B", "city": "Y", "state": "MO", "zip5": "2"}]
    assert ld.candidates_hash(base) != ld.candidates_hash(extended)


# --- cache key --------------------------------------------------------

def test_cache_path_includes_model_and_prompt_version(tmp_path: Path, monkeypatch) -> None:
    """If the prompt or model changes, prior cached judgments must NOT be
    served — bumping SYSTEM_PROMPT_VERSION or MODEL has to invalidate."""
    p1 = ld.cache_path("ep-abc", "deadbeef" * 2, "claude-haiku-4-5")
    p2 = ld.cache_path("ep-abc", "deadbeef" * 2, "claude-sonnet-4-6")
    assert p1 != p2
    assert "haiku-4-5" in p1.name
    assert "sonnet-4-6" in p2.name
    assert ld.SYSTEM_PROMPT_VERSION in p1.name


def test_cache_path_handles_unsafe_endpoint_id() -> None:
    """Endpoint ids can contain `/`, `:`, etc. (Cerner uses `+` and `/`)."""
    p = ld.cache_path("RhoTxOoxMilOn79VYwU6di1wsxxWqzYwks/Wx8dgCM0=", "abc", "claude-haiku-4-5")
    assert "/" not in p.name
    assert ":" not in p.name
    # Stays bounded so we don't blow up the filesystem with 500-char filenames.
    assert len(p.name) < 200


# --- row selection ----------------------------------------------------

def test_select_rows_skips_already_matched() -> None:
    payload = {"endpoints": [
        {"endpoint_id": "a", "ccn": "100001", "candidates": [{"ccn": "100001", "name": "X", "city": "X", "state": "X"}]},
        {"endpoint_id": "b", "ccn": None,     "candidates": [{"ccn": "100002", "name": "Y", "city": "Y", "state": "Y"}]},
        {"endpoint_id": "c", "ccn": None,     "candidates": []},
    ]}
    rows = ld.select_rows_for_disambiguation(payload)
    assert [r["endpoint_id"] for r in rows] == ["b"]


def test_select_rows_skips_missing_candidates_field() -> None:
    """Rows produced by the resolver may have no `candidates` key at all when
    the strategy is `unmatched`. Those should be skipped, not blow up."""
    payload = {"endpoints": [
        {"endpoint_id": "a", "ccn": None},  # no candidates field
        {"endpoint_id": "b", "ccn": None, "candidates": None},
    ]}
    assert ld.select_rows_for_disambiguation(payload) == []


# --- prompt rendering -------------------------------------------------

def test_render_user_prompt_includes_all_signals() -> None:
    row = {
        "endpoint_id": "ep-1",
        "vendor": "epic",
        "name_observed": "Boston Children's Hospital",
        "endpoint_address": "https://epicproxy.et1234.epichosted.com/...",
        "city": None, "state": None, "zip5": None,
        "candidates": [
            {"ccn": "220001", "name": "BOSTON CHILDREN'S HOSPITAL", "city": "BOSTON",
             "state": "MA", "zip5": "02115", "score": 1.0},
        ],
    }
    text = ld.render_user_prompt(row)
    assert "epic" in text
    assert "Boston Children's Hospital" in text
    assert "epicproxy.et1234.epichosted.com" in text
    assert "220001" in text
    assert "BOSTON" in text
    # Empty observed fields render as empty strings, not "None"
    assert "observed_state: ''" in text
    assert "None" not in text.replace("(none)", "")  # the literal "(none)" sentinel is OK


def test_render_user_prompt_empty_candidates() -> None:
    row = {
        "endpoint_id": "ep-2",
        "name_observed": "Mystery Co.",
        "endpoint_address": "",
        "candidates": [],
    }
    text = ld.render_user_prompt(row)
    assert "(none)" in text


# --- candidate validation (defense layer) -----------------------------

def test_validate_against_candidates_rejects_invented_ccn() -> None:
    candidates = [
        {"ccn": "100001", "name": "X", "city": "X", "state": "X"},
        {"ccn": "100002", "name": "Y", "city": "Y", "state": "Y"},
    ]
    bad_result = {"ccn": "999999", "confidence": "high", "reason": "made it up"}
    fixed = ld.validate_against_candidates(bad_result, candidates)
    assert fixed["ccn"] == ""
    assert fixed["confidence"] == "none"
    assert fixed["validation_failed"] is True
    assert "INVALIDATED" in fixed["reason"]


def test_validate_against_candidates_accepts_valid_ccn() -> None:
    candidates = [{"ccn": "100001", "name": "X", "city": "X", "state": "X"}]
    good = {"ccn": "100001", "confidence": "high", "reason": "exact match"}
    fixed = ld.validate_against_candidates(good, candidates)
    assert fixed["ccn"] == "100001"
    assert fixed["confidence"] == "high"
    assert "validation_failed" not in fixed


def test_validate_against_candidates_passes_through_none() -> None:
    """confidence=none with empty ccn is the legitimate refusal path; don't flag it."""
    candidates = [{"ccn": "100001", "name": "X", "city": "X", "state": "X"}]
    refusal = {"ccn": "", "confidence": "none", "reason": "not a hospital"}
    fixed = ld.validate_against_candidates(refusal, candidates)
    assert fixed["ccn"] == ""
    assert fixed["confidence"] == "none"
    assert "validation_failed" not in fixed


# --- cache hit short-circuits the API ---------------------------------

def test_disambiguate_one_uses_cache(tmp_path: Path, monkeypatch) -> None:
    """If a cache file already exists for (endpoint_id, candidates_hash, model),
    disambiguate_one must NOT call the client."""
    monkeypatch.setattr(ld, "CACHE_DIR", tmp_path / "cache")
    row = {
        "endpoint_id": "ep-1",
        "candidates": [{"ccn": "100001", "name": "X", "city": "X", "state": "X", "zip5": "1"}],
    }
    c_hash = ld.candidates_hash(row["candidates"])
    cp = ld.cache_path(row["endpoint_id"], c_hash, ld.MODEL)
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps({
        "ccn": "100001", "confidence": "high", "reason": "from cache",
        "model": ld.MODEL,
    }))

    # Stub the client so any call would raise — proves we didn't call it.
    class ExplodingClient:
        def messages(self):
            raise RuntimeError("API was called despite cache hit")

    result, was_cached = ld.disambiguate_one(ExplodingClient(), row)
    assert was_cached is True
    assert result["reason"] == "from cache"


def test_disambiguate_one_force_refresh_skips_cache(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(ld, "CACHE_DIR", tmp_path / "cache")
    row = {
        "endpoint_id": "ep-1",
        "candidates": [{"ccn": "100001", "name": "X", "city": "X", "state": "X", "zip5": "1"}],
    }
    c_hash = ld.candidates_hash(row["candidates"])
    cp = ld.cache_path(row["endpoint_id"], c_hash, ld.MODEL)
    cp.parent.mkdir(parents=True, exist_ok=True)
    cp.write_text(json.dumps({"ccn": "100001", "confidence": "high", "reason": "stale"}))

    # With --force-refresh, we expect the call to be attempted; a stub raises
    # to prove we got past the cache.
    class StubClient:
        @property
        def messages(self):
            raise RuntimeError("expected call attempted")

    with pytest.raises(RuntimeError, match="expected call attempted"):
        ld.disambiguate_one(StubClient(), row, force_refresh=True)


# --- tool schema sanity -----------------------------------------------

def test_disambiguate_tool_has_strict_shape() -> None:
    schema = ld.DISAMBIGUATE_TOOL["input_schema"]
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"ccn", "confidence", "reason"}
    assert schema["properties"]["confidence"]["enum"] == ["high", "medium", "low", "none"]
    assert schema["properties"]["ccn"]["type"] == "string"
