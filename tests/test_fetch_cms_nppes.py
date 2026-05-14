"""Unit tests for NPPES URL discovery.

We don't exercise the actual multi-GB download here — that's a manual /
CI-driven smoke test. The walk-back logic is what we'd most likely break
on a refactor, so it's what gets the most coverage.
"""
from __future__ import annotations

from datetime import date

import pytest

from tools import fetch_cms_nppes


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {}


def _make_head_fake(allow: set[str]):
    def fake_head(url: str, **_: object) -> _FakeResponse:
        return _FakeResponse(200 if url in allow else 404)
    return fake_head


def test_url_for_month_format() -> None:
    assert (
        fetch_cms_nppes.url_for_month(date(2026, 2, 1))
        == "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026.zip"
    )
    assert (
        fetch_cms_nppes.url_for_month(date(2025, 12, 1))
        == "https://download.cms.gov/nppes/NPPES_Data_Dissemination_December_2025.zip"
    )


def test_discover_picks_current_month_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    today = date(2026, 5, 14)
    may_url = fetch_cms_nppes.url_for_month(date(2026, 5, 1))
    monkeypatch.setattr(fetch_cms_nppes.requests, "head", _make_head_fake({may_url}))
    url, release_month = fetch_cms_nppes.discover_latest_url(today=today)
    assert url == may_url
    assert release_month == date(2026, 5, 1)


def test_discover_walks_back_one_month(monkeypatch: pytest.MonkeyPatch) -> None:
    today = date(2026, 5, 3)
    april_url = fetch_cms_nppes.url_for_month(date(2026, 4, 1))
    # May hasn't been published yet (CMS publishes ~mid-month).
    monkeypatch.setattr(fetch_cms_nppes.requests, "head", _make_head_fake({april_url}))
    url, release_month = fetch_cms_nppes.discover_latest_url(today=today)
    assert url == april_url
    assert release_month == date(2026, 4, 1)


def test_discover_crosses_year_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    today = date(2026, 1, 5)
    december_url = fetch_cms_nppes.url_for_month(date(2025, 12, 1))
    monkeypatch.setattr(fetch_cms_nppes.requests, "head", _make_head_fake({december_url}))
    url, release_month = fetch_cms_nppes.discover_latest_url(today=today)
    assert url == december_url
    assert release_month == date(2025, 12, 1)


def test_discover_raises_when_nothing_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch_cms_nppes.requests, "head", _make_head_fake(set()))
    with pytest.raises(RuntimeError, match="Could not locate"):
        fetch_cms_nppes.discover_latest_url(today=date(2026, 5, 14), max_months_back=2)


def test_parse_pin_month_yyyy_mm() -> None:
    assert fetch_cms_nppes._parse_pin_month("2026-02") == date(2026, 2, 1)
    assert fetch_cms_nppes._parse_pin_month("2025-12") == date(2025, 12, 1)


def test_parse_pin_month_yyyy_mm_dd_anchors_to_first() -> None:
    # Day component is accepted but anchored to first-of-month for storage.
    assert fetch_cms_nppes._parse_pin_month("2026-02-08") == date(2026, 2, 1)


def test_parse_pin_month_rejects_garbage() -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        fetch_cms_nppes._parse_pin_month("not-a-date")
    with pytest.raises(argparse.ArgumentTypeError):
        fetch_cms_nppes._parse_pin_month("2026-13")
