"""Offline smoke tests for parsimony-shared.

parsimony-shared is a shared helper library, not a ``parsimony.providers``
plugin, so it carries no conformance test. These checks pin the public helper
surface so the release build has real (offline) coverage instead of failing
pytest's "no tests collected" exit code.
"""

from __future__ import annotations

import dataclasses

import httpx
import parsimony_shared as ps
import pytest
from parsimony_shared import (
    DESCRIPTION_CHAR_CAP,
    MetadataCrawlConfig,
    enumerate_descriptions,
    parse_retry_after,
    truncate_description,
)


def test_public_api_exports() -> None:
    for name in ps.__all__:
        assert hasattr(ps, name), f"missing export: {name}"


def test_truncate_description() -> None:
    assert truncate_description("") == ""
    assert truncate_description("short") == "short"
    out = truncate_description("x" * (DESCRIPTION_CHAR_CAP + 50))
    assert len(out) <= DESCRIPTION_CHAR_CAP


def test_enumerate_descriptions_joins_and_caps() -> None:
    assert enumerate_descriptions("a", "", "  ", "b") == "a b"
    capped = enumerate_descriptions("y" * 1000, "z" * 1000, cap=100)
    assert len(capped) <= 100


def test_metadata_crawl_config_defaults_are_frozen() -> None:
    cfg = MetadataCrawlConfig()
    assert cfg.inter_request_delay_s >= 0
    assert 429 in cfg.retry_statuses
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.inter_request_delay_s = 99.0  # type: ignore[misc]


def test_parse_retry_after() -> None:
    assert parse_retry_after(httpx.Response(429, headers={"Retry-After": "5"})) == 5.0
    assert parse_retry_after(httpx.Response(429)) is None
    assert parse_retry_after(httpx.Response(429, headers={"Retry-After": "soon"})) is None
