from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from scripts.sidecar_release.tag import build_sidecar_release_tag, metricflow_version_from_tag


def test_build_sidecar_release_tag_with_explicit_timestamp() -> None:
    """The tag should combine the version, a UTC YYMMDDHHmm timestamp, and an 8-char sha."""
    tag = build_sidecar_release_tag(
        metricflow_version="0.208.0",
        commit_sha="a1b2c3d4e5f6789",
        timestamp=datetime(2026, 7, 28, 15, 34, tzinfo=timezone.utc),
    )
    assert tag == "mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4"


def test_build_sidecar_release_tag_truncates_full_length_sha() -> None:
    """A full 40-character commit SHA should be truncated to its first 8 characters."""
    full_sha = "a1b2c3d4e5f6789012345678901234567890abcd"
    tag = build_sidecar_release_tag(
        metricflow_version="0.208.0",
        commit_sha=full_sha,
        timestamp=datetime(2026, 7, 28, 15, 34, tzinfo=timezone.utc),
    )
    assert tag.endswith(".a1b2c3d4")


def test_build_sidecar_release_tag_defaults_to_now_when_timestamp_omitted() -> None:
    """Omitting the timestamp should still produce a validly-shaped tag using the current time."""
    tag = build_sidecar_release_tag(metricflow_version="0.208.0", commit_sha="a1b2c3d4e5f6")
    assert re.fullmatch(r"mf-stdio-sidecar/v0\.208\.0\+\d{10}\.a1b2c3d4", tag)


@pytest.mark.parametrize(
    ("tag", "expected_version"),
    [
        ("mf-stdio-sidecar/v0.208.0+2607281534.a1b2c3d4", "0.208.0"),
        ("sidecar/v0.208.0+1", "0.208.0"),
        ("v0.208.0", "0.208.0"),
    ],
)
def test_metricflow_version_from_tag(tag: str, expected_version: str) -> None:
    """The embedded MetricFlow version should parse out regardless of prefix or suffix shape."""
    assert metricflow_version_from_tag(tag) == expected_version
