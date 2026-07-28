"""Build and parse `mf-entry-bin/v<version>+<timestamp>.<sha8>` sidecar release tags.

DI-4871: sidecar binary releases are tagged independently of MetricFlow's own release
tags, so a sidecar-only change (e.g. a new mf_entry.py entry point) can ship without a
MetricFlow version bump. A tag looks like:

    mf-entry-bin/v0.208.0+2607281534.a1b2c3d4

`<timestamp>` is `YYMMDDHHmm` in UTC and `<sha8>` is the first 8 characters of the commit
SHA the binary was built from -- together they disambiguate multiple sidecar builds off the
same MetricFlow version with no stateful counter to compute (no need to scan existing tags
for the highest value in use). This is not required to be PEP 440-compliant: unlike
MetricFlow's own `v<version>` tags, this tag never goes through PyPI/`pip`/`twine`, so
there's no actual constraint on the local-version-segment syntax to satisfy.

This is the single source of truth for the tag format -- both `build_sidecar_release_tag`
(used by release_step_3.py, which already knows the version/commit it's tagging, and by
cut_adhoc_release.py, which resolves them from git) and `metricflow_version_from_tag` (used
by generate_build_info.py) live here so the two directions of the format can't drift apart.
"""

from __future__ import annotations

from datetime import datetime, timezone

MF_ENTRY_BIN_TAG_PREFIX = "mf-entry-bin/v"


def build_sidecar_release_tag(
    metricflow_version: str,
    commit_sha: str,
    timestamp: datetime | None = None,
) -> str:
    """Build a sidecar release tag for the given MetricFlow version and commit."""
    timestamp = timestamp or datetime.now(timezone.utc)
    return f"{MF_ENTRY_BIN_TAG_PREFIX}{metricflow_version}+{timestamp.strftime('%y%m%d%H%M')}.{commit_sha[:8]}"


def metricflow_version_from_tag(tag: str) -> str:
    """Return the embedded MetricFlow version encoded in a sidecar release tag.

    Deliberately agnostic to the tag's namespace prefix (works the same whether it's
    `mf-entry-bin/`, the retired `sidecar/`, or none at all) and to what follows the
    version -- only relies on the version being between the tag's last `/` and its first
    `+`, with a leading `v`.
    """
    return tag.rsplit("/", 1)[-1].split("+", 1)[0].removeprefix("v")
