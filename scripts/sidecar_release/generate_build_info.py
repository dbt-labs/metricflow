"""Generate the build-info.json release asset for a sidecar binary build.

Replaces the inline python3 heredoc in cd-build-sidecar-binaries.yaml's `publish` job
(DI-4871). Records the embedded MetricFlow version, the source commit, and the Nuitka/Python
versions actually measured at build time -- the thing to check if a specific binary's exact
provenance is ever in question, since the tag/archive names alone don't encode the commit.

The embedded MetricFlow version is parsed via `tag.py` -- the single source of truth for
the sidecar release tag format -- so this stays correct automatically if that format ever
changes again.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.sidecar_release.tag import metricflow_version_from_tag


def generate_build_info(
    tag: str,
    commit_sha: str,
    nuitka_version: str,
    python_version: str,
) -> dict[str, str]:
    """Return the build-info.json contents for a sidecar binary build."""
    return {
        "tag": tag,
        "metricflow_version": metricflow_version_from_tag(tag),
        "commit": commit_sha,
        "nuitka_version": nuitka_version.strip(),
        "python_version": python_version.strip(),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="The git tag (or ref name) this release was cut from.")
    parser.add_argument("--commit", required=True, help="The commit SHA this release was built from.")
    parser.add_argument(
        "--nuitka-version-file", type=Path, required=True, help="File containing `nuitka --version` output."
    )
    parser.add_argument(
        "--python-version-file", type=Path, required=True, help="File containing `python --version` output."
    )
    parser.add_argument("--output", type=Path, required=True, help="Where to write build-info.json.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:  # noqa: D103
    args = _parse_args(argv)
    build_info = generate_build_info(
        tag=args.tag,
        commit_sha=args.commit,
        nuitka_version=args.nuitka_version_file.read_text(),
        python_version=args.python_version_file.read_text(),
    )
    args.output.write_text(json.dumps(build_info, indent=2) + "\n")


if __name__ == "__main__":
    main()
