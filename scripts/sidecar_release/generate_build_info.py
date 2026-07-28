"""Generate the build-info.json release asset for a sidecar binary build.

Replaces the inline python3 heredoc in cd-build-sidecar-binaries.yaml's `publish` job
(DI-4871). Records the embedded MetricFlow version, the source commit, and the Nuitka/Python
versions actually measured at build time -- the thing to check if a specific binary's exact
provenance is ever in question, since the tag/archive names alone don't encode the commit.

Deliberately prefix-agnostic (works the same regardless of the tag's namespace prefix, e.g.
`sidecar/` or `mf-entry-bin/`): the embedded MetricFlow version is parsed as everything after
the tag's last `/`, before its first `+`, with a leading `v` stripped.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def metricflow_version_from_tag(tag: str) -> str:
    """Return the embedded MetricFlow version encoded in a sidecar release tag."""
    return tag.rsplit("/", 1)[-1].split("+", 1)[0].removeprefix("v")


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
