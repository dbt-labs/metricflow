"""Cut an ad hoc sidecar release tag for a sidecar-only change (DI-4871).

Replaces the inline bash tag-resolution step in cd-cut-adhoc-sidecar-release.yaml. For a
change to sidecar/mf_entry.py or mf_ipc_protocol.py that doesn't correspond to a new
MetricFlow version -- the tag cut alongside each MetricFlow release is instead pushed
automatically by scripts/release_tool/release_step_3.py, which already knows the version
and commit it's releasing and calls `build_sidecar_release_tag` directly, with no need to
resolve either the way this script does.

Resolves MetricFlow's current version from metricflow/__about__.py and HEAD's own commit
SHA, then creates the tag locally. Does not push it -- that, and re-dispatching
cd-build-sidecar-binaries.yaml for the new tag, are genuinely CI/remote actions and stay as
separate, explicit steps in the workflow, matching the "the workflow becomes mostly
orchestrating these scripts" framing this was written for.

Deliberately reads __about__.py rather than resolving the nearest real release tag via
`git describe`: between releases, __about__.py holds a `.devN` pre-release version (e.g.
`0.212.0.dev0`), and an ad hoc release doesn't guard against metricflow/,
metricflow_semantics/, or metricflow_semantic_interfaces/ having changed since the last real
release (see the acceptance criteria in DI-4871) -- so labeling it with the last released
version number would overstate how closely it actually matches that release. `.devN`
honestly signals "unreleased, in-development state" whether or not real changes happened,
rather than implying an exact match to a specific release that may not hold.

Run with `--dry-run` to preview the tag that would be cut without creating anything --
runnable locally against a real clone, no CI required.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

from scripts.mf_script_helper import MetricFlowScriptHelper
from scripts.sidecar_release.tag import build_sidecar_release_tag

_VERSION_ASSIGNMENT_PATTERN = re.compile(r'__version__\s*=\s*"([^"]+)"')


def resolve_metricflow_version(repository_directory: Path) -> str:
    """Return MetricFlow's current version, as declared in metricflow/__about__.py."""
    about_file_path = repository_directory / "metricflow" / "__about__.py"
    match = _VERSION_ASSIGNMENT_PATTERN.search(about_file_path.read_text())
    if match is None:
        raise RuntimeError(f'Could not find a `__version__ = "..."` assignment in {about_file_path}')
    return match.group(1)


def resolve_head_commit_sha(repository_directory: Path) -> str:
    """Return the full commit SHA at HEAD."""
    result = MetricFlowScriptHelper.run_command(
        ["git", "rev-parse", "HEAD"],
        working_directory=repository_directory,
        capture_output=True,
    )
    return result.stdout.decode().strip()


def create_local_tag(tag: str, repository_directory: Path) -> None:
    """Create a lightweight git tag at HEAD. Does not push it."""
    MetricFlowScriptHelper.run_command(["git", "tag", tag], working_directory=repository_directory)


def cut_adhoc_release(repository_directory: Path, dry_run: bool) -> str:
    """Resolve the next ad hoc sidecar release tag and, unless `dry_run`, create it locally."""
    metricflow_version = resolve_metricflow_version(repository_directory)
    commit_sha = resolve_head_commit_sha(repository_directory)

    tag = build_sidecar_release_tag(metricflow_version=metricflow_version, commit_sha=commit_sha)

    if dry_run:
        print(f"[dry run] would create tag: {tag}")
    else:
        create_local_tag(tag, repository_directory)
        print(f"Created local tag: {tag}")

    return tag


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-directory", type=Path, default=Path.cwd())
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the tag that would be cut without creating it locally.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:  # noqa: D103
    args = _parse_args(argv)
    tag = cut_adhoc_release(args.repository_directory, dry_run=args.dry_run)

    # $GITHUB_OUTPUT is only set when running inside a GitHub Actions job; writing to it is
    # a no-op (skipped) when run locally.
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with Path(github_output).open("a") as output_file:
            output_file.write(f"tag={tag}\n")


if __name__ == "__main__":
    main()
