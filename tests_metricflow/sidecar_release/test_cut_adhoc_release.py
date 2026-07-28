from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.sidecar_release.cut_adhoc_release import (
    cut_adhoc_release,
    resolve_head_commit_sha,
    resolve_metricflow_version,
)


def _run_git(args: list[str], repository_directory: Path) -> None:
    subprocess.run(["git", *args], cwd=repository_directory, check=True, capture_output=True)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    """A real, throwaway git repository with one commit and a metricflow/__about__.py."""
    repository_directory = tmp_path / "repo"
    (repository_directory / "metricflow").mkdir(parents=True)
    (repository_directory / "metricflow" / "__about__.py").write_text(
        'from __future__ import annotations\n\n__version__ = "0.212.0.dev0"\n'
    )
    _run_git(["init", "-q", "."], repository_directory)
    _run_git(["config", "user.email", "test@example.com"], repository_directory)
    _run_git(["config", "user.name", "Test"], repository_directory)
    _run_git(["add", "metricflow/__about__.py"], repository_directory)
    _run_git(["commit", "-q", "-m", "initial"], repository_directory)
    return repository_directory


def test_resolve_metricflow_version(git_repo: Path) -> None:
    """Should resolve MetricFlow's current version from metricflow/__about__.py."""
    assert resolve_metricflow_version(git_repo) == "0.212.0.dev0"


def test_resolve_metricflow_version_reads_released_versions_too(git_repo: Path) -> None:
    """Should resolve a real released version just as readily as a `.devN` one."""
    (git_repo / "metricflow" / "__about__.py").write_text(
        'from __future__ import annotations\n\n__version__ = "0.211.0"\n'
    )
    assert resolve_metricflow_version(git_repo) == "0.211.0"


def test_resolve_metricflow_version_raises_when_about_file_missing(tmp_path: Path) -> None:
    """A missing __about__.py should fail loudly rather than produce a malformed tag."""
    repository_directory = tmp_path / "repo"
    repository_directory.mkdir()

    with pytest.raises(FileNotFoundError):
        resolve_metricflow_version(repository_directory)


def test_resolve_metricflow_version_raises_when_version_assignment_missing(tmp_path: Path) -> None:
    """An __about__.py with no `__version__ = ...` assignment should fail loudly."""
    repository_directory = tmp_path / "repo"
    (repository_directory / "metricflow").mkdir(parents=True)
    (repository_directory / "metricflow" / "__about__.py").write_text("# no version here\n")

    with pytest.raises(RuntimeError, match="Could not find"):
        resolve_metricflow_version(repository_directory)


def test_resolve_head_commit_sha_matches_git_rev_parse(git_repo: Path) -> None:
    """Should return the same value `git rev-parse HEAD` would print."""
    expected = (
        subprocess.run(["git", "rev-parse", "HEAD"], cwd=git_repo, check=True, capture_output=True)
        .stdout.decode()
        .strip()
    )
    assert resolve_head_commit_sha(git_repo) == expected


def test_cut_adhoc_release_dry_run_does_not_create_a_tag(git_repo: Path) -> None:
    """--dry-run should compute the tag without creating it in the repository."""
    tag = cut_adhoc_release(git_repo, dry_run=True)
    assert tag.startswith("mf-entry-bin/v0.212.0.dev0+")

    existing_tags = subprocess.run(["git", "tag", "-l"], cwd=git_repo, check=True, capture_output=True).stdout.decode()
    assert tag not in existing_tags


def test_cut_adhoc_release_creates_local_tag(git_repo: Path) -> None:
    """Without --dry-run, the computed tag should actually be created in the repository."""
    tag = cut_adhoc_release(git_repo, dry_run=False)

    existing_tags = subprocess.run(["git", "tag", "-l"], cwd=git_repo, check=True, capture_output=True).stdout.decode()
    assert tag in existing_tags.splitlines()
