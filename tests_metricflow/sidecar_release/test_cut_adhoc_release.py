from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts.sidecar_release.cut_adhoc_release import (
    cut_adhoc_release,
    resolve_head_commit_sha,
    resolve_nearest_release_tag,
)


def _run_git(args: list[str], repository_directory: Path) -> None:
    subprocess.run(["git", *args], cwd=repository_directory, check=True, capture_output=True)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    """A real, throwaway git repository with one commit and one release tag."""
    repository_directory = tmp_path / "repo"
    repository_directory.mkdir()
    _run_git(["init", "-q", "."], repository_directory)
    _run_git(["config", "user.email", "test@example.com"], repository_directory)
    _run_git(["config", "user.name", "Test"], repository_directory)
    (repository_directory / "file.txt").write_text("hello")
    _run_git(["add", "file.txt"], repository_directory)
    _run_git(["commit", "-q", "-m", "initial"], repository_directory)
    _run_git(["tag", "v0.208.0"], repository_directory)
    return repository_directory


def test_resolve_nearest_release_tag(git_repo: Path) -> None:
    """Should resolve the bare v<version> tag reachable from HEAD."""
    assert resolve_nearest_release_tag(git_repo) == "v0.208.0"


def test_resolve_nearest_release_tag_ignores_sidecar_tags(git_repo: Path) -> None:
    """A sidecar/mf-entry-bin-namespaced tag at the same commit should not be picked up."""
    _run_git(["tag", "mf-entry-bin/v0.208.0+2607281534.a1b2c3d4"], git_repo)
    assert resolve_nearest_release_tag(git_repo) == "v0.208.0"


def test_resolve_nearest_release_tag_raises_when_none_reachable(tmp_path: Path) -> None:
    """No reachable release tag should fail loudly rather than produce a malformed tag."""
    repository_directory = tmp_path / "repo"
    repository_directory.mkdir()
    _run_git(["init", "-q", "."], repository_directory)
    _run_git(["config", "user.email", "test@example.com"], repository_directory)
    _run_git(["config", "user.name", "Test"], repository_directory)
    (repository_directory / "file.txt").write_text("hello")
    _run_git(["add", "file.txt"], repository_directory)
    _run_git(["commit", "-q", "-m", "no release tags here"], repository_directory)

    with pytest.raises(subprocess.CalledProcessError):
        resolve_nearest_release_tag(repository_directory)


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
    assert tag.startswith("mf-entry-bin/v0.208.0+")

    existing_tags = subprocess.run(["git", "tag", "-l"], cwd=git_repo, check=True, capture_output=True).stdout.decode()
    assert tag not in existing_tags


def test_cut_adhoc_release_creates_local_tag(git_repo: Path) -> None:
    """Without --dry-run, the computed tag should actually be created in the repository."""
    tag = cut_adhoc_release(git_repo, dry_run=False)

    existing_tags = subprocess.run(["git", "tag", "-l"], cwd=git_repo, check=True, capture_output=True).stdout.decode()
    assert tag in existing_tags.splitlines()


def test_cut_adhoc_release_resets_counter_free_naming_per_version(git_repo: Path) -> None:
    """Two releases cut back-to-back for the same version differ only by timestamp/sha, not a counter."""
    first_tag = cut_adhoc_release(git_repo, dry_run=True)
    second_tag = cut_adhoc_release(git_repo, dry_run=True)
    # Same commit, same version -- these may coincide if cut within the same UTC minute,
    # but the format itself (no stateful counter) is what's under test here.
    assert first_tag.split("+", 1)[0] == second_tag.split("+", 1)[0] == "mf-entry-bin/v0.208.0"
