from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import ClassVar

from dulwich import porcelain
from dulwich.errors import NotGitRepository
from dulwich.refs import HEADREF
from dulwich.repo import Repo

logger = logging.getLogger(__name__)


class GitManager:
    """Lightweight wrapper for git operations through Dulwich."""

    MAIN_BRANCH: ClassVar[str] = "main"

    def __init__(self, repository_path: Path | str) -> None:
        """Initialize a manager for the repository at `repository_path`."""
        self._repository_path = Path(repository_path)

    def add_file(self, file_path: Path | str) -> None:
        """Stage a file in the repository index."""
        porcelain.add(repo=self._repository_path, paths=[file_path])

    def is_git_repo(self) -> bool:
        """Return true if the repository path is a Git repository."""
        try:
            with Repo(self._repository_path):
                return True
        except NotGitRepository:
            return False

    def has_remote_repository(self, repository_name: str) -> bool:
        """Return true if the origin remote points to `repository_name`."""
        expected_repository_name = repository_name.lower()
        try:
            with Repo(self._repository_path) as repo:
                remote_url = repo.get_config().get((b"remote", b"origin"), b"url").decode("utf-8")
        except (KeyError, NotGitRepository):
            return False

        return self._normalize_remote_url(remote_url=remote_url) == expected_repository_name

    @staticmethod
    def _normalize_remote_url(remote_url: str) -> str:
        """Normalize supported GitHub remote URL forms to owner/repository.git."""
        cleaned_remote_url = remote_url.strip().removesuffix("/").lower()
        if cleaned_remote_url.startswith("git@github.com:"):
            return cleaned_remote_url.removeprefix("git@github.com:")
        if cleaned_remote_url.startswith("ssh://git@github.com/"):
            return cleaned_remote_url.removeprefix("ssh://git@github.com/")
        if cleaned_remote_url.startswith("https://github.com/"):
            return cleaned_remote_url.removeprefix("https://github.com/")

        return cleaned_remote_url

    def add_all_changes(self) -> None:
        """Stage all repository changes, including untracked files and deletions."""
        porcelain.add(repo=self._repository_path, paths=None)

        deleted_file_paths = []
        status = porcelain.status(repo=self._repository_path)
        for file_path in status.unstaged:
            file_path_str = os.fsdecode(file_path)
            if not self._repository_path.joinpath(file_path_str).exists():
                deleted_file_paths.append(file_path_str)

        if deleted_file_paths:
            porcelain.remove(repo=self._repository_path, paths=deleted_file_paths, cached=True)

    def add_commit(
        self,
        message: str,
        author: bytes | None = None,
        committer: bytes | None = None,
    ) -> str:
        """Create a commit from staged changes and return its SHA."""
        commit_sha = porcelain.commit(
            repo=self._repository_path,
            message=message,
            author=author,
            committer=committer,
        )
        return commit_sha.decode("ascii")

    def find_commit_sha(self, commit_message_regex: str) -> str | None:
        """Return the newest commit SHA with a message matching the regex."""
        commit_message_pattern = re.compile(commit_message_regex)
        with Repo(self._repository_path) as repo:
            for commit_entry in repo.get_walker():
                commit_message = commit_entry.commit.message.decode("utf-8", errors="replace")
                if commit_message_pattern.search(commit_message):
                    return commit_entry.commit.id.decode("ascii")

        return None

    def create_branch(self, branch_name: str, objectish: str | None = None) -> None:
        """Create a branch from `objectish`, or HEAD when omitted."""
        porcelain.branch_create(repo=self._repository_path, name=branch_name, objectish=objectish)

    def branch_exists(self, branch_name: str) -> bool:
        """Return true when a local branch with `branch_name` exists."""
        branch_ref_name = f"refs/heads/{branch_name}".encode("utf-8")
        with Repo(self._repository_path) as repo:
            return branch_ref_name in repo.refs.keys()

    def switch_branch(self, branch_name: str) -> None:
        """Switch the repository worktree to `branch_name`."""
        porcelain.checkout(repo=self._repository_path, target=branch_name)

    def is_state_clean(self) -> bool:
        """Return true when there are no staged, unstaged, or untracked changes."""
        status = porcelain.status(repo=self._repository_path)
        has_staged_changes = any(status.staged[change_type] for change_type in status.staged)
        return not has_staged_changes and not status.unstaged and not status.untracked

    def changed_file_paths(self) -> tuple[str, ...]:
        """Return repository-relative paths with staged, unstaged, or untracked changes."""
        status = porcelain.status(repo=self._repository_path)
        changed_file_paths = set()
        for staged_file_paths in status.staged.values():
            for file_path in staged_file_paths:
                changed_file_paths.add(os.fsdecode(file_path))
        for file_path in status.unstaged:
            changed_file_paths.add(os.fsdecode(file_path))
        for file_path in status.untracked:
            changed_file_paths.add(os.fsdecode(file_path))

        return tuple(sorted(changed_file_paths))

    def delete_branch(self, branch_name: str) -> None:
        """Delete a local branch. No-op if the branch does not exist."""
        porcelain.branch_delete(repo=self._repository_path, name=branch_name)

    def push_branch(self, branch_name: str, remote_location: str = "origin", force: bool = False) -> None:
        """Push a local branch to the matching branch on the remote."""
        refspec = f"refs/heads/{branch_name}:refs/heads/{branch_name}"
        if force:
            refspec = f"+{refspec}"
        porcelain.push(repo=self._repository_path, remote_location=remote_location, refspecs=refspec)

    def pull_current_branch(self, remote_location: str = "origin") -> None:
        """Pull the current local branch from the matching branch on the remote."""
        with Repo(self._repository_path) as repo:
            current_branch_ref = repo.refs.follow(HEADREF)[0][1]

        local_branch_prefix = b"refs/heads/"
        if not current_branch_ref.startswith(local_branch_prefix):
            raise ValueError("Unable to pull because HEAD is not attached to a local branch.")

        current_branch = current_branch_ref.removeprefix(local_branch_prefix).decode("utf-8")
        refspec = f"refs/heads/{current_branch}:refs/heads/{current_branch}"
        porcelain.pull(repo=self._repository_path, remote_location=remote_location, refspecs=refspec)

    def cherry_pick(self, commit_sha: str) -> str:
        """Cherry-pick a commit onto the current branch and return the new commit SHA."""
        result = porcelain.cherry_pick(repo=self._repository_path, committish=commit_sha.encode("ascii"))
        if result is None:
            raise RuntimeError(f"Cherry-pick of {commit_sha} failed or produced conflicts.")
        return result.decode("ascii")

    def push_tag(
        self,
        tag_name: str,
        remote_location: str = "origin",
        objectish: str = "HEAD",
        lightweight: bool = True,
        force: bool = False,
        message: str | None = None,
        author: str | bytes | None = None,
    ) -> None:
        """Create a tag and push it to the remote, using a lightweight tag by default."""
        porcelain.tag_create(
            repo=self._repository_path,
            tag=tag_name,
            annotated=not lightweight,
            objectish=objectish,
            message=message,
            author=author,
        )
        refspec = f"refs/tags/{tag_name}:refs/tags/{tag_name}"
        if force:
            refspec = f"+{refspec}"
        porcelain.push(repo=self._repository_path, remote_location=remote_location, refspecs=refspec)
