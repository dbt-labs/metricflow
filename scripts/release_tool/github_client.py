from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from enum import Enum
from typing import ClassVar

from github import Auth, Github
from github.GithubObject import NotSet
from github.PullRequest import PullRequest
from github.Repository import Repository

logger = logging.getLogger(__name__)


class GitHubMergeMethod(str, Enum):
    """Supported GitHub pull request merge methods."""

    MERGE = "merge"
    SQUASH = "squash"
    REBASE = "rebase"


class GitHubReleaseMakeLatest(str, Enum):
    """Supported GitHub release latest-status options."""

    TRUE = "true"
    FALSE = "false"
    LEGACY = "legacy"


class GitHubClient(ABC):
    """Abstract base for GitHub operations used by the MetricFlow release tool."""

    # GitHub mergeable state that indicates a PR is ready to merge.
    PR_MERGE_READY_STATE: ClassVar[str] = "clean"

    @classmethod
    def is_mergeable_state_ready(cls, mergeable_state: str) -> bool:
        """Return true when the GitHub mergeable state indicates merge readiness."""
        return mergeable_state == cls.PR_MERGE_READY_STATE

    def is_pr_merge_ready(self, pr_number: int) -> bool:
        """Return true when the pull request is ready to merge."""
        return self.is_mergeable_state_ready(self.get_pr_mergeable_state(pr_number))

    @abstractmethod
    def create_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Create a pull request, apply any labels, and return its PR number."""
        ...

    @abstractmethod
    def create_or_update_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        pr_number: int | None = None,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Create a pull request or update an existing one."""
        ...

    @abstractmethod
    def is_pr_merged(self, pr_number: int) -> bool:
        """Return true when the pull request has been merged."""
        ...

    @abstractmethod
    def get_pr_merge_commit_sha(self, pr_number: int) -> str:
        """Return the merge commit SHA for a pull request."""
        ...

    @abstractmethod
    def get_pr_mergeable_state(self, pr_number: int) -> str:
        """Return the mergeable state of a pull request after refreshing its data."""
        ...

    @abstractmethod
    def merge_pr(self, pr_number: int, merge_method: GitHubMergeMethod | None = None) -> str:
        """Merge a pull request and return the merge commit SHA."""
        ...

    @abstractmethod
    def run_workflow(
        self,
        workflow_id_or_file_name: str | int,
        ref: str,
        inputs: Mapping[str, str] | None = None,
    ) -> bool:
        """Dispatch a workflow with a branch, tag, or commit SHA as ``ref``."""
        ...

    @abstractmethod
    def create_release_note(
        self,
        tag_name: str,
        title: str,
        body: str,
        make_latest: GitHubReleaseMakeLatest = GitHubReleaseMakeLatest.TRUE,
    ) -> str:
        """Create a GitHub release note for the given tag and return the release URL."""
        ...


class PyGithubClient(GitHubClient):
    """GitHub API access implemented with PyGithub."""

    def __init__(
        self,
        access_token: str,
        repository_name: str,
        github_api: Github | None = None,
    ) -> None:
        """Initialize a client for `repository_name`, such as `dbt-labs/metricflow`."""
        if github_api is None:
            github_api = Github(auth=Auth.Token(access_token))

        self._repository: Repository = github_api.get_repo(repository_name)

    def create_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Create a pull request, apply any labels, and return its PR number."""
        pull_request = self._repository.create_pull(
            title=title,
            body=NotSet if body is None else body,
            head=head_branch,
            base=base_branch,
            maintainer_can_modify=maintainer_can_modify,
            draft=draft,
        )
        if labels:
            pull_request.add_to_labels(*labels)
        return pull_request.number

    def create_or_update_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        pr_number: int | None = None,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Create a pull request or update an existing one.

        When `pr_number` is provided, that pull request is updated in place.
        Otherwise, a new pull request is created.
        """
        existing_pull_request = self._find_pull_request(pr_number=pr_number) if pr_number is not None else None
        if existing_pull_request is None:
            return self.create_pr(
                title=title,
                body=body,
                head_branch=head_branch,
                base_branch=base_branch,
                maintainer_can_modify=maintainer_can_modify,
                draft=draft,
                labels=labels,
            )

        existing_pull_request.edit(
            title=title,
            body=NotSet if body is None else body,
            base=base_branch,
        )
        if labels:
            existing_pull_request.add_to_labels(*labels)
        return existing_pull_request.number

    def _find_pull_request(self, pr_number: int) -> PullRequest:
        """Return the pull request for `pr_number`."""
        return self._repository.get_pull(pr_number)

    def is_pr_merged(self, pr_number: int) -> bool:
        """Return true when the pull request has been merged."""
        return self._repository.get_pull(pr_number).merged

    def get_pr_merge_commit_sha(self, pr_number: int) -> str:
        """Return the merge commit SHA for a pull request."""
        pull_request = self._repository.get_pull(pr_number)
        pull_request.update()
        merge_commit_sha = pull_request.merge_commit_sha
        if merge_commit_sha is None:
            raise RuntimeError(f"Pull request #{pr_number} does not have a merge commit SHA.")
        return merge_commit_sha

    def get_pr_mergeable_state(self, pr_number: int) -> str:
        """Return the mergeable state of a pull request after refreshing its data.

        Common values include ``clean`` (ready to merge), ``blocked``
        (e.g. missing approvals or failing checks), and ``unstable``.
        """
        pull_request = self._repository.get_pull(pr_number)
        pull_request.update()
        return pull_request.mergeable_state

    def merge_pr(self, pr_number: int, merge_method: GitHubMergeMethod | None = None) -> str:
        """Merge a pull request and return the merge commit SHA."""
        pull_request = self._repository.get_pull(pr_number)
        if pull_request.merged:
            raise RuntimeError(f"Pull request #{pr_number} has already been merged.")

        merge_status = pull_request.merge(merge_method=NotSet if merge_method is None else merge_method.value)
        if not merge_status.merged:
            raise RuntimeError(f"Failed to merge PR #{pr_number}.")
        return merge_status.sha

    def run_workflow(
        self,
        workflow_id_or_file_name: str | int,
        ref: str,
        inputs: Mapping[str, str] | None = None,
    ) -> bool:
        """Dispatch a workflow with a branch, tag, or commit SHA as `ref`."""
        workflow = self._repository.get_workflow(workflow_id_or_file_name)
        return workflow.create_dispatch(ref=ref, inputs=dict(inputs or {}))

    def create_release_note(
        self,
        tag_name: str,
        title: str,
        body: str,
        make_latest: GitHubReleaseMakeLatest = GitHubReleaseMakeLatest.TRUE,
    ) -> str:
        """Create a GitHub release note for the given tag and return the release URL."""
        release = self._repository.create_git_release(
            tag=tag_name,
            name=title,
            message=body,
            make_latest=make_latest.value,
        )
        return release.html_url
