from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import ClassVar, Protocol, cast

from github import Auth, Github

logger = logging.getLogger(__name__)


class _MergeStatus(Protocol):
    """Result of a pull request merge operation."""

    @property
    def merged(self) -> bool:
        """Return true when the merge succeeded."""
        ...

    @property
    def sha(self) -> str:
        """Return the merge commit SHA."""
        ...


class _PullRequest(Protocol):
    """Pull request fields used by `GitHubClient`."""

    @property
    def number(self) -> int:
        """Return the pull request number."""
        ...

    @property
    def merged(self) -> bool:
        """Return true when the pull request has been merged."""
        ...

    @property
    def merge_commit_sha(self) -> str | None:
        """Return the merge commit SHA when available."""
        ...

    @property
    def mergeable_state(self) -> str:
        """Return the mergeable state (e.g. 'clean', 'blocked', 'unstable')."""
        ...

    @property
    def title(self) -> str:
        """Return the pull request title."""
        ...

    @property
    def body(self) -> str | None:
        """Return the pull request body."""
        ...

    @property
    def base(self) -> _PullRequestBase:
        """Return the base branch metadata."""
        ...

    def add_to_labels(self, *labels: str) -> None:
        """Add labels to the pull request."""
        ...

    def edit(
        self,
        *,
        title: str | None = None,
        body: str | None = None,
        base: str | None = None,
    ) -> None:
        """Update pull request fields."""
        ...

    def merge(self, merge_method: str | None = None) -> _MergeStatus:
        """Merge the pull request using the specified merge method."""
        ...

    def update(self) -> None:
        """Re-fetch pull request data from the API."""
        ...


class _Workflow(Protocol):
    """Workflow dispatch API used by `GitHubClient`."""

    def create_dispatch(self, ref: str, inputs: dict[str, str], throw: bool = False) -> bool:
        """Run the workflow for the given ref and inputs."""
        ...


class _PullRequestBase(Protocol):
    """Base branch metadata used by `GitHubClient`."""

    @property
    def ref(self) -> str:
        """Return the base branch name."""
        ...


class _Repository(Protocol):
    """Repository API used by `GitHubClient`."""

    def create_pull(
        self,
        base: str,
        head: str,
        *,
        title: str,
        body: str | None,
        maintainer_can_modify: bool,
        draft: bool,
    ) -> _PullRequest:
        """Create a pull request."""
        ...

    def get_pull(self, number: int) -> _PullRequest:
        """Return a pull request by number."""
        ...

    def get_workflow(self, id_or_file_name: str | int) -> _Workflow:
        """Return a workflow by ID or file name."""
        ...


class _GitHubApi(Protocol):
    """GitHub API surface used by `GitHubClient`."""

    def get_repo(self, full_name_or_id: int | str) -> _Repository:
        """Return a repository by full name or ID."""
        ...


class GitHubClient:
    """Lightweight wrapper for GitHub operations through PyGithub."""

    # GitHub mergeable state that indicates a PR is ready to merge.
    PR_MERGE_READY_STATE: ClassVar[str] = "clean"
    # GitHub merge method for squash commits.
    SQUASH_MERGE_METHOD: ClassVar[str] = "squash"

    def __init__(
        self,
        access_token: str,
        repository_name: str,
        github_api: _GitHubApi | None = None,
    ) -> None:
        """Initialize a client for `repository_name`, such as `dbt-labs/metricflow`."""
        if github_api is None:
            github_api = cast(_GitHubApi, Github(auth=Auth.Token(access_token)))

        self._repository = github_api.get_repo(repository_name)

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
            body=body,
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
            body=body,
            base=base_branch,
        )
        if labels:
            existing_pull_request.add_to_labels(*labels)
        return existing_pull_request.number

    def _find_pull_request(self, pr_number: int) -> _PullRequest:
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

    def is_pr_merge_ready(self, pr_number: int) -> bool:
        """Return true when the pull request is ready to merge."""
        return self.is_mergeable_state_ready(self.get_pr_mergeable_state(pr_number))

    @classmethod
    def is_mergeable_state_ready(cls, mergeable_state: str) -> bool:
        """Return true when the GitHub mergeable state indicates merge readiness."""
        return mergeable_state == cls.PR_MERGE_READY_STATE

    def merge_pr(self, pr_number: int, merge_method: str | None = None) -> str:
        """Merge a pull request and return the merge commit SHA."""
        pull_request = self._repository.get_pull(pr_number)
        if pull_request.merged:
            raise RuntimeError(f"Pull request #{pr_number} has already been merged.")

        merge_status = pull_request.merge(merge_method=merge_method)
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
