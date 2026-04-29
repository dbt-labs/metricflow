from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.release_tool.cli_command_runner import CliCommandRunner
from scripts.release_tool.git_manager import GitManager
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.release_helper import ReleaseHelper

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReleasePrResult:
    """Result from creating or updating a release pull request."""

    # Commit SHAs produced by release tasks.
    commit_shas: tuple[str, ...]
    # Pull request number.
    pr_number: int
    # Pull request link.
    pr_link: str


@dataclass(frozen=True)
class ReleasePrCommitTask:
    """A single commit-producing task in a release pull-request branch."""

    # Run the full task: generate changes, validate expected paths, and any user-visible work.
    action: Callable[[], None]
    # Commit message for the task's changes.
    commit_message: str


@dataclass(frozen=True)
class ReleasePrRunner:
    """Executes the common pull-request preparation flow to create or update a PR for the release."""

    # Release branch to recreate and push.
    release_branch_name: str
    # Pull request title.
    pr_title: str
    # Pull request body.
    pr_body: str
    # Existing pull request number to update, if any.
    existing_pr_number: int | None
    # Ordered release tasks to execute on the release branch.
    tasks: tuple[ReleasePrCommitTask, ...]
    # GitHub client used to create or update the release PR.
    github_client: GitHubClient
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper
    # Base branch for the release PR.
    base_branch: str = GitManager.MAIN_BRANCH
    # Whether to pull the base branch before recreating the PR branch.
    pull_base_branch: bool = True

    def run(self) -> ReleasePrResult:
        """Run the release-PR flow and return PR details."""
        self._prepare_release_branch()
        commit_shas: list[str] = []
        for task in self.tasks:
            commit_sha = self._run_task(task=task)
            if commit_sha is not None:
                commit_shas.append(commit_sha)
        self._wait_for_user_review()
        self._push_release_branch()
        return self._create_or_update_pr(commit_shas=tuple(commit_shas))

    def _prepare_release_branch(self) -> None:
        """Create or recreate the local release branch from the base branch."""
        self.release_helper.run(
            description=f"Switch to {self.base_branch}",
            action=lambda: self.release_helper.git_manager.switch_branch(self.base_branch),
        )
        if self.pull_base_branch:
            self.release_helper.run(
                description=f"Pull {self.base_branch}",
                action=self.release_helper.git_manager.pull_current_branch,
            )
        self.release_helper.run(
            description=f"Delete branch {self.release_branch_name}",
            action=lambda: self.release_helper.git_manager.delete_branch(self.release_branch_name),
        )
        self.release_helper.run(
            description=f"Create branch {self.release_branch_name}",
            action=lambda: self.release_helper.git_manager.create_branch(self.release_branch_name),
        )
        self.release_helper.run(
            description=f"Switch to {self.release_branch_name}",
            action=lambda: self.release_helper.git_manager.switch_branch(self.release_branch_name),
        )

    def _run_task(self, task: ReleasePrCommitTask) -> str | None:
        """Run one release task through staging, lint, and commit."""
        task.action()
        self.release_helper.run(
            description="Commit all changes", action=self.release_helper.git_manager.add_all_changes
        )
        run_release_lint(
            current_directory=self.release_helper.current_directory,
            cli_command_runner=self.release_helper.cli_command_runner,
            echo=self.release_helper.console.echo,
        )
        self.release_helper.run(
            description="Add all changes in case lint made fixes",
            action=self.release_helper.git_manager.add_all_changes,
        )
        commit_sha: str | None = None

        def _commit() -> None:
            nonlocal commit_sha
            commit_sha = self.release_helper.add_release_step_commit(message=task.commit_message)

        self.release_helper.run(
            description="Commit release task",
            action=_commit,
        )
        if commit_sha is None:
            raise RuntimeError(f"Commit SHA was not captured for release task '{task.commit_message}'.")
        return commit_sha

    def _wait_for_user_review(self) -> None:
        """Prompt for a manual review pause before pushing."""
        if not self.release_helper.confirm_all:
            self.release_helper.console.confirm("Make any edits to the commits now. Continue when ready?")

    def _push_release_branch(self) -> None:
        """Force-push the release branch."""
        self.release_helper.run_confirmed_remote_action(
            description=f"Force push branch {self.release_branch_name}",
            action=lambda: self.release_helper.git_manager.push_branch(self.release_branch_name, force=True),
        )

    def _create_or_update_pr(self, commit_shas: tuple[str, ...]) -> ReleasePrResult:
        """Create or update the release PR and return its details."""
        create_pr_description = f"Create or update PR '{self.pr_title}' for {self.release_branch_name}"
        self.release_helper.confirm_state_changing_remote_action(description=create_pr_description)
        self.release_helper.console.echo(create_pr_description)
        pr_number = self.github_client.create_or_update_pr(
            title=self.pr_title,
            body=self.pr_body,
            head_branch=self.release_branch_name,
            base_branch=self.base_branch,
            pr_number=self.existing_pr_number,
            draft=True,
            labels=ReleaseHelper.RELEASE_PR_LABELS,
        )
        pr_link = self.release_helper.pr_link(pr_number=pr_number)
        self.release_helper.console.echo(f"PR link: {pr_link}")
        return ReleasePrResult(commit_shas=commit_shas, pr_number=pr_number, pr_link=pr_link)


def run_release_lint(
    current_directory: Path,
    cli_command_runner: CliCommandRunner,
    echo: Callable[[str], None],
) -> None:
    """Run ``make lint``, retrying once on failure to handle auto-fix."""
    lint_command = ("make", "lint")
    description = f"Run {' '.join(lint_command)}"
    echo(description)
    env = ReleaseHelper.environment_without_hatch_variables()
    result = cli_command_runner.run(lint_command, current_directory, env=env, raise_exception_on_error=False)
    if result.returncode != 0:
        echo(f"Retrying: {description} as returned a non-zero exit code, possibly due to auto-fix.")
        echo(description)
        cli_command_runner.run(lint_command, current_directory, env=env)
