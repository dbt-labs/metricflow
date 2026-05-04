from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, ContextManager, TypeVar

from metricflow_semantics.test_helpers.terminal_helpers import mf_hyperlink
from metricflow_semantics.toolkit.string_helpers import mf_indent

from scripts.release_tool.cli_command_runner import CliCommandRunner
from scripts.release_tool.git_manager import GitManager

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_TRAILING_BANNER_SEPARATOR = "=" * 72


class ReleaseHelperConsole(ABC):
    """Console interactions used by shared release-step helpers.

    A different implementation can be used for capturing output in tests.
    """

    @abstractmethod
    def echo(self, message: str) -> None:
        """Write a message to the user."""

    @abstractmethod
    def confirm(self, message: str) -> None:
        """Prompt the user to confirm a message."""

    @abstractmethod
    def spinner(self, message: str) -> ContextManager[None]:
        """Return a spinner context for a long-running subprocess."""


@dataclass(frozen=True)
class ReleaseHelper:
    """Shared constants and operations used by release-tool steps."""

    RELEASE_PR_LABELS: ClassVar[tuple[str, ...]] = ("Skip Changelog",)
    # Prefix for metricflow release version tags.
    METRICFLOW_RELEASE_TAG_PREFIX: ClassVar[str] = "v"
    # Prefix for dbt-metricflow release version tags.
    DBT_METRICFLOW_RELEASE_TAG_PREFIX: ClassVar[str] = "dbt-metricflow/v"
    # GitHub Actions workflow file that publishes MetricFlow to PyPI after a release tag.
    CD_PUSH_METRICFLOW_TO_PYPI_WORKFLOW_FILE_NAME: ClassVar[str] = "cd-push-metricflow-to-pypi.yaml"
    # GitHub Actions workflow file that publishes dbt-metricflow to PyPI after a release tag.
    CD_PUSH_DBT_METRICFLOW_TO_PYPI_WORKFLOW_FILE_NAME: ClassVar[str] = "cd-push-dbt-metricflow-to-pypi.yaml"
    # Hatch environment variables removed before running subprocesses.
    HATCH_ENVIRONMENT_VARIABLE_NAMES: ClassVar[tuple[str, ...]] = ("VIRTUAL_ENV", "HATCH_ENV_ACTIVE")

    # GitHub repository name, such as ``dbt-labs/metricflow``.
    repository_name: str
    # Repository directory.
    current_directory: Path
    # Whether to skip confirmation prompts for remote actions.
    confirm_all: bool
    # Git manager for the repository.
    git_manager: GitManager
    # CLI command runner used by the release step.
    cli_command_runner: CliCommandRunner
    # Console used for user-visible messages and confirmations.
    console: ReleaseHelperConsole

    def run(self, description: str, action: Callable[[], object]) -> None:
        """Run an action and echo a description first."""
        self.console.echo(description)
        action()

    def confirm_state_changing_remote_action(self, description: str) -> None:
        """Ask for confirmation before a remote action changes state."""
        if self.confirm_all:
            return

        self.console.confirm(f"Continue with: {description}?")

    def run_confirmed_remote_action(self, description: str, action: Callable[[], _T]) -> _T:
        """Run a remote action after optional confirmation, echoing the description first."""
        self.confirm_state_changing_remote_action(description=description)
        self.console.echo(description)
        return action()

    def run_cli_command(self, command: tuple[str, ...]) -> None:
        """Run a CLI command from the release repository root."""

        def _action() -> None:
            self.cli_command_runner.run(command, self.current_directory)

        self.run(description=f"Run {' '.join(command)}", action=_action)

    def add_release_step_commit(self, message: str) -> str:
        """Display changed files, add a commit with the given message, and return the commit SHA."""
        changed_file_paths = self.git_manager.changed_file_paths()
        self.console.echo("Changed files:")
        for file_path in changed_file_paths:
            self.console.echo(f"  {file_path}")
        return self.git_manager.add_commit(message=message)

    @staticmethod
    def environment_without_hatch_variables() -> dict[str, str]:
        """Return the current environment with hatch virtual-environment variables removed."""
        return {
            key: value for key, value in os.environ.items() if key not in ReleaseHelper.HATCH_ENVIRONMENT_VARIABLE_NAMES
        }

    def pr_link(self, pr_number: int) -> str:
        """Return the GitHub link for a pull request."""
        return f"https://github.com/{self.repository_name}/pull/{pr_number}"

    def echo_pull_request_review_banner(self, pr_link: str) -> None:
        """Echo a trailing banner with the pull request URL and a review reminder."""
        self.console.echo("")
        self.console.echo(_TRAILING_BANNER_SEPARATOR)
        self.console.echo("Please have this pull request reviewed and approved before continuing:")
        self.console.echo("")
        self.console.echo(mf_indent(mf_hyperlink(pr_link)))
        self.console.echo("")
        self.console.echo(_TRAILING_BANNER_SEPARATOR)

    def echo_github_actions_workflow_approval_banner(self, workflow_file_name: str) -> None:
        """Echo a trailing banner with the workflow URL and an approval reminder."""
        workflow_url = f"https://github.com/{self.repository_name}/actions/workflows/{workflow_file_name}"
        self.console.echo("")
        self.console.echo(_TRAILING_BANNER_SEPARATOR)
        self.console.echo("Please go to this workflow and approve it before continuing:")
        self.console.echo("")
        self.console.echo(mf_indent(mf_hyperlink(workflow_url)))
        self.console.echo("")
        self.console.echo(_TRAILING_BANNER_SEPARATOR)
