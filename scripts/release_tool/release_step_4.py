from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import click

from msi_pydantic_shim import BaseModel
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.package_version import PackageVersionUpdate
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_pr_runner import ReleasePrCommitTask, ReleasePrRunner
from scripts.release_tool.release_step_1 import ReleaseStep1State

logger = logging.getLogger(__name__)


class ReleaseStep4State(BaseModel):
    """State captured during release step 4."""

    # `metricflow` version pinned in `dbt-metricflow` (from step 1).
    metricflow_package_version: str
    # `dbt-metricflow` package version for this release.
    dbt_metricflow_package_version: str
    # Release branch name.
    branch_name: str
    # Pull request number for above branch.
    pr_number: int
    # Release pull request link.
    pr_link: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False


@dataclass(frozen=True)
class ReleaseStep4Runner:
    """Executes release-tool step 4.

    Step 4 prepares the `dbt-metricflow` release branch and release pull
    request by:

    * Creating or recreating the local step-4 branch from `main`
    * Updating the pinned `metricflow` requirement for `dbt-metricflow` and
      committing
    * Updating the `dbt-metricflow` package version, linting, and committing
    * Force-pushing the branch
    * Creating a draft release PR targeting `main`
    """

    # Subdirectory of the monorepo that is the `dbt-metricflow` hatch project.
    DBT_METRICFLOW_SUBDIRECTORY: ClassVar[str] = "dbt-metricflow"
    # Path to the dbt-metricflow requirements file that pins the `metricflow` package.
    REQUIREMENTS_FILE_PATH: ClassVar[str] = "dbt-metricflow/requirements-files/requirements-metricflow.txt"
    # File path that the `dbt-metricflow` version update is allowed to modify.
    ALLOWED_DBT_ABOUT_FILE_PATH: ClassVar[str] = "dbt-metricflow/dbt_metricflow/__about__.py"
    # Name of the `metricflow` package pinned in the requirements file.
    METRICFLOW_PACKAGE_NAME: ClassVar[str] = "metricflow"
    # Commit message template for the dbt-metricflow package version update.
    DBT_VERSION_COMMIT_MESSAGE_TEMPLATE: ClassVar[str] = "Update `dbt-metricflow` package version to {version}"
    # CLI commands that must be available on PATH for step 4.
    REQUIRED_CLI_COMMANDS: ClassVar[tuple[str, ...]] = ("hatch",)
    # New semantic version for the `dbt-metricflow` package release.
    dbt_metricflow_version: str
    # State saved from step 1.
    step_1_state: ReleaseStep1State
    # Environment variables used by the release step.
    environment: Mapping[str, str]
    # GitHub client used to create the release pull request.
    github_client: GitHubClient
    # Existing saved state from a previous step-4 run, if any.
    existing_state: ReleaseStep4State | None
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper

    def run(self) -> ReleaseStep4State:
        """Run step 4 and return state to save."""
        mf_version = self.step_1_state.metricflow_package_version
        dbt_metricflow_version = self.dbt_metricflow_version
        release_branch_name = f"{self.environment['GITHUB_USERNAME']}/release_pr/{mf_version}/step_4"
        requirement_line = self._requirement_line(version=mf_version)
        pr_title = self._release_step_4_pr_title(
            metricflow_version=mf_version,
            dbt_metricflow_version=dbt_metricflow_version,
        )
        pr_result = ReleasePrRunner(
            release_branch_name=release_branch_name,
            pr_title=pr_title,
            pr_body=self._release_step_4_pr_body(
                requirement_line=requirement_line,
                dbt_version=dbt_metricflow_version,
                release_pr_link=self.step_1_state.pr_link,
            ),
            existing_pr_number=self.existing_state.pr_number if self.existing_state is not None else None,
            tasks=self._release_pr_commit_tasks(requirement_line=requirement_line),
            github_client=self.github_client,
            release_helper=self.release_helper,
        ).run()

        self.release_helper.echo_pull_request_review_banner(pr_link=pr_result.pr_link)
        return ReleaseStep4State(
            metricflow_package_version=mf_version,
            dbt_metricflow_package_version=dbt_metricflow_version,
            branch_name=release_branch_name,
            pr_number=pr_result.pr_number,
            pr_link=pr_result.pr_link,
        )

    def _release_pr_commit_tasks(self, requirement_line: str) -> tuple[ReleasePrCommitTask, ...]:
        """Return the per-commit release-PR tasks for step 4."""
        dbt_version_commit_message = ReleaseStep4Runner.DBT_VERSION_COMMIT_MESSAGE_TEMPLATE.format(
            version=self.dbt_metricflow_version
        )
        requirements_commit_message = f"Update `dbt-metricflow` to use `{requirement_line}`"
        package_version_update = PackageVersionUpdate(
            version=self.dbt_metricflow_version,
            release_helper=self.release_helper,
            hatch_project_directory=self._dbt_metricflow_root(),
        )

        def requirement_pin_action() -> None:
            self._update_requirement_pin(requirement_line=requirement_line)

        return (
            ReleasePrCommitTask(
                action=requirement_pin_action,
                validate=self._check_requirements_only_changes,
                commit_message=requirements_commit_message,
            ),
            package_version_update.as_release_pr_commit_task(
                commit_message=dbt_version_commit_message,
            ),
        )

    @staticmethod
    def _requirement_line(version: str) -> str:
        """Return the pinned requirement line for the `metricflow` package."""
        return f"{ReleaseStep4Runner.METRICFLOW_PACKAGE_NAME}=={version}"

    def _update_requirement_pin(self, requirement_line: str) -> None:
        """Write the requirements pin."""
        self.release_helper.run(
            description=f"Update {ReleaseStep4Runner.REQUIREMENTS_FILE_PATH} to pin `{requirement_line}`",
            action=lambda: self._write_requirements_file(requirement_line=requirement_line),
        )

    def _write_requirements_file(self, requirement_line: str) -> None:
        """Write the pinned `metricflow` requirement to the dbt-metricflow requirements file."""
        requirements_path = self.release_helper.current_directory / ReleaseStep4Runner.REQUIREMENTS_FILE_PATH
        requirements_path.write_text(f"{requirement_line}\n")

    def _dbt_metricflow_root(self) -> Path:
        """Return the dbt-metricflow hatch project directory."""
        return self.release_helper.current_directory / ReleaseStep4Runner.DBT_METRICFLOW_SUBDIRECTORY

    def _check_requirements_only_changes(self) -> None:
        """Raise if the requirements update changed files outside the pin file."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [
            file_path for file_path in changed_file_paths if file_path != ReleaseStep4Runner.REQUIREMENTS_FILE_PATH
        ]
        if unexpected_file_paths:
            raise click.ClickException(
                f"Pin update may only change {ReleaseStep4Runner.REQUIREMENTS_FILE_PATH}. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    @staticmethod
    def _release_step_4_pr_title(metricflow_version: str, dbt_metricflow_version: str) -> str:
        """Return the pull request title for release step 4."""
        return f"Release `dbt-metricflow` {dbt_metricflow_version} with `metricflow` {metricflow_version}"

    def _release_step_4_pr_body(self, requirement_line: str, dbt_version: str, release_pr_link: str) -> str:
        """Return the pull request body for release step 4."""
        return (
            f"Release `dbt-metricflow` {dbt_version} with updated dependency `{requirement_line}`.\n\n"
            f"Release PR: {release_pr_link}"
        )
