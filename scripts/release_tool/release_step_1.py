from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

import click

from msi_pydantic_shim import BaseModel
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.package_version import PackageVersionUpdate
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_pr_runner import ReleasePrCommitTask, ReleasePrRunner

logger = logging.getLogger(__name__)


class ReleaseStep1State(BaseModel):
    """State captured during release step 1."""

    # MetricFlow package version for the release.
    metricflow_package_version: str
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
class ReleaseStep1Runner:
    """Executes release-tool step 1.

    Step 1 prepares the release branch and release pull request by:

    * Creating or recreating the local step-1 branch from `main`
    * Generating the changelog updates
    * Generating the FOSSA attribution update
    * Updating the package version
    * Linting and validating the resulting file changes
    * Committing the changelog, attribution, and version updates
    * Force-pushing the branch
    * Creating a draft release PR
    """

    # CLI commands that must be available on PATH for step 1.
    REQUIRED_CLI_COMMANDS: ClassVar[tuple[str, ...]] = ("fossa", "changie")
    # Commit message used for the changelog update commit.
    CHANGELOG_COMMIT_MESSAGE: ClassVar[str] = "Update change log"
    # Commit message used for the FOSSA attribution update commit.
    ATTRIBUTION_COMMIT_MESSAGE: ClassVar[str] = "Update attribution from FOSSA"
    # File path that changelog generation is allowed to modify.
    ALLOWED_CHANGELOG_FILE_PATH: ClassVar[str] = "CHANGELOG.md"
    # Directory that changelog generation is allowed to modify.
    ALLOWED_CHANGE_DIRECTORY: ClassVar[str] = ".changes/"
    # File path that FOSSA attribution is allowed to modify.
    ALLOWED_ATTRIBUTION_FILE_PATH: ClassVar[str] = "ATTRIBUTION.md"
    # Command used to generate the FOSSA attribution report.
    FOSSA_REPORT_COMMAND: ClassVar[tuple[str, ...]] = ("fossa", "report", "attribution", "--format", "markdown")
    # File path that the version update is allowed to modify.
    ALLOWED_ABOUT_FILE_PATH: ClassVar[str] = "metricflow/__about__.py"
    # Commit message used for the package version update commit.
    VERSION_COMMIT_MESSAGE_TEMPLATE: ClassVar[str] = "Update `metricflow` package version to {version}"
    # Release version to prepare.
    version: str
    # Environment variables used by the release step.
    environment: Mapping[str, str]
    # GitHub client used to create the release pull request.
    github_client: GitHubClient
    # Existing saved state from a previous step-1 run, if any.
    existing_state: ReleaseStep1State | None
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper

    def run(self) -> ReleaseStep1State:
        """Run step 1 and return state to save."""
        release_branch_name = f"{self.environment['GITHUB_USERNAME']}/release_pr/{self.version}/step_1"
        pr_result = ReleasePrRunner(
            release_branch_name=release_branch_name,
            pr_title=self._release_step_1_pr_title(),
            pr_body=self._release_step_1_pr_body(),
            existing_pr_number=self.existing_state.pr_number if self.existing_state is not None else None,
            tasks=self._release_pr_commit_tasks(),
            github_client=self.github_client,
            release_helper=self.release_helper,
        ).run()

        self.release_helper.echo_pull_request_review_banner(pr_link=pr_result.pr_link)
        return ReleaseStep1State(
            metricflow_package_version=self.version,
            branch_name=release_branch_name,
            pr_number=pr_result.pr_number,
            pr_link=pr_result.pr_link,
        )

    def _release_pr_commit_tasks(self) -> tuple[ReleasePrCommitTask, ...]:
        """Return the per-commit release-PR tasks for step 1."""
        version_commit_message = ReleaseStep1Runner.VERSION_COMMIT_MESSAGE_TEMPLATE.format(version=self.version)
        package_version_update = PackageVersionUpdate(
            version=self.version,
            release_helper=self.release_helper,
            hatch_project_directory=self.release_helper.current_directory,
        )
        return (
            ReleasePrCommitTask(
                action=self._apply_changelog,
                commit_message=ReleaseStep1Runner.CHANGELOG_COMMIT_MESSAGE,
            ),
            ReleasePrCommitTask(
                action=self._apply_fossa_attribution,
                commit_message=ReleaseStep1Runner.ATTRIBUTION_COMMIT_MESSAGE,
            ),
            package_version_update.as_release_pr_commit_task(
                commit_message=version_commit_message,
            ),
        )

    def _apply_changelog(self) -> None:
        """Generate changelog changes, then ensure only the allowed paths were touched."""
        self._generate_changelog()
        self._check_step_1_changes()

    def _generate_changelog(self) -> None:
        """Generate changelog changes for the release."""
        self.release_helper.run_cli_command(command=("changie", "batch", self.version))
        self.release_helper.run_cli_command(command=("changie", "merge"))

    def _apply_fossa_attribution(self) -> None:
        """Generate FOSSA attribution, then ensure only the attribution file was touched."""
        self._generate_fossa_attribution()
        self._check_step_1_attribution_changes()

    def _generate_fossa_attribution(self) -> None:
        """Generate FOSSA attribution changes for the release."""
        self.release_helper.run_cli_command(command=("fossa", "analyze"))
        self._run_fossa_report()

    def _check_step_1_changes(self) -> None:
        """Raise if step 1 changed files outside the changelog inputs or output."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [
            file_path
            for file_path in changed_file_paths
            if file_path != ReleaseStep1Runner.ALLOWED_CHANGELOG_FILE_PATH
            and not file_path.startswith(ReleaseStep1Runner.ALLOWED_CHANGE_DIRECTORY)
        ]
        if unexpected_file_paths:
            raise click.ClickException(
                "Release step 1 may only change CHANGELOG.md or files under .changes. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    def _run_fossa_report(self) -> None:
        """Run ``fossa report attribution`` and write the output to ATTRIBUTION.md."""
        description = (
            f"Run {' '.join(ReleaseStep1Runner.FOSSA_REPORT_COMMAND)} > "
            f"{ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH}"
        )
        with self.release_helper.console.spinner(description):
            result = self.release_helper.cli_command_runner.run(
                ReleaseStep1Runner.FOSSA_REPORT_COMMAND,
                self.release_helper.current_directory,
                capture_output=True,
            )
        attribution_path = self.release_helper.current_directory / ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH
        attribution_path.write_bytes(result.stdout)

    def _check_step_1_attribution_changes(self) -> None:
        """Raise if FOSSA attribution changed files outside ATTRIBUTION.md."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [
            file_path
            for file_path in changed_file_paths
            if file_path != ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH
        ]
        if unexpected_file_paths:
            raise click.ClickException(
                f"FOSSA attribution may only change {ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH}. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    def _release_step_1_pr_title(self) -> str:
        """Return the pull request title for release step 1."""
        return f"Release `metricflow` {self.version}"

    def _release_step_1_pr_body(self) -> str:
        """Return the pull request body for release step 1."""
        return f"Release `metricflow` {self.version} with updated changelog and attribution."
