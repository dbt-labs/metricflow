from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import click

from msi_pydantic_shim import BaseModel
from scripts.release_tool import RELEASE_TOOL_DIRECTORY_ANCHOR
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
    * Generating the ORT attribution update
    * Updating the package version
    * Linting and validating the resulting file changes
    * Committing the changelog, attribution, and version updates
    * Force-pushing the branch
    * Creating a draft release PR
    """

    # CLI commands that must be available on PATH for step 1.
    REQUIRED_CLI_COMMANDS: ClassVar[tuple[str, ...]] = ("docker", "changie")
    # Commit message used for the changelog update commit.
    CHANGELOG_COMMIT_MESSAGE: ClassVar[str] = "Update change log"
    # Commit message used for the ORT attribution update commit.
    ATTRIBUTION_COMMIT_MESSAGE: ClassVar[str] = "Update attribution from ORT"
    # File path that changelog generation is allowed to modify.
    ALLOWED_CHANGELOG_FILE_PATH: ClassVar[str] = "CHANGELOG.md"
    # Directory that changelog generation is allowed to modify.
    ALLOWED_CHANGE_DIRECTORY: ClassVar[str] = ".changes/"
    # File path that ORT attribution is allowed to modify.
    ALLOWED_ATTRIBUTION_FILE_PATH: ClassVar[str] = "ATTRIBUTION.md"
    # Docker image used to run ORT.
    ORT_IMAGE: ClassVar[str] = "ghcr.io/oss-review-toolkit/ort"
    # Python version available in the ORT container for python-inspector.
    ORT_PYTHON_VERSION: ClassVar[str] = "3.13"
    # Release-tool ORT configuration directory.
    ORT_CONFIG_DIRECTORY_PATH: ClassVar[Path] = RELEASE_TOOL_DIRECTORY_ANCHOR.directory / "ort"
    # ORT configuration file path inside the Docker container.
    ORT_CONFIG_CONTAINER_FILE_PATH: ClassVar[Path] = Path("/ort-config/config.yml")
    # Repository-relative requirements file used as the ORT analyzer input.
    ORT_REQUIREMENTS_FILE_PATH: ClassVar[Path] = Path("requirements-files/requirements.txt")
    # Repository-relative output directory for ORT intermediate files.
    ORT_OUTPUT_DIRECTORY: ClassVar[Path] = Path("git_ignored/ort-attribution/release-step-1")
    # ORT notice report path relative to ``ORT_OUTPUT_DIRECTORY``.
    ORT_NOTICE_REPORT_FILE_PATH: ClassVar[Path] = Path("reporter/NOTICE_DEFAULT")
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
                action=self._generate_changelog,
                validate=self._check_only_changelog_changed,
                commit_message=ReleaseStep1Runner.CHANGELOG_COMMIT_MESSAGE,
            ),
            ReleasePrCommitTask(
                action=self._generate_ort_attribution,
                validate=self._check_only_attribution_changed,
                commit_message=ReleaseStep1Runner.ATTRIBUTION_COMMIT_MESSAGE,
            ),
            package_version_update.as_release_pr_commit_task(
                commit_message=version_commit_message,
            ),
        )

    def _generate_changelog(self) -> None:
        """Generate changelog changes for the release."""
        self.release_helper.run_cli_command(command=("changie", "batch", self.version))
        self.release_helper.run_cli_command(command=("changie", "merge"))

    def _generate_ort_attribution(self) -> None:
        """Generate ORT attribution changes for the release."""
        self._prepare_ort_output_directory()
        self._run_ort_analyze()
        self._run_ort_scan()
        self._run_ort_report()
        self._copy_ort_report_to_attribution()

    def _check_only_changelog_changed(self) -> None:
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

    def _run_ort_command(self, command: Sequence[str]) -> None:
        """Run an ORT Docker command from the release repository root."""
        self.release_helper.run_cli_command(command=tuple(command))

    def _prepare_ort_output_directory(self) -> None:
        """Create the ignored ORT output directory before Docker bind-mounts it."""
        (self.release_helper.current_directory / ReleaseStep1Runner.ORT_OUTPUT_DIRECTORY).mkdir(
            parents=True, exist_ok=True
        )

    def _ort_docker_command(self, ort_args: Sequence[str]) -> tuple[str, ...]:
        """Return a Docker command that runs ORT with the shared release config."""
        project_mount = f"{self.release_helper.current_directory}:/project"
        output_mount = f"{self.release_helper.current_directory / ReleaseStep1Runner.ORT_OUTPUT_DIRECTORY}:/ort-out"
        config_mount = f"{ReleaseStep1Runner.ORT_CONFIG_DIRECTORY_PATH}:/ort-config:ro"
        return (
            "docker",
            "run",
            "--rm",
            "-e",
            f"PYENV_VERSION={ReleaseStep1Runner.ORT_PYTHON_VERSION}",
            "-v",
            project_mount,
            "-v",
            output_mount,
            "-v",
            config_mount,
            ReleaseStep1Runner.ORT_IMAGE,
            "--info",
            "-c",
            str(ReleaseStep1Runner.ORT_CONFIG_CONTAINER_FILE_PATH),
            *ort_args,
        )

    def _run_ort_analyze(self) -> None:
        """Run ORT analysis for the release requirements file."""
        self._run_ort_command(
            command=self._ort_docker_command(
                (
                    "analyze",
                    "-i",
                    f"/project/{ReleaseStep1Runner.ORT_REQUIREMENTS_FILE_PATH}",
                    "-o",
                    "/ort-out/analyzer",
                )
            )
        )

    def _run_ort_scan(self) -> None:
        """Run ORT package scanning from the analyzer result."""
        self._run_ort_command(
            command=self._ort_docker_command(
                (
                    "scan",
                    "-i",
                    "/ort-out/analyzer/analyzer-result.yml",
                    "--package-types",
                    "PACKAGE",
                    "-o",
                    "/ort-out/scanner",
                )
            )
        )

    def _run_ort_report(self) -> None:
        """Run ORT notice report generation from the scanner result."""
        self._run_ort_command(
            command=self._ort_docker_command(
                (
                    "report",
                    "--report-formats",
                    "PlainTextTemplate",
                    "-i",
                    "/ort-out/scanner/scan-result.yml",
                    "-o",
                    "/ort-out/reporter",
                    "-O",
                    "PlainTextTemplate=template.id=NOTICE_DEFAULT",
                )
            )
        )

    def _copy_ort_report_to_attribution(self) -> None:
        """Copy the generated ORT notice report to ATTRIBUTION.md."""
        attribution_path = self.release_helper.current_directory / ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH
        notice_report_path = (
            self.release_helper.current_directory
            / ReleaseStep1Runner.ORT_OUTPUT_DIRECTORY
            / ReleaseStep1Runner.ORT_NOTICE_REPORT_FILE_PATH
        )
        attribution_path.write_bytes(notice_report_path.read_bytes())

    def _check_only_attribution_changed(self) -> None:
        """Raise if ORT attribution changed files outside ATTRIBUTION.md."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [
            file_path
            for file_path in changed_file_paths
            if file_path != ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH
        ]
        if unexpected_file_paths:
            raise click.ClickException(
                f"ORT attribution may only change {ReleaseStep1Runner.ALLOWED_ATTRIBUTION_FILE_PATH}. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    def _release_step_1_pr_title(self) -> str:
        """Return the pull request title for release step 1."""
        return f"Release `metricflow` {self.version}"

    def _release_step_1_pr_body(self) -> str:
        """Return the pull request body for release step 1."""
        return f"Release `metricflow` {self.version} with updated changelog and attribution."
