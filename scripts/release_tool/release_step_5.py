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
from scripts.release_tool.release_step_2 import ReleaseStep2Runner
from scripts.release_tool.release_step_4 import ReleaseStep4Runner, ReleaseStep4State

logger = logging.getLogger(__name__)


class ReleaseStep5State(BaseModel):
    """State captured during release step 5."""

    # `metricflow` version pinned in `dbt-metricflow` for the release.
    metricflow_package_version: str
    # Next development version for the `dbt-metricflow` package.
    dbt_metricflow_package_version: str
    # Development version branch name.
    branch_name: str
    # Commit SHA for the version update.
    commit_sha: str
    # Commit SHA for setting dbt-metricflow's metricflow dependency back to the local monorepo package.
    local_metricflow_requirement_commit_sha: str
    # Development version pull request number.
    pr_number: int
    # Development version pull request link.
    pr_link: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False

    def commit_shas_for_branch_refresh(self) -> tuple[str, ...]:
        """Return the step-5 commit SHAs that should be replayed when refreshing the branch."""
        return (self.commit_sha, self.local_metricflow_requirement_commit_sha)


@dataclass(frozen=True)
class ReleaseStep5Runner:
    """Executes release-tool step 5.

    Step 5 prepares the follow-up development-version PR for `dbt-metricflow` by:

    * Creating the local step-5 branch from the step-4 branch
    * Updating the `dbt-metricflow` package version to the next dev version
    * Validating that only `dbt-metricflow/dbt_metricflow/__about__.py` changed
    * Committing the version update
    * Setting the `dbt-metricflow` package's `metricflow` requirement to use
      the local monorepo package
    * Committing the requirements update and force-pushing the branch
    * Creating or updating a PR against the step-4 branch
    """

    # File path that the `dbt-metricflow` version update is allowed to modify.
    ALLOWED_DBT_ABOUT_FILE_PATH: ClassVar[str] = ReleaseStep4Runner.ALLOWED_DBT_ABOUT_FILE_PATH
    # Path to the dbt-metricflow requirements file that depends on the `metricflow` package.
    REQUIREMENTS_FILE_PATH: ClassVar[str] = ReleaseStep4Runner.REQUIREMENTS_FILE_PATH
    # Name of the package updated in step 5.
    PACKAGE_NAME: ClassVar[str] = "dbt-metricflow"
    # Requirement line used by dbt-metricflow development branches to depend on the local monorepo metricflow package.
    LOCAL_METRICFLOW_REQUIREMENT_LINE: ClassVar[str] = "metricflow @ {root:parent:uri}"
    # Commit message used when setting the local metricflow requirement.
    LOCAL_METRICFLOW_REQUIREMENT_COMMIT_MESSAGE: ClassVar[str] = "Set local `metricflow` requirement"
    # CLI commands that must be available on PATH for step 5.
    REQUIRED_CLI_COMMANDS: ClassVar[tuple[str, ...]] = ("hatch",)
    # State saved from step 4.
    step_4_state: ReleaseStep4State
    # Environment variables used by the release step.
    environment: Mapping[str, str]
    # GitHub client used to create the development-version pull request.
    github_client: GitHubClient
    # Existing state saved from a prior step-5 run.
    existing_state: ReleaseStep5State | None
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper

    @staticmethod
    def next_dev_version(version: str) -> str:
        """Return the next development version for a released package version."""
        return ReleaseStep2Runner.next_dev_version(version)

    def run(self) -> ReleaseStep5State:
        """Run step 5 and return state to save."""
        mf_version = self.step_4_state.metricflow_package_version
        step_4_branch_name = self.step_4_state.branch_name
        new_version = ReleaseStep5Runner.next_dev_version(self.step_4_state.dbt_metricflow_package_version)
        step_5_branch_name = f"{self.environment['GITHUB_USERNAME']}/release_pr/{mf_version}/step_5"

        package_version_update = PackageVersionUpdate(
            version=new_version,
            release_helper=self.release_helper,
            hatch_project_directory=self._dbt_metricflow_root(),
        )
        pr_title = self._development_version_pr_title(version=new_version)
        pr_result = ReleasePrRunner(
            release_branch_name=step_5_branch_name,
            pr_title=pr_title,
            pr_body=self._development_version_pr_body(version=new_version),
            existing_pr_number=self.existing_state.pr_number if self.existing_state is not None else None,
            tasks=(
                package_version_update.as_release_pr_commit_task(
                    commit_message=self._development_version_pr_title(version=new_version),
                ),
                ReleasePrCommitTask(
                    action=self._set_local_metricflow_requirement,
                    validate=self._check_requirements_only_changes,
                    commit_message=ReleaseStep5Runner.LOCAL_METRICFLOW_REQUIREMENT_COMMIT_MESSAGE,
                ),
            ),
            github_client=self.github_client,
            release_helper=self.release_helper,
            base_branch=step_4_branch_name,
            pull_base_branch=False,
        ).run()
        return ReleaseStep5State(
            metricflow_package_version=mf_version,
            dbt_metricflow_package_version=new_version,
            branch_name=step_5_branch_name,
            commit_sha=pr_result.commit_shas[0],
            local_metricflow_requirement_commit_sha=pr_result.commit_shas[1],
            pr_number=pr_result.pr_number,
            pr_link=pr_result.pr_link,
        )

    def _set_local_metricflow_requirement(self) -> None:
        """Set the local metricflow requirement for post-release dbt-metricflow development."""
        self.release_helper.run(
            description=(
                f"Update {ReleaseStep5Runner.REQUIREMENTS_FILE_PATH} to use "
                f"`{ReleaseStep5Runner.LOCAL_METRICFLOW_REQUIREMENT_LINE}`"
            ),
            action=lambda: self._write_requirements_file(
                requirement_line=ReleaseStep5Runner.LOCAL_METRICFLOW_REQUIREMENT_LINE
            ),
        )

    def _write_requirements_file(self, requirement_line: str) -> None:
        """Write the `metricflow` requirement to the dbt-metricflow requirements file."""
        requirements_path = self.release_helper.current_directory / ReleaseStep5Runner.REQUIREMENTS_FILE_PATH
        requirements_path.write_text(f"{requirement_line}\n")

    def _check_requirements_only_changes(self) -> None:
        """Raise if setting the local requirement changed files outside the metricflow requirement file."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [
            file_path for file_path in changed_file_paths if file_path != ReleaseStep5Runner.REQUIREMENTS_FILE_PATH
        ]
        if unexpected_file_paths:
            raise click.ClickException(
                f"Setting the local requirement may only change {ReleaseStep5Runner.REQUIREMENTS_FILE_PATH}. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    def _dbt_metricflow_root(self) -> Path:
        """Return the dbt-metricflow hatch project directory."""
        return self.release_helper.current_directory / ReleaseStep4Runner.DBT_METRICFLOW_SUBDIRECTORY

    def _development_version_pr_title(self, version: str) -> str:
        """Return the development-version PR title."""
        return f"Update `{ReleaseStep5Runner.PACKAGE_NAME}` version to {version}"

    def _development_version_pr_body(self, version: str) -> str:
        """Return the development-version PR body."""
        return f"Update `{ReleaseStep5Runner.PACKAGE_NAME}` version to {version} to reflect in-progress development."
