from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from msi_pydantic_shim import BaseModel
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.package_version import PackageVersionUpdate
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_pr_runner import ReleasePrRunner
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
    # Development version pull request number.
    pr_number: int
    # Development version pull request link.
    pr_link: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False


@dataclass(frozen=True)
class ReleaseStep5Runner:
    """Executes release-tool step 5.

    Step 5 prepares the follow-up development-version PR for `dbt-metricflow` by:

    * Creating the local step-5 branch from the step-4 branch
    * Updating the `dbt-metricflow` package version to the next dev version
    * Validating that only `dbt-metricflow/dbt_metricflow/__about__.py` changed
    * Committing and force-pushing the version update
    * Creating or updating a PR against the step-4 branch
    """

    # File path that the `dbt-metricflow` version update is allowed to modify.
    ALLOWED_DBT_ABOUT_FILE_PATH: ClassVar[str] = ReleaseStep4Runner.ALLOWED_DBT_ABOUT_FILE_PATH
    # Name of the package updated in step 5.
    PACKAGE_NAME: ClassVar[str] = "dbt-metricflow"
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
                    commit_message=pr_title,
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
            pr_number=pr_result.pr_number,
            pr_link=pr_result.pr_link,
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
