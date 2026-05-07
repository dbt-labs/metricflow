from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from packaging.version import Version

from msi_pydantic_shim import BaseModel
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.package_version import PackageVersionUpdate
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_pr_runner import ReleasePrRunner
from scripts.release_tool.release_step_1 import ReleaseStep1State

logger = logging.getLogger(__name__)


class ReleaseStep2State(BaseModel):
    """State captured during release step 2."""

    # Next development version for MetricFlow.
    metricflow_package_version: str
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
class ReleaseStep2Runner:
    """Executes release-tool step 2.

    Step 2 prepares the follow-up development-version PR by:

    * Creating the local step-2 branch from the step-1 branch
    * Updating the MetricFlow package version to the next dev version
    * Validating that only `metricflow/__about__.py` changed
    * Committing and force-pushing the version update
    * Creating or updating a PR against the step-1 branch
    """

    # File path that the version update is allowed to modify.
    ALLOWED_ABOUT_FILE_PATH: ClassVar[str] = "metricflow/__about__.py"
    # Name of the package updated in step 2.
    PACKAGE_NAME: ClassVar[str] = "metricflow"
    # State saved from step 1.
    step_1_state: ReleaseStep1State
    # Environment variables used by the release step.
    environment: Mapping[str, str]
    # GitHub client used to create the release pull request.
    github_client: GitHubClient
    # Existing state saved from a prior step-2 run.
    existing_state: ReleaseStep2State | None
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper

    @staticmethod
    def next_dev_version(version: str) -> str:
        """Return the next development version by incrementing the minor component and adding `.dev0`.

        Given ``1.2.3``, returns ``1.3.0.dev0``.
        """
        parsed_version = Version(version)
        major, minor, _patch = parsed_version.release
        return str(Version(f"{major}.{minor + 1}.0.dev0"))

    def run(self) -> ReleaseStep2State:
        """Run step 2 and return state to save."""
        step_1_version = self.step_1_state.metricflow_package_version
        step_1_branch_name = self.step_1_state.branch_name
        new_version = ReleaseStep2Runner.next_dev_version(step_1_version)
        step_2_branch_name = f"{self.environment['GITHUB_USERNAME']}/release_pr/{step_1_version}/step_2"

        package_version_update = PackageVersionUpdate(
            version=new_version,
            release_helper=self.release_helper,
            hatch_project_directory=self.release_helper.current_directory,
        )
        pr_title = self._development_version_pr_title(version=new_version)
        pr_result = ReleasePrRunner(
            release_branch_name=step_2_branch_name,
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
            base_branch=step_1_branch_name,
            pull_base_branch=False,
        ).run()
        self.release_helper.echo_pull_request_review_banner(pr_link=pr_result.pr_link)
        return ReleaseStep2State(
            metricflow_package_version=new_version,
            branch_name=step_2_branch_name,
            commit_sha=pr_result.commit_shas[0],
            pr_number=pr_result.pr_number,
            pr_link=pr_result.pr_link,
        )

    def _development_version_pr_title(self, version: str) -> str:
        """Return the development-version PR title."""
        return f"Update `{ReleaseStep2Runner.PACKAGE_NAME}` version to {version}"

    def _development_version_pr_body(self, version: str) -> str:
        """Return the development-version PR body."""
        return f"Update `{ReleaseStep2Runner.PACKAGE_NAME}` version to {version} to reflect in-progress development."
