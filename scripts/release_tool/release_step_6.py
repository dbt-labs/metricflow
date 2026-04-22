from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar

from msi_pydantic_shim import BaseModel
from scripts.release_tool.git_manager import GitManager
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_step_4 import ReleaseStep4State
from scripts.release_tool.release_step_5 import ReleaseStep5State

logger = logging.getLogger(__name__)


class ReleaseStep6State(BaseModel):
    """State captured during release step 6."""

    # Merge commit SHA for the step 4 PR.
    step_4_merge_commit_sha: str
    # Merge commit SHA for the step 5 PR.
    step_5_merge_commit_sha: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False


@dataclass(frozen=True)
class ReleaseStep6Runner:
    """Executes release-tool step 6.

    Step 6 merges the dbt-metricflow release pull requests from steps 4 and 5,
    then tags the dbt-metricflow release:

    * Polling until both the step-4 and step-5 PRs are ready to merge
    * Merging the step-4 PR
    * Switching to main, pulling, then recreating the step-5 branch from main
      and cherry-picking the step-5 commit onto it
    * Force-pushing the step-5 branch
    * Polling until the step-5 PR is ready to merge, then merging it
    * Creating or updating a lightweight ``dbt-metricflow/v$VERSION`` tag
      pointing to the step-4 merge commit and force-pushing it
    """

    # Seconds to wait between merge-readiness polls.
    PR_MERGE_POLL_SECONDS: ClassVar[float] = 60.0
    # Prefix for dbt-metricflow release version tags.
    TAG_PREFIX: ClassVar[str] = "dbt-metricflow/v"
    # State saved from step 4.
    step_4_state: ReleaseStep4State
    # State saved from step 5.
    step_5_state: ReleaseStep5State
    # GitHub client used to poll and merge pull requests.
    github_client: GitHubClient
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper
    # Function used to sleep between poll iterations.
    sleep: Callable[[float], None]

    def run(self) -> ReleaseStep6State:
        """Run step 6 and return state to save."""
        step_4_pr = self.step_4_state.pr_number
        step_5_pr = self.step_5_state.pr_number
        step_5_branch = self.step_5_state.branch_name
        step_5_commit_sha = self.step_5_state.commit_sha
        helper = self.release_helper
        git = helper.git_manager
        tag_name = f"{ReleaseStep6Runner.TAG_PREFIX}{self.step_4_state.dbt_metricflow_package_version}"

        self._wait_for_prs(step_4_pr=step_4_pr, step_5_pr=step_5_pr)

        step_4_merge_sha = self._merge_pr_if_needed(step_4_pr)
        step_5_already_merged = self.github_client.is_pr_merged(step_5_pr)

        if step_5_already_merged:
            helper.console.echo(f"PR #{step_5_pr} is already merged. Skipping step-5 branch refresh.")
        else:

            def _switch_to_main_and_pull() -> None:
                git.switch_branch(GitManager.MAIN_BRANCH)
                git.pull_current_branch()

            helper.run(
                description=f"Switch to {GitManager.MAIN_BRANCH} and pull",
                action=_switch_to_main_and_pull,
            )
            helper.run(
                description=f"Delete branch {step_5_branch}",
                action=lambda: git.delete_branch(step_5_branch),
            )

            def _create_branch_and_cherry_pick() -> None:
                git.create_branch(step_5_branch)
                git.switch_branch(step_5_branch)
                git.cherry_pick(step_5_commit_sha)

            helper.run(
                description=f"Create branch {step_5_branch} and cherry-pick {step_5_commit_sha}",
                action=_create_branch_and_cherry_pick,
            )
            helper.run_confirmed_remote_action(
                description=f"Force push branch {step_5_branch}",
                action=lambda: git.push_branch(step_5_branch, force=True),
            )

            self._wait_for_pr(step_5_pr)

        step_5_merge_sha = self._merge_pr_if_needed(step_5_pr)

        helper.run_confirmed_remote_action(
            description=f"Create and force push tag {tag_name} at {step_4_merge_sha}",
            action=lambda: git.push_tag(
                tag_name=tag_name,
                objectish=step_4_merge_sha,
                force=True,
            ),
        )
        return ReleaseStep6State(
            step_4_merge_commit_sha=step_4_merge_sha,
            step_5_merge_commit_sha=step_5_merge_sha,
        )

    def _merge_pr(self, pr_number: int) -> str:
        """Merge a pull request and echo the result."""
        merge_sha = self.github_client.merge_pr(
            pr_number=pr_number,
            merge_method=GitHubClient.SQUASH_MERGE_METHOD,
        )
        self.release_helper.console.echo(f"PR #{pr_number} merged with commit {merge_sha}")
        return merge_sha

    def _merge_pr_if_needed(self, pr_number: int) -> str:
        """Merge a pull request unless it has already been merged."""
        description = f"Merge PR #{pr_number}"
        if self.github_client.is_pr_merged(pr_number):
            merge_sha = self.github_client.get_pr_merge_commit_sha(pr_number)
            self.release_helper.console.echo(f"PR #{pr_number} is already merged with commit {merge_sha}")
            return merge_sha

        self.release_helper.confirm_state_changing_remote_action(description=description)
        self.release_helper.console.echo(description)
        return self._merge_pr(pr_number)

    def _wait_for_prs(self, step_4_pr: int, step_5_pr: int) -> None:
        """Wait for both step 4 and step 5 PRs to be merge-ready."""
        description = f"Wait for PR #{step_4_pr} and PR #{step_5_pr} to be ready to merge"
        self.release_helper.console.echo(f"{description}...")
        for pr_number, pr_link in (
            (step_4_pr, self.step_4_state.pr_link),
            (step_5_pr, self.step_5_state.pr_link),
        ):
            if self.github_client.is_pr_merged(pr_number):
                self.release_helper.console.echo(f"PR #{pr_number} is already merged.")
                continue
            self._wait_for_pr_merge_ready(pr_number=pr_number, pr_link=pr_link)

    def _wait_for_pr(self, pr_number: int) -> None:
        """Wait for a single PR to be merge-ready."""
        description = f"Wait for PR #{pr_number} to be ready to merge"
        if self.github_client.is_pr_merged(pr_number):
            self.release_helper.console.echo(f"PR #{pr_number} is already merged.")
            return

        self.release_helper.console.echo(f"{description}...")
        self._wait_for_pr_merge_ready(pr_number=pr_number, pr_link=self.step_5_state.pr_link)

    def _wait_for_pr_merge_ready(self, pr_number: int, pr_link: str) -> None:
        """Poll until the pull request is ready to merge."""
        while True:
            state = self.github_client.get_pr_mergeable_state(pr_number)
            if self.github_client.is_mergeable_state_ready(state):
                self.release_helper.console.echo(f"PR #{pr_number} is ready to merge.")
                return
            self.release_helper.console.echo(f"PR #{pr_number} is not ready to merge (state: {state}). Check {pr_link}")
            self.release_helper.console.echo(
                f"Waiting {int(ReleaseStep6Runner.PR_MERGE_POLL_SECONDS)}s before checking again..."
            )
            self.sleep(ReleaseStep6Runner.PR_MERGE_POLL_SECONDS)
