from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar

from msi_pydantic_shim import BaseModel
from scripts.release_tool.git_manager import GitManager
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_step_1 import ReleaseStep1State
from scripts.release_tool.release_step_2 import ReleaseStep2State

logger = logging.getLogger(__name__)


class ReleaseStep3State(BaseModel):
    """State captured during release step 3."""

    # Merge commit SHA for the step 1 PR.
    step_1_merge_commit_sha: str
    # Merge commit SHA for the step 2 PR.
    step_2_merge_commit_sha: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False


@dataclass(frozen=True)
class ReleaseStep3Runner:
    """Executes release-tool step 3.

    Step 3 merges the release pull requests from steps 1 and 2, then
    tags the release and triggers the publish workflow:

    * Polling until both the step-1 and step-2 PRs are ready to merge
    * Merging the step-1 PR
    * Switching to main, pulling, then recreating the step-2 branch from
      main and cherry-picking the step-2 commit onto it
    * Force-pushing the step-2 branch
    * Polling until the step-2 PR is ready to merge, then merging it
    * Creating or updating a lightweight ``v$VERSION`` tag pointing to
      the step-1 merge commit and force-pushing it
    * Triggering the publish workflow using the new tag
    """

    # Seconds to wait between merge-readiness polls.
    PR_MERGE_POLL_SECONDS: ClassVar[float] = 60.0
    # Prefix for release version tags.
    TAG_PREFIX: ClassVar[str] = "v"
    # State saved from step 1.
    step_1_state: ReleaseStep1State
    # State saved from step 2.
    step_2_state: ReleaseStep2State
    # GitHub client used to poll and merge pull requests.
    github_client: GitHubClient
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper
    # Function used to sleep between poll iterations.
    sleep: Callable[[float], None]

    def run(self) -> ReleaseStep3State:
        """Run step 3 and return state to save."""
        step_1_pr = self.step_1_state.pr_number
        step_2_pr = self.step_2_state.pr_number
        step_2_branch = self.step_2_state.branch_name
        step_2_commit_sha = self.step_2_state.commit_sha
        helper = self.release_helper
        git = helper.git_manager
        tag_name = f"{ReleaseStep3Runner.TAG_PREFIX}{self.step_1_state.metricflow_package_version}"

        self._wait_for_prs(step_1_pr=step_1_pr, step_2_pr=step_2_pr)

        step_1_merge_sha = self._merge_pr_if_needed(step_1_pr)
        step_2_already_merged = self.github_client.is_pr_merged(step_2_pr)

        if step_2_already_merged:
            helper.console.echo(f"PR #{step_2_pr} is already merged. Skipping step-2 branch refresh.")
        else:

            def _switch_to_main_and_pull() -> None:
                git.switch_branch(GitManager.MAIN_BRANCH)
                git.pull_current_branch()

            helper.run(
                description=f"Switch to {GitManager.MAIN_BRANCH} and pull",
                action=_switch_to_main_and_pull,
            )
            helper.run(
                description=f"Delete branch {step_2_branch}",
                action=lambda: git.delete_branch(step_2_branch),
            )

            def _create_branch_and_cherry_pick() -> None:
                git.create_branch(step_2_branch)
                git.switch_branch(step_2_branch)
                git.cherry_pick(step_2_commit_sha)

            helper.run(
                description=f"Create branch {step_2_branch} and cherry-pick {step_2_commit_sha}",
                action=_create_branch_and_cherry_pick,
            )
            helper.run_confirmed_remote_action(
                description=f"Force push branch {step_2_branch}",
                action=lambda: git.push_branch(step_2_branch, force=True),
            )

            self._wait_for_pr(step_2_pr)

        step_2_merge_sha = self._merge_pr_if_needed(step_2_pr)

        helper.run_confirmed_remote_action(
            description=f"Create and force push tag {tag_name} at {step_1_merge_sha}",
            action=lambda: git.push_tag(
                tag_name=tag_name,
                objectish=step_1_merge_sha,
                force=True,
            ),
        )
        return ReleaseStep3State(
            step_1_merge_commit_sha=step_1_merge_sha,
            step_2_merge_commit_sha=step_2_merge_sha,
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

    def _wait_for_prs(self, step_1_pr: int, step_2_pr: int) -> None:
        """Wait for both step 1 and step 2 PRs to be merge-ready."""
        description = f"Wait for PR #{step_1_pr} and PR #{step_2_pr} to be ready to merge"
        self.release_helper.console.echo(f"{description}...")
        for pr_number, pr_link in (
            (step_1_pr, self.step_1_state.pr_link),
            (step_2_pr, self.step_2_state.pr_link),
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
        self._wait_for_pr_merge_ready(pr_number=pr_number, pr_link=self.step_2_state.pr_link)

    def _wait_for_pr_merge_ready(self, pr_number: int, pr_link: str) -> None:
        """Poll until the pull request is ready to merge."""
        while True:
            state = self.github_client.get_pr_mergeable_state(pr_number)
            if self.github_client.is_mergeable_state_ready(state):
                self.release_helper.console.echo(f"PR #{pr_number} is ready to merge.")
                return
            self.release_helper.console.echo(f"PR #{pr_number} is not ready to merge (state: {state}). Check {pr_link}")
            self.release_helper.console.echo(
                f"Waiting {int(ReleaseStep3Runner.PR_MERGE_POLL_SECONDS)}s before checking again..."
            )
            self.sleep(ReleaseStep3Runner.PR_MERGE_POLL_SECONDS)
