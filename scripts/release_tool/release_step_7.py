from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import click
from metricflow_semantics.test_helpers.terminal_helpers import mf_path_hyperlink
from metricflow_semantics.toolkit.string_helpers import mf_indent

from msi_pydantic_shim import BaseModel
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_step_1 import ReleaseStep1State
from scripts.release_tool.release_step_3 import ReleaseStep3State

logger = logging.getLogger(__name__)


class ReleaseStep7State(BaseModel):
    """State captured during release step 7."""

    # Tag used for the GitHub release note.
    metricflow_release_tag_name: str
    # Repository-relative changelog file path used as the release-note body.
    change_log_file_path: str
    # GitHub release note link.
    release_note_link: str

    class Config:
        """Pydantic configuration."""

        allow_mutation = False


@dataclass(frozen=True)
class ReleaseStep7Runner:
    """Executes release-tool step 7.

    Step 7 creates the GitHub release note for the MetricFlow package release by:

    * Reading the version-specific changelog file from `.changes/`
    * Creating a GitHub release for the tag created in step 3
    * Using the changelog contents as the release-note body
    """

    # Directory containing changie-generated release changelog files.
    CHANGELOG_DIRECTORY: ClassVar[str] = ".changes"
    # State saved from step 1.
    step_1_state: ReleaseStep1State
    # State saved from step 3.
    step_3_state: ReleaseStep3State
    # GitHub client used to create the release note.
    github_client: GitHubClient
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper

    def run(self) -> ReleaseStep7State:
        """Run step 7 and return state to save."""
        tag_name = self._tag_name()
        version = self._version_from_tag(tag_name=tag_name)
        change_log_file_path = Path(ReleaseStep7Runner.CHANGELOG_DIRECTORY) / f"{version}.md"
        change_log_body = self._read_change_log_body(change_log_file_path=change_log_file_path)
        release_note_body = self._review_release_note_body(tag_name=tag_name, body=change_log_body)
        release_title = f"MetricFlow {version}"
        release_note_link = self._create_release_note(
            tag_name=tag_name,
            title=release_title,
            body=release_note_body,
        )
        return ReleaseStep7State(
            metricflow_release_tag_name=tag_name,
            change_log_file_path=change_log_file_path.as_posix(),
            release_note_link=release_note_link,
        )

    def _tag_name(self) -> str:
        """Return the step-3 tag, deriving it for old state files that predate persisted tags."""
        if self.step_3_state.metricflow_release_tag_name is not None:
            return self.step_3_state.metricflow_release_tag_name
        return f"{ReleaseHelper.METRICFLOW_RELEASE_TAG_PREFIX}{self.step_1_state.metricflow_package_version}"

    def _version_from_tag(self, tag_name: str) -> str:
        """Return the version suffix from a step-3 tag."""
        tag_prefix = ReleaseHelper.METRICFLOW_RELEASE_TAG_PREFIX
        if not tag_name.startswith(tag_prefix):
            raise click.ClickException(f"Step 3 tag must start with `{tag_prefix}`. Found: {tag_name}")
        return tag_name.removeprefix(tag_prefix)

    def _read_change_log_body(self, change_log_file_path: Path) -> str:
        """Return the changelog body for the released version."""
        full_change_log_file_path = self.release_helper.current_directory / change_log_file_path
        if not full_change_log_file_path.exists():
            raise click.ClickException(f"Release changelog file not found at {full_change_log_file_path}.")
        return full_change_log_file_path.read_text()

    def _review_release_note_body(self, tag_name: str, body: str) -> str:
        """Write release-note contents to a temp file and return the reviewed contents."""
        # Close the file before review so editors do not modify a file handle this process is still holding.
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix="metricflow-release-note-",
            suffix=f"-{tag_name}.md",
            delete=False,
        ) as release_note_file:
            release_note_file.write(body)
            release_note_file_path = Path(release_note_file.name)

        if not self.release_helper.confirm_all:
            self.release_helper.console.confirm(
                f"Review and edit the release note file at:"
                f"\n\n{mf_indent(mf_path_hyperlink(release_note_file_path))}"
                f"\n\nContinue when ready?"
            )

        try:
            return release_note_file_path.read_text()
        finally:
            release_note_file_path.unlink(missing_ok=True)

    def _create_release_note(self, tag_name: str, title: str, body: str) -> str:
        """Create the GitHub release note and return its link."""
        description = f"Create release note for tag {tag_name}"
        release_note_link = self.release_helper.run_confirmed_remote_action(
            description=description,
            action=lambda: self.github_client.create_release_note(
                tag_name=tag_name,
                title=title,
                body=body,
            ),
        )
        self.release_helper.console.echo(f"Release note link: {release_note_link}")
        return release_note_link
