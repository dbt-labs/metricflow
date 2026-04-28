from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import click
import tomli as tomllib

from scripts.release_tool.release_helper import ReleaseHelper
from scripts.release_tool.release_pr_runner import ReleasePrCommitTask

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PackageVersionUpdate:
    """Updates and validates a hatch-managed package version."""

    # New package version.
    version: str
    # Shared release-step helper for common constants and operations.
    release_helper: ReleaseHelper
    # Directory where ``hatch version`` should run.
    hatch_project_directory: Path

    @property
    def about_file_path(self) -> str:
        """Return the repository-relative hatch version path from the package pyproject."""
        pyproject_path = self.hatch_project_directory / "pyproject.toml"
        try:
            with pyproject_path.open("rb") as pyproject_file:
                pyproject = tomllib.load(pyproject_file)
            hatch_version_path = cast(str, pyproject["tool"]["hatch"]["version"]["path"])
        except (FileNotFoundError, KeyError, tomllib.TOMLDecodeError) as exc:
            raise click.ClickException(f"Unable to read hatch version path from {pyproject_path}.") from exc

        return (
            self.hatch_project_directory.joinpath(hatch_version_path)
            .relative_to(self.release_helper.current_directory)
            .as_posix()
        )

    def run_hatch_version_update(self) -> None:
        """Run ``hatch version`` with hatch environment variables removed."""
        hatch_version_command = ("hatch", "version", self.version)
        description = f"Run {' '.join(hatch_version_command)} in {self.hatch_project_directory}"
        self.release_helper.console.echo(description)
        env = ReleaseHelper.environment_without_hatch_variables()
        self.release_helper.cli_command_runner.run(
            hatch_version_command,
            self.hatch_project_directory,
            env=env,
        )

    def check_only_about_file_changed(self) -> None:
        """Raise if changed files include anything outside the configured package about file."""
        changed_file_paths = self.release_helper.git_manager.changed_file_paths()
        unexpected_file_paths = [file_path for file_path in changed_file_paths if file_path != self.about_file_path]
        if unexpected_file_paths:
            raise click.ClickException(
                f"Running `hatch version` may only change {self.about_file_path}. "
                f"Unexpected changed paths: {', '.join(unexpected_file_paths)}"
            )

    def as_release_pr_commit_task(self, commit_message: str) -> ReleasePrCommitTask:
        """Return a release-PR commit task for updating this package version."""

        def run_version_update() -> None:
            self.run_hatch_version_update()
            self.check_only_about_file_changed()

        return ReleasePrCommitTask(
            action=run_version_update,
            commit_message=commit_message,
        )
