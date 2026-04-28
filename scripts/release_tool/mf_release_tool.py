from __future__ import annotations

import logging
import os
import shutil
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from itertools import cycle
from pathlib import Path
from typing import Any, cast

import click
from packaging.version import InvalidVersion, Version

from msi_pydantic_shim import BaseModel
from scripts.release_tool.cli_command_runner import CliCommandRunner, MetricFlowCliCommandRunner
from scripts.release_tool.git_manager import DulwichGitManager, GitManager
from scripts.release_tool.github_client import GitHubClient, PyGithubClient
from scripts.release_tool.release_helper import ReleaseHelper, ReleaseHelperConsole
from scripts.release_tool.release_step_1 import ReleaseStep1Runner, ReleaseStep1State

logger = logging.getLogger(__name__)

GITHUB_REPOSITORY_NAME = "dbt-labs/metricflow"
GITHUB_REPOSITORY_GIT_NAME = f"{GITHUB_REPOSITORY_NAME}.git"
GITHUB_REPOSITORY_HTML_BASE_URL = f"https://github.com/{GITHUB_REPOSITORY_NAME}"
REQUIRED_ENVIRONMENT_VARIABLES = ("GITHUB_USERNAME", "GITHUB_API_TOKEN", "FOSSA_API_KEY")
RELEASE_TOOL_STATE_FILE_PATH = Path("git_ignored/mf_release_tool_state.json")
CLI_COMMAND_STEP_1 = "step-1"

CLI_OPTION_YES = "--yes"
CLI_OPTION_YES_SHORT = "-y"
CLI_OPTION_METRICFLOW_REPO = "--metricflow-repo"
CLI_OPTION_VERSION = "--version"


class _ClickReleaseConsole(ReleaseHelperConsole):
    """Console adapter that uses the release tool's Click helpers."""

    @staticmethod
    def _flush_output() -> None:
        """Flush stdout and stderr to prevent interleaved output from subprocesses."""
        sys.stdout.flush()
        sys.stderr.flush()

    def echo(self, message: str) -> None:
        """Write a styled release-tool message to stdout and flush immediately."""
        click.secho(message, fg="green")
        self._flush_output()

    def confirm(self, message: str) -> None:
        """Prompt for confirmation and flush immediately."""
        self._flush_output()
        click.confirm(click.style(message, fg="white", bold=True), abort=True)
        self._flush_output()

    @contextmanager
    def spinner(self, message: str) -> Iterator[None]:
        """Show a spinner for a long-running subprocess command."""
        self._flush_output()
        if not sys.stdout.isatty():
            self.echo(message)
            yield
            return

        stop_event = threading.Event()

        def _spin() -> None:
            for frame in cycle("|/-\\"):
                sys.stdout.write(f"\r{frame} {message}")
                sys.stdout.flush()
                if stop_event.wait(0.1):
                    break
            clear_line = " " * (len(message) + 2)
            sys.stdout.write(f"\r{clear_line}\r")
            sys.stdout.flush()

        spinner_thread = threading.Thread(target=_spin, daemon=True)
        spinner_thread.start()
        try:
            yield
        finally:
            stop_event.set()
            spinner_thread.join()
            self._flush_output()


class ReleaseToolState(BaseModel):
    """State persisted between release-tool steps."""

    # State captured during step 1, populated after step 1 completes.
    step_1: ReleaseStep1State | None = None
    # Reserved for later steps; JSON includes these keys so snapshots match the full release-tool schema.
    step_2: Any | None = None
    step_3: Any | None = None
    step_4: Any | None = None
    step_5: Any | None = None
    step_6: Any | None = None
    step_7: Any | None = None

    class Config:
        """Pydantic configuration."""

        allow_mutation = False

    def with_step_state(self, updated_step_1: ReleaseStep1State | None = None) -> ReleaseToolState:
        """Return a copy with the specified step states replaced, preserving unspecified ones."""
        updates: dict[str, Any] = {}
        if updated_step_1 is not None:
            updates["step_1"] = updated_step_1
        return self.copy(update=updates)


@dataclass(frozen=True)
class ReleaseToolContext:
    """Runtime dependencies for the release tool."""

    # Environment variables used by release steps.
    environment: Mapping[str, str]
    # Directory where the command is running.
    current_directory: Path
    # Whether to skip confirmations for state-changing remote actions.
    confirm_all: bool
    # Factory for Git operations.
    git_manager_factory: Callable[[Path], GitManager]
    # Factory for GitHub operations.
    github_client_factory: Callable[[str, str], GitHubClient]
    # Function used to check whether a CLI command is available.
    is_cli_command_available: Callable[[str], bool]
    # CLI command runner used to execute repository commands.
    cli_command_runner: CliCommandRunner
    # Function used to sleep between poll iterations.
    sleep: Callable[[float], None]

    def copy(
        self,
        *,
        environment: Mapping[str, str] | None = None,
        current_directory: Path | None = None,
        confirm_all: bool | None = None,
        git_manager_factory: Callable[[Path], GitManager] | None = None,
        github_client_factory: Callable[[str, str], GitHubClient] | None = None,
        is_cli_command_available: Callable[[str], bool] | None = None,
        cli_command_runner: CliCommandRunner | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> ReleaseToolContext:
        """Return a copy with any provided fields replaced; pass ``None`` to leave a field unchanged."""
        return replace(
            self,
            environment=self.environment if environment is None else environment,
            current_directory=self.current_directory if current_directory is None else current_directory,
            confirm_all=self.confirm_all if confirm_all is None else confirm_all,
            git_manager_factory=self.git_manager_factory if git_manager_factory is None else git_manager_factory,
            github_client_factory=(
                self.github_client_factory if github_client_factory is None else github_client_factory
            ),
            is_cli_command_available=(
                self.is_cli_command_available if is_cli_command_available is None else is_cli_command_available
            ),
            cli_command_runner=self.cli_command_runner if cli_command_runner is None else cli_command_runner,
            sleep=self.sleep if sleep is None else sleep,
        )


def _is_cli_command_available(command_name: str) -> bool:
    """Return true if the CLI command is available on PATH."""
    return shutil.which(command_name) is not None


def _github_client_factory(access_token: str, repository_name: str) -> GitHubClient:
    """Return a GitHub client."""
    return PyGithubClient(access_token=access_token, repository_name=repository_name)


def _default_release_tool_context(current_directory: Path) -> ReleaseToolContext:
    """Return release-tool dependencies for normal CLI execution."""
    return ReleaseToolContext(
        environment=os.environ,
        current_directory=current_directory,
        confirm_all=False,
        git_manager_factory=DulwichGitManager,
        github_client_factory=_github_client_factory,
        is_cli_command_available=_is_cli_command_available,
        cli_command_runner=MetricFlowCliCommandRunner(),
        sleep=time.sleep,
    )


def _release_tool_context(ctx: click.Context) -> ReleaseToolContext:
    """Return the configured release-tool context."""
    if ctx.obj is None:
        raise click.ClickException("MetricFlow repository directory was not configured.")

    return cast(ReleaseToolContext, ctx.obj)


def _validate_semantic_version(ctx: click.Context, param: click.Parameter, value: str) -> str:
    """Validate a semantic-version argument."""
    try:
        parsed_version = Version(value)
    except InvalidVersion as exc:
        raise click.BadParameter("must be a semantic version such as 1.2.3") from exc

    if len(parsed_version.release) != 3:
        raise click.BadParameter("must be a semantic version such as 1.2.3")

    return str(parsed_version)


def _check_required_environment_variables(environment: Mapping[str, str]) -> None:
    """Raise if required release environment variables are missing."""
    missing_environment_variables = [
        environment_variable
        for environment_variable in REQUIRED_ENVIRONMENT_VARIABLES
        if not environment.get(environment_variable)
    ]
    if missing_environment_variables:
        raise click.ClickException(
            f"Missing required environment variables: {', '.join(missing_environment_variables)}"
        )


def _check_metricflow_repo(git_manager: GitManager) -> None:
    """Raise if the current directory is not a clean metricflow git repository."""
    if not git_manager.is_git_repo():
        raise click.ClickException("MetricFlow repository directory must be a Git repository.")

    if not git_manager.has_remote_repository(GITHUB_REPOSITORY_GIT_NAME):
        raise click.ClickException(
            f"MetricFlow repository directory must represent the {GITHUB_REPOSITORY_GIT_NAME} remote repository."
        )

    if not git_manager.is_state_clean():
        raise click.ClickException("The metricflow git repository must be clean before running a release step.")


def _check_required_cli_commands(
    command_names: tuple[str, ...], is_cli_command_available: Callable[[str], bool]
) -> None:
    """Raise if required CLI commands are not available."""
    missing_command_names = [
        command_name for command_name in command_names if not is_cli_command_available(command_name)
    ]
    if missing_command_names:
        raise click.ClickException(f"Missing required CLI commands: {', '.join(missing_command_names)}")


def _release_tool_state_file_path(current_directory: Path) -> Path:
    """Return the path for release-tool state."""
    return current_directory / RELEASE_TOOL_STATE_FILE_PATH


def _save_release_tool_state(state_file_path: Path, state: ReleaseToolState, console: ReleaseHelperConsole) -> None:
    """Write pretty-formatted release-tool state JSON and display its contents."""
    state_file_path.parent.mkdir(parents=True, exist_ok=True)
    state_json = f"{state.json(indent=4)}\n"
    state_file_path.write_text(state_json)
    console.echo(f"Save release state to {state_file_path}")
    console.echo(state_json)


def _load_release_tool_state(state_file_path: Path) -> ReleaseToolState:
    """Load and return the release-tool state, raising if the file is missing."""
    if not state_file_path.exists():
        raise click.ClickException(f"Release tool state file not found at {state_file_path}.")
    return ReleaseToolState.parse_raw(state_file_path.read_text())


def _run_release_step_prechecks(context: ReleaseToolContext) -> GitManager:
    """Run common release-step prechecks and return a Git manager."""
    _check_required_environment_variables(environment=context.environment)
    git_manager = context.git_manager_factory(context.current_directory)
    _check_metricflow_repo(git_manager=git_manager)
    return git_manager


@click.group()
@click.option(
    CLI_OPTION_YES,
    CLI_OPTION_YES_SHORT,
    "confirm_all",
    is_flag=True,
    help="Answer yes to all confirmations.",
)
@click.pass_context
def cli(ctx: click.Context, confirm_all: bool) -> None:
    """Run MetricFlow release steps."""
    if ctx.obj is None:
        ctx.obj = _default_release_tool_context(current_directory=Path.cwd()).copy(confirm_all=confirm_all)
        return

    release_tool_context = cast(ReleaseToolContext, ctx.obj)
    ctx.obj = release_tool_context.copy(confirm_all=confirm_all)


@cli.command(CLI_COMMAND_STEP_1)
@click.option(
    CLI_OPTION_METRICFLOW_REPO,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help="Path to the metricflow git repository.",
)
@click.option(
    CLI_OPTION_VERSION, required=True, callback=_validate_semantic_version, help="Semantic version for the release."
)
@click.pass_context
def step_1(ctx: click.Context, metricflow_repo: Path, version: str) -> None:
    """Prepare the first release pull-request branch."""
    console = _ClickReleaseConsole()
    console.echo(f"MetricFlow repo directory: {metricflow_repo}")
    context = _release_tool_context(ctx).copy(current_directory=metricflow_repo)
    git_manager = _run_release_step_prechecks(context=context)
    _check_required_cli_commands(
        command_names=ReleaseStep1Runner.REQUIRED_CLI_COMMANDS,
        is_cli_command_available=context.is_cli_command_available,
    )
    state_file_path = _release_tool_state_file_path(current_directory=context.current_directory)
    existing_release_tool_state = _load_release_tool_state(state_file_path) if state_file_path.exists() else None
    existing_step_1_state = existing_release_tool_state.step_1 if existing_release_tool_state is not None else None
    github_client = context.github_client_factory(context.environment["GITHUB_API_TOKEN"], GITHUB_REPOSITORY_NAME)

    release_helper = ReleaseHelper(
        repository_name=GITHUB_REPOSITORY_NAME,
        current_directory=context.current_directory,
        confirm_all=context.confirm_all,
        git_manager=git_manager,
        cli_command_runner=context.cli_command_runner,
        console=console,
    )
    step_1_runner = ReleaseStep1Runner(
        version=version,
        environment=context.environment,
        github_client=github_client,
        existing_state=existing_step_1_state,
        release_helper=release_helper,
    )
    step_1_state = step_1_runner.run()

    base_state = existing_release_tool_state if existing_release_tool_state is not None else ReleaseToolState()
    release_tool_state = base_state.with_step_state(updated_step_1=step_1_state)
    _save_release_tool_state(state_file_path=state_file_path, state=release_tool_state, console=console)


if __name__ == "__main__":
    cli()
