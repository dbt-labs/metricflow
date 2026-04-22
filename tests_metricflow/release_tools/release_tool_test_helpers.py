from __future__ import annotations

import logging
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias

from scripts.release_tool.cli_command_runner import CliCommandResult, CliCommandRunner
from scripts.release_tool.git_manager import GitManager
from scripts.release_tool.github_client import GitHubClient
from scripts.release_tool.mf_release_tool import ReleaseToolContext

logger = logging.getLogger(__name__)


_REQUIRED_ENVIRONMENT = {
    "GITHUB_USERNAME": "metricflow-user",
    "GITHUB_API_TOKEN": "github-token",
    "FOSSA_API_KEY": "fossa-token",
}
_STATE_FILE_PATH = Path("git_ignored/mf_release_tool_state.json")


_STEP_4_REQUIREMENTS_FILE_PATH = "dbt-metricflow/requirements-files/requirements-metricflow.txt"
_STEP_4_DBT_ABOUT_FILE_PATH = "dbt-metricflow/dbt_metricflow/__about__.py"
# Semantic version passed to `step-4 --version` in tests.
_TEST_DBT_METRICFLOW_RELEASE_VERSION = "0.12.0"


@dataclass(frozen=True)
class FakeSnapshotEntry:
    """Base class for DTOs in ``FakeRunSnapshot.operations`` (``mf_pformat`` snapshot output)."""


@dataclass(frozen=True)
class FakeGitOperation(FakeSnapshotEntry):
    """Git operation recorded by the fake manager."""

    # Name of the Git operation.
    name: str
    # Branch used by the Git operation.
    branch_name: str | None = None
    # Commit message used by the Git operation.
    message: str | None = None
    # Commit SHA used by the Git operation.
    commit_sha: str | None = None
    # File path used by the Git operation.
    file_path: str | None = None


@dataclass(frozen=True)
class CreatedPullRequest:
    """Pull request recorded by the fake GitHub client."""

    # Pull request title.
    title: str
    # Pull request body.
    body: str | None
    # Branch containing proposed changes.
    head_branch: str
    # Branch receiving proposed changes.
    base_branch: str
    # Whether the pull request was created as a draft.
    draft: bool
    # Labels applied to the pull request.
    labels: tuple[str, ...]


@dataclass(frozen=True)
class FakeGitManagerFactoryPath:
    """``git_manager_factory`` returned a manager for a repository path."""

    repository_path: Path


@dataclass(frozen=True)
class FakeCli:
    """A ``FakeCliCommandRunner.run`` call."""

    command: tuple[str, ...]
    current_directory: Path


@dataclass(frozen=True)
class FakeGitHubTokens(FakeSnapshotEntry):
    """A ``github_client_factory`` call (cumulative state after the call)."""

    access_tokens: tuple[str, ...]
    repository_names: tuple[str, ...]


@dataclass(frozen=True)
class FakeGitHubPullRequest(FakeSnapshotEntry):
    """``create_pr`` or ``create_or_update_pr`` recorded a pull request payload."""

    source: Literal["create_pr", "create_or_update_create", "create_or_update_update"]
    pull_request: CreatedPullRequest


@dataclass(frozen=True)
class FakeGitHubMergeableState(FakeSnapshotEntry):
    """``get_pr_mergeable_state`` on the fake GitHub client."""

    pr_number: int


@dataclass(frozen=True)
class FakeGitHubIsMerged(FakeSnapshotEntry):
    """``is_pr_merged`` on the fake GitHub client."""

    pr_number: int
    result: bool


@dataclass(frozen=True)
class FakeGitHubMergeSha(FakeSnapshotEntry):
    """``get_pr_merge_commit_sha`` on the fake GitHub client."""

    pr_number: int
    result: str


@dataclass(frozen=True)
class FakeGitHubMerge(FakeSnapshotEntry):
    """``merge_pr`` on the fake GitHub client (success)."""

    pr_number: int
    merge_method: str | None


@dataclass(frozen=True)
class FakeGitHubMergeError(FakeSnapshotEntry):
    """``merge_pr`` when the PR is already merged."""

    pr_number: int


@dataclass(frozen=True)
class FakeGitHubRunWorkflow:
    """``run_workflow`` on the fake GitHub client."""

    workflow_id_or_file_name: str | int
    ref: str
    inputs: Mapping[str, str] | None


@dataclass(frozen=True)
class FakeReleaseStateFile:
    """``mf_release_tool_state.json`` path and contents, appended in tests when recording final state."""

    file_path: Path
    file_text: str


FakeEntry: TypeAlias = (
    FakeGitManagerFactoryPath
    | FakeGitOperation
    | FakeCli
    | FakeGitHubTokens
    | FakeGitHubPullRequest
    | FakeGitHubMergeableState
    | FakeGitHubIsMerged
    | FakeGitHubMergeSha
    | FakeGitHubMerge
    | FakeGitHubMergeError
    | FakeGitHubRunWorkflow
    | FakeReleaseStateFile
)


# Snapshot DTO: paths in ``<TMP>`` form for stable ``mf_pformat`` output across runs.
@dataclass(frozen=True)
class FakeGitPath(FakeSnapshotEntry):
    """Normalized ``repository_path`` (``<TMP>``) for a ``FakeGitManagerFactoryPath`` entry."""

    repository_path: str


@dataclass(frozen=True)
class FakeCliCommand(FakeSnapshotEntry):
    """Single CLI subprocess line with normalized ``current_directory`` (``<TMP>``)."""

    command: tuple[str, ...]
    current_directory: str


@dataclass(frozen=True)
class FakeGitHubWorkflow(FakeSnapshotEntry):
    """Workflow dispatch in snapshot form (sorted string inputs for stable diffs)."""

    workflow_id_or_file_name: str | int
    ref: str
    inputs: tuple[tuple[str, str], ...] | None


@dataclass(frozen=True)
class FakeReleaseStateFileSnapshot(FakeSnapshotEntry):
    """``mf_release_tool_state.json`` with normalized path (``<TMP>``) for ``mf_pformat``."""

    file_path: str
    file_text: str


@dataclass(frozen=True)
class FakeRunSnapshot:
    """``exit_code`` and normalized ``operations`` for `assert_object_snapshot_equal`."""

    exit_code: int
    operations: tuple[FakeSnapshotEntry, ...]


def _tmp_replacement_path(path: Path, tmp_path: Path) -> str:
    return str(path).replace(str(tmp_path), "<TMP>")


def _normalize_workflow_inputs(inputs: Mapping[str, str] | None) -> tuple[tuple[str, str], ...] | None:
    if inputs is None:
        return None
    return tuple(sorted((key, str(value)) for key, value in inputs.items()))


def _entry_to_snapshot(entry: FakeEntry, tmp_path: Path) -> FakeSnapshotEntry:
    match entry:
        case FakeGitManagerFactoryPath():
            return FakeGitPath(
                repository_path=_tmp_replacement_path(entry.repository_path, tmp_path),
            )
        case FakeGitOperation():
            return entry
        case FakeCli():
            return FakeCliCommand(
                command=entry.command,
                current_directory=_tmp_replacement_path(entry.current_directory, tmp_path),
            )
        case FakeGitHubTokens():
            return entry
        case FakeGitHubPullRequest():
            return entry
        case FakeGitHubMergeableState():
            return entry
        case FakeGitHubIsMerged():
            return entry
        case FakeGitHubMergeSha():
            return entry
        case FakeGitHubMerge():
            return entry
        case FakeGitHubMergeError():
            return entry
        case FakeGitHubRunWorkflow():
            return FakeGitHubWorkflow(
                workflow_id_or_file_name=entry.workflow_id_or_file_name,
                ref=entry.ref,
                inputs=_normalize_workflow_inputs(entry.inputs),
            )
        case FakeReleaseStateFile():
            return FakeReleaseStateFileSnapshot(
                file_path=_tmp_replacement_path(entry.file_path, tmp_path),
                file_text=entry.file_text,
            )
    raise AssertionError(f"Unhandled fake entry: {type(entry)}")


class FakeOperations:
    """Chronological fake client operations (and optional ``FakeReleaseStateFile`` from tests)."""

    def __init__(self) -> None:
        """Initialize with no operations recorded yet."""
        self._entries: list[FakeEntry] = []

    def append(self, entry: FakeEntry) -> None:
        """Record one operation in chronological order (used by fakes in call order)."""
        self._entries.append(entry)

    def to_run_snapshot(self, tmp_path: Path, exit_code: int) -> FakeRunSnapshot:
        """Build a snapshot DTO: paths and maps normalized for machine-independent diffs."""
        return FakeRunSnapshot(
            exit_code=exit_code,
            operations=tuple(_entry_to_snapshot(log_entry, tmp_path) for log_entry in self._entries),
        )


class FakeGitManager(GitManager):
    """Fake Git manager for release CLI tests."""

    def __init__(
        self,
        clean: bool = True,
        git_repo: bool = True,
        remote_repository: bool = True,
        existing_branch_names: tuple[str, ...] = (),
        changed_file_paths: tuple[str, ...] = ("CHANGELOG.md", ".changes/release.yaml"),
        changed_file_paths_sequence: tuple[tuple[str, ...], ...] | None = None,
        operation_log: FakeOperations | None = None,
    ) -> None:
        """Initialize the fake manager.

        When ``changed_file_paths_sequence`` is provided, each call to
        ``changed_file_paths`` returns the next entry in the sequence,
        repeating the last entry once exhausted. Otherwise, every call
        returns ``changed_file_paths``.
        """
        self.clean = clean
        self.git_repo = git_repo
        self.remote_repository = remote_repository
        self.existing_branch_names = set(existing_branch_names)
        self._changed_file_paths_sequence: tuple[tuple[str, ...], ...] = (
            changed_file_paths_sequence if changed_file_paths_sequence is not None else (changed_file_paths,)
        )
        self._changed_file_paths_index = 0
        self.operations: list[FakeGitOperation] = []
        self._operation_log = operation_log

    def _append_operation(self, operation: FakeGitOperation) -> None:
        self.operations.append(operation)
        if self._operation_log is not None:
            self._operation_log.append(operation)

    def switch_branch(self, branch_name: str) -> None:
        """Record branch switch operations."""
        self._append_operation(FakeGitOperation(name="switch_branch", branch_name=branch_name))

    def pull_current_branch(self, remote_location: str = "origin") -> None:
        """Record pull operations."""
        self._append_operation(FakeGitOperation(name="pull_current_branch"))

    def create_branch(self, branch_name: str, objectish: str | None = None) -> None:
        """Record branch creation operations."""
        self.existing_branch_names.add(branch_name)
        self._append_operation(FakeGitOperation(name="create_branch", branch_name=branch_name))

    def branch_exists(self, branch_name: str) -> bool:
        """Return true when the configured branch exists locally."""
        return branch_name in self.existing_branch_names

    def add_all_changes(self) -> None:
        """Record add-all operations."""
        self._append_operation(FakeGitOperation(name="add_all_changes"))

    def add_file(self, file_path: Path | str) -> None:
        """Record single-file add operations."""
        self._append_operation(FakeGitOperation(name="add_file", file_path=str(file_path)))

    def add_commit(
        self,
        message: str,
        author: bytes | None = None,
        committer: bytes | None = None,
    ) -> str:
        """Record commit operations."""
        self._append_operation(FakeGitOperation(name="add_commit", message=message))
        return "commit-sha"

    def delete_branch(self, branch_name: str) -> None:
        """Record branch deletion operations."""
        self.existing_branch_names.discard(branch_name)
        self._append_operation(FakeGitOperation(name="delete_branch", branch_name=branch_name))

    def cherry_pick(self, commit_sha: str) -> str:
        """Record cherry-pick operations."""
        self._append_operation(FakeGitOperation(name="cherry_pick", commit_sha=commit_sha))
        return f"cherry-picked-{commit_sha}"

    def push_branch(self, branch_name: str, remote_location: str = "origin", force: bool = False) -> None:
        """Record push operations."""
        self._append_operation(FakeGitOperation(name="push_branch", branch_name=branch_name))

    def push_tag(
        self,
        tag_name: str,
        remote_location: str = "origin",
        objectish: str = "HEAD",
        lightweight: bool = True,
        force: bool = False,
        message: str | None = None,
        author: str | bytes | None = None,
    ) -> None:
        """Record tag push operations."""
        self._append_operation(FakeGitOperation(name="push_tag", branch_name=tag_name, commit_sha=objectish))

    def is_git_repo(self) -> bool:
        """Return the configured Git-repository state."""
        return self.git_repo

    def has_remote_repository(self, repository_name: str) -> bool:
        """Return the configured remote-repository state."""
        return self.remote_repository

    def is_state_clean(self) -> bool:
        """Return the configured clean state."""
        return self.clean

    def changed_file_paths(self) -> tuple[str, ...]:
        """Return the next configured changed file paths, repeating the last entry once exhausted."""
        paths = self._changed_file_paths_sequence[self._changed_file_paths_index]
        if self._changed_file_paths_index < len(self._changed_file_paths_sequence) - 1:
            self._changed_file_paths_index += 1
        return paths


class FakeGitManagerFactory:
    """Factory for fake Git managers."""

    def __init__(self, git_manager: FakeGitManager, operation_log: FakeOperations | None = None) -> None:
        """Initialize the factory."""
        self.git_manager = git_manager
        self.repository_paths: list[Path] = []
        self._operation_log = operation_log

    def __call__(self, repository_path: Path) -> FakeGitManager:
        """Record the repository path and return the configured manager."""
        self.repository_paths.append(repository_path)
        if self._operation_log is not None:
            self._operation_log.append(FakeGitManagerFactoryPath(repository_path=repository_path))
        return self.git_manager


@dataclass(frozen=True)
class CliCommand:
    """CLI command recorded by the fake runner."""

    # Command and arguments.
    command: tuple[str, ...]
    # Directory where the command was run.
    current_directory: Path


class FakeCliCommandRunner(CliCommandRunner):
    """Fake CLI command runner for release CLI tests."""

    def __init__(self, operation_log: FakeOperations | None = None) -> None:
        """Initialize the fake runner."""
        self.commands: list[CliCommand] = []
        self.captured_outputs: dict[tuple[str, ...], bytes] = {}
        self._operation_log = operation_log

    def _record_cli(self, command: tuple[str, ...], current_directory: Path) -> None:
        if self._operation_log is not None:
            self._operation_log.append(FakeCli(command=command, current_directory=current_directory))

    def run(
        self,
        command: tuple[str, ...],
        current_directory: Path,
        env: Mapping[str, str] | None = None,
        raise_exception_on_error: bool = True,
        capture_output: bool = False,
    ) -> CliCommandResult:
        """Record CLI commands and return success."""
        self.commands.append(CliCommand(command=command, current_directory=current_directory))
        self._record_cli(command, current_directory)
        stdout = self.captured_outputs.get(command, b"") if capture_output else b""
        return CliCommandResult(returncode=0, stdout=stdout)


class FakeReleaseHelperConsole:
    """Fake console for release-helper tests."""

    def __init__(self) -> None:
        """Initialize recorded console interactions."""
        self.messages: list[str] = []
        self.confirmations: list[str] = []
        self.spinner_messages: list[str] = []

    def echo(self, message: str) -> None:
        """Record a console message."""
        self.messages.append(message)

    def confirm(self, message: str) -> None:
        """Record a confirmation prompt."""
        self.confirmations.append(message)

    @contextmanager
    def spinner(self, message: str) -> Iterator[None]:
        """Record spinner usage while yielding control."""
        self.spinner_messages.append(message)
        yield


class FakeGitHubClient(GitHubClient):
    """Fake GitHub client for release CLI tests."""

    def __init__(self, pr_number: int = 123, operation_log: FakeOperations | None = None) -> None:
        """Initialize the fake GitHub client."""
        self.pr_number = pr_number
        self.created_pull_requests: list[CreatedPullRequest] = []
        self.updated_pull_requests: list[CreatedPullRequest] = []
        self.existing_pr_numbers_by_head_branch: dict[str, int] = {}
        self.mergeable_states: dict[int, list[str]] = {}
        self.already_merged_prs: set[int] = set()
        self.merged_prs: list[int] = []
        self.merge_methods: dict[int, str | None] = {}
        self.merge_shas: dict[int, str] = {}
        self.dispatched_workflows: list[tuple[str | int, str]] = []
        self._operation_log = operation_log

    def create_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Record pull request creation."""
        pr_record = CreatedPullRequest(
            title=title,
            body=body,
            head_branch=head_branch,
            base_branch=base_branch,
            draft=draft,
            labels=labels,
        )
        self.created_pull_requests.append(pr_record)
        if self._operation_log is not None:
            self._operation_log.append(FakeGitHubPullRequest(source="create_pr", pull_request=pr_record))
        return self.pr_number

    def create_or_update_pr(
        self,
        title: str,
        body: str | None,
        head_branch: str,
        base_branch: str,
        pr_number: int | None = None,
        maintainer_can_modify: bool = True,
        draft: bool = False,
        labels: tuple[str, ...] = (),
    ) -> int:
        """Record pull request upserts, updating an existing PR when configured."""
        del maintainer_can_modify
        pr_record = CreatedPullRequest(
            title=title,
            body=body,
            head_branch=head_branch,
            base_branch=base_branch,
            draft=draft,
            labels=labels,
        )
        existing_pr_number = pr_number or self.existing_pr_numbers_by_head_branch.get(head_branch)
        if existing_pr_number is None:
            self.created_pull_requests.append(pr_record)
            if self._operation_log is not None:
                self._operation_log.append(
                    FakeGitHubPullRequest(
                        source="create_or_update_create",
                        pull_request=pr_record,
                    )
                )
            return self.pr_number

        self.updated_pull_requests.append(pr_record)
        if self._operation_log is not None:
            self._operation_log.append(
                FakeGitHubPullRequest(
                    source="create_or_update_update",
                    pull_request=pr_record,
                )
            )
        return existing_pr_number

    def get_pr_mergeable_state(self, pr_number: int) -> str:
        """Return the next configured mergeable state for the given PR."""
        if self._operation_log is not None:
            self._operation_log.append(FakeGitHubMergeableState(pr_number=pr_number))
        states = self.mergeable_states.get(pr_number, ["clean"])
        if len(states) > 1:
            return states.pop(0)
        return states[0]

    def is_pr_merged(self, pr_number: int) -> bool:
        """Return true when the configured PR has been merged."""
        merged = pr_number in self.already_merged_prs
        if self._operation_log is not None:
            self._operation_log.append(FakeGitHubIsMerged(pr_number=pr_number, result=merged))
        return merged

    def get_pr_merge_commit_sha(self, pr_number: int) -> str:
        """Return the configured merge SHA for the PR."""
        sha = self.merge_shas.get(pr_number, f"merge-sha-{pr_number}")
        if self._operation_log is not None:
            self._operation_log.append(FakeGitHubMergeSha(pr_number=pr_number, result=sha))
        return sha

    def merge_pr(self, pr_number: int, merge_method: str | None = None) -> str:
        """Record the merge and return the configured SHA."""
        if pr_number in self.already_merged_prs:
            if self._operation_log is not None:
                self._operation_log.append(FakeGitHubMergeError(pr_number=pr_number))
            raise RuntimeError(f"Pull request #{pr_number} has already been merged.")

        if self._operation_log is not None:
            self._operation_log.append(FakeGitHubMerge(pr_number=pr_number, merge_method=merge_method))
        self.merged_prs.append(pr_number)
        self.merge_methods[pr_number] = merge_method
        merge_sha = self.merge_shas.get(pr_number, f"merge-sha-{pr_number}")
        self.already_merged_prs.add(pr_number)
        return merge_sha

    def run_workflow(
        self,
        workflow_id_or_file_name: str | int,
        ref: str,
        inputs: Mapping[str, str] | None = None,
    ) -> bool:
        """Record workflow dispatch."""
        if self._operation_log is not None:
            self._operation_log.append(
                FakeGitHubRunWorkflow(
                    workflow_id_or_file_name=workflow_id_or_file_name,
                    ref=ref,
                    inputs=inputs,
                )
            )
        self.dispatched_workflows.append((workflow_id_or_file_name, ref))
        return True


class FakeGitHubClientFactory:
    """Factory for fake GitHub clients."""

    def __init__(self, github_client: FakeGitHubClient, operation_log: FakeOperations | None = None) -> None:
        """Initialize the factory."""
        self.github_client = github_client
        self.access_tokens: list[str] = []
        self.repository_names: list[str] = []
        self._operation_log = operation_log

    def __call__(self, access_token: str, repository_name: str) -> FakeGitHubClient:
        """Record GitHub client construction."""
        self.access_tokens.append(access_token)
        self.repository_names.append(repository_name)
        if self._operation_log is not None:
            self._operation_log.append(
                FakeGitHubTokens(
                    access_tokens=tuple(self.access_tokens),
                    repository_names=tuple(self.repository_names),
                )
            )
        return self.github_client


def _make_metricflow_repo(tmp_path: Path) -> Path:
    repo_path = tmp_path / "metricflow"
    repo_path.mkdir()
    repo_path.joinpath(".git").mkdir()
    repo_path.joinpath("pyproject.toml").write_text('[tool.hatch.version]\npath = "metricflow/__about__.py"\n')
    dbt_metricflow_path = repo_path / "dbt-metricflow"
    dbt_metricflow_path.mkdir()
    dbt_metricflow_path.joinpath("pyproject.toml").write_text(
        '[tool.hatch.version]\npath = "dbt_metricflow/__about__.py"\n'
    )
    return repo_path


def _release_tool_command(repo_path: Path, step_args: list[str], yes: bool = True) -> list[str]:
    command: list[str] = []
    if yes:
        command.append("--yes")
    return [*command, step_args[0], "--metricflow-repo", str(repo_path), *step_args[1:]]


class FakeSleep:
    """Fake sleep callable that records durations without actually sleeping."""

    def __init__(self) -> None:
        """Initialize the fake sleep."""
        self.durations: list[float] = []

    def __call__(self, seconds: float) -> None:
        """Record the sleep duration."""
        self.durations.append(seconds)


def _make_context(
    current_directory: Path,
    git_manager: FakeGitManager,
    environment: dict[str, str] | None = None,
    confirm_all: bool = False,
    available_cli_commands: Collection[str] = ("fossa", "changie"),
    cli_command_runner: FakeCliCommandRunner | None = None,
    github_client_factory: FakeGitHubClientFactory | None = None,
    sleep: FakeSleep | None = None,
    operation_log: FakeOperations | None = None,
) -> ReleaseToolContext:
    command_runner = cli_command_runner or FakeCliCommandRunner(operation_log=operation_log)
    if github_client_factory is None:
        make_github_client = FakeGitHubClientFactory(
            github_client=FakeGitHubClient(operation_log=operation_log),
            operation_log=operation_log,
        )
    else:
        make_github_client = github_client_factory
    return ReleaseToolContext(
        environment=environment or _REQUIRED_ENVIRONMENT,
        current_directory=current_directory,
        confirm_all=confirm_all,
        git_manager_factory=FakeGitManagerFactory(git_manager=git_manager, operation_log=operation_log),
        github_client_factory=make_github_client,
        is_cli_command_available=available_cli_commands.__contains__,
        cli_command_runner=command_runner,
        sleep=sleep or FakeSleep(),
    )
