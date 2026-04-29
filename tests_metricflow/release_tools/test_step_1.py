from __future__ import annotations

import logging
from pathlib import Path

from _pytest.fixtures import FixtureRequest
from click.testing import CliRunner
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal

from scripts.release_tool.mf_release_tool import cli
from tests_metricflow.release_tools.release_tool_test_helpers import (
    _STATE_FILE_PATH,
    FakeCliCommandRunner,
    FakeGitHubClient,
    FakeGitHubClientFactory,
    FakeGitManager,
    FakeOperations,
    FakeReleaseStateFile,
    _make_context,
    _make_metricflow_repo,
    _release_tool_command,
)

logger = logging.getLogger(__name__)


def test_step_1_all_operations_match_snapshot(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    tmp_path: Path,
) -> None:
    """Snapshot of the fake-operation log and final state file for ``step-1`` with ``--yes`` (``--yes`` → confirm_all)."""
    operation_log = FakeOperations()
    git_manager = FakeGitManager(
        changed_file_paths_sequence=(
            ("CHANGELOG.md", ".changes/release.yaml"),
            ("CHANGELOG.md", ".changes/release.yaml"),
            ("ATTRIBUTION.md",),
            ("ATTRIBUTION.md",),
            ("metricflow/__about__.py",),
            ("metricflow/__about__.py",),
        ),
        operation_log=operation_log,
    )
    cli_command_runner = FakeCliCommandRunner(operation_log=operation_log)
    cli_command_runner.captured_outputs[("fossa", "report", "attribution", "--format", "markdown")] = b"# Attribution\n"
    github_client = FakeGitHubClient(operation_log=operation_log)
    github_client_factory = FakeGitHubClientFactory(github_client=github_client, operation_log=operation_log)
    repo_path = _make_metricflow_repo(tmp_path)
    release_tool_context = _make_context(
        current_directory=repo_path,
        git_manager=git_manager,
        cli_command_runner=cli_command_runner,
        github_client_factory=github_client_factory,
        operation_log=operation_log,
    )

    result = CliRunner().invoke(
        cli,
        _release_tool_command(repo_path, ["step-1", "--version", "1.2.3"], yes=True),
        obj=release_tool_context,
    )
    assert result.exit_code == 0, result.output

    state_path = repo_path / _STATE_FILE_PATH
    operation_log.append(
        FakeReleaseStateFile(
            file_path=state_path,
            file_text=state_path.read_text(),
        )
    )

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=operation_log.to_run_snapshot(tmp_path=tmp_path, exit_code=result.exit_code),
        obj_id="result",
        expectation_description=(
            "Sequential log from fake git, GitHub, and CLI clients after a successful step-1 with --yes, "
            "with the final release state file appended."
        ),
    )
