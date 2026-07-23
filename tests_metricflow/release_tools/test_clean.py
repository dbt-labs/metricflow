from __future__ import annotations

import logging
from pathlib import Path

from _pytest.fixtures import FixtureRequest
from click.testing import CliRunner
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal

from scripts.release_tool.mf_release_tool import (
    CLI_COMMAND_CLEAN,
    RELEASE_TOOL_STATE_FILE_PATH,
    ReleaseToolContext,
    cli,
)
from tests_metricflow.release_tools.release_tool_test_helpers import (
    RELEASE_TOOL_TEST_ENVIRONMENT,
    FakeCliCommandRunner,
    FakeGitHubClient,
    FakeGitHubClientFactory,
    FakeGitManager,
    FakeGitManagerFactory,
    FakeSleep,
    _make_metricflow_repo,
    _release_tool_command,
)

logger = logging.getLogger(__name__)


def _release_tool_context(repo_path: Path, git_manager: FakeGitManager) -> ReleaseToolContext:
    """Create a release-tool context backed by test fakes."""
    fake_git_manager_factory = FakeGitManagerFactory(git_manager=git_manager)
    return ReleaseToolContext(
        environment=RELEASE_TOOL_TEST_ENVIRONMENT,
        current_directory=repo_path,
        confirm_all=False,
        git_manager_factory=fake_git_manager_factory.create_manager,
        github_client_factory=FakeGitHubClientFactory(github_client=FakeGitHubClient()).create_client,
        is_cli_command_available=("docker", "changie").__contains__,
        cli_command_runner=FakeCliCommandRunner(),
        sleep=FakeSleep().sleep,
    )


def _normalize_output(output: str, tmp_path: Path) -> str:
    """Return command output with temp paths normalized for snapshots."""
    return output.replace(str(tmp_path), "<TMP>")


def test_clean_with_missing_state_file(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    tmp_path: Path,
) -> None:
    """Check running `clean` with a missing state file."""
    git_manager = FakeGitManager()
    repo_path = _make_metricflow_repo(tmp_path)

    result = CliRunner().invoke(
        cli,
        _release_tool_command(repo_path, [CLI_COMMAND_CLEAN], yes=True),
        obj=_release_tool_context(repo_path=repo_path, git_manager=git_manager),
    )

    assert result.exit_code == 0, result.output
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=_normalize_output(output=result.output, tmp_path=tmp_path),
        expectation_description="Includes a message for a missing state file.",
    )


def test_clean_with_invalid_state_file(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    tmp_path: Path,
) -> None:
    """Check running `clean` with an invalid state file."""
    git_manager = FakeGitManager()
    repo_path = _make_metricflow_repo(tmp_path)
    state_file_path = repo_path / RELEASE_TOOL_STATE_FILE_PATH
    state_file_path.parent.mkdir(parents=True, exist_ok=True)
    state_file_path.write_text("<Invalid JSON>")

    result = CliRunner().invoke(
        cli,
        _release_tool_command(repo_path, [CLI_COMMAND_CLEAN], yes=True),
        obj=_release_tool_context(repo_path=repo_path, git_manager=git_manager),
    )

    assert result.exit_code == 1, result.output
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_str=_normalize_output(output=result.output, tmp_path=tmp_path),
        expectation_description="Includes a message for an invalid state file.",
    )
