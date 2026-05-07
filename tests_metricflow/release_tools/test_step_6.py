from __future__ import annotations

import logging
from pathlib import Path

from _pytest.fixtures import FixtureRequest
from click.testing import CliRunner
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal

from scripts.release_tool.mf_release_tool import (
    CLI_COMMAND_STEP_6,
    GITHUB_REPOSITORY_HTML_BASE_URL,
    RELEASE_TOOL_STATE_FILE_PATH,
    ReleaseStep1State,
    ReleaseStep4State,
    ReleaseStep5State,
    ReleaseToolContext,
    ReleaseToolState,
    cli,
)
from tests_metricflow.release_tools.release_tool_test_helpers import (
    RELEASE_TOOL_TEST_ENVIRONMENT,
    FakeCliCommandRunner,
    FakeGitHubClient,
    FakeGitHubClientFactory,
    FakeGitManager,
    FakeGitManagerFactory,
    FakeOperations,
    FakeReleaseStateFile,
    FakeSleep,
    _make_metricflow_repo,
    _release_tool_command,
    write_release_tool_state_file,
)

logger = logging.getLogger(__name__)


def _write_step_1_and_step_4_and_step_5_state(
    repo_path: Path,
    version: str = "1.2.3",
    dbt_version: str = "0.12.0",
    dbt_dev_version: str = "0.13.0.dev0",
    step_1_pr_number: int = 100,
    step_4_pr_number: int = 300,
    step_5_pr_number: int = 400,
    username: str = "metricflow-user",
) -> None:
    """Write a state file with step 1, 4, and 5 done (for step-6)."""
    step_1 = ReleaseStep1State(
        metricflow_package_version=version,
        branch_name=f"{username}/release_pr/{version}/step_1",
        pr_number=step_1_pr_number,
        pr_link=f"{GITHUB_REPOSITORY_HTML_BASE_URL}/pull/{step_1_pr_number}",
    )
    step_4 = ReleaseStep4State(
        metricflow_package_version=version,
        dbt_metricflow_package_version=dbt_version,
        branch_name=f"{username}/release_pr/{version}/step_4",
        pr_number=step_4_pr_number,
        pr_link=f"{GITHUB_REPOSITORY_HTML_BASE_URL}/pull/{step_4_pr_number}",
    )
    step_5 = ReleaseStep5State(
        metricflow_package_version=version,
        dbt_metricflow_package_version=dbt_dev_version,
        branch_name=f"{username}/release_pr/{version}/step_5",
        commit_sha="fake-step-5-commit-sha",
        pr_number=step_5_pr_number,
        pr_link=f"{GITHUB_REPOSITORY_HTML_BASE_URL}/pull/{step_5_pr_number}",
    )
    write_release_tool_state_file(repo_path, ReleaseToolState(step_1=step_1, step_4=step_4, step_5=step_5))


def test_step_6_all_operations_match_snapshot(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    tmp_path: Path,
) -> None:
    """Snapshot of the fake-operation log and final state file for ``step-6`` with ``--yes``."""
    operation_log = FakeOperations()
    git_manager = FakeGitManager(operation_log=operation_log)
    github_client = FakeGitHubClient(operation_log=operation_log)
    github_client.merge_shas = {300: "abc123", 400: "def456"}
    github_client_factory = FakeGitHubClientFactory(github_client=github_client, operation_log=operation_log)
    fake_sleep = FakeSleep()
    repo_path = _make_metricflow_repo(tmp_path)
    _write_step_1_and_step_4_and_step_5_state(repo_path)
    cli_runner = FakeCliCommandRunner(operation_log=operation_log)
    fake_git_manager_factory = FakeGitManagerFactory(git_manager=git_manager, operation_log=operation_log)
    release_tool_context = ReleaseToolContext(
        environment=RELEASE_TOOL_TEST_ENVIRONMENT,
        current_directory=repo_path,
        confirm_all=False,
        git_manager_factory=fake_git_manager_factory.create_manager,
        github_client_factory=github_client_factory.create_client,
        is_cli_command_available=("fossa", "changie").__contains__,
        cli_command_runner=cli_runner,
        sleep=fake_sleep.sleep,
    )

    result = CliRunner().invoke(
        cli,
        _release_tool_command(repo_path, [CLI_COMMAND_STEP_6], yes=True),
        obj=release_tool_context,
    )
    assert result.exit_code == 0, result.output

    state_path = repo_path / RELEASE_TOOL_STATE_FILE_PATH
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
            "Sequential log from fake git, GitHub, and CLI clients after a successful step-6 with --yes, "
            "with the final release state file appended."
        ),
    )
