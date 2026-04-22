from __future__ import annotations

import json
import logging
from pathlib import Path

from _pytest.fixtures import FixtureRequest
from click.testing import CliRunner
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal

from scripts.release_tool.mf_release_tool import cli
from tests_metricflow.release_tools.release_tool_test_helpers import (
    _STATE_FILE_PATH,
    _STEP_4_DBT_ABOUT_FILE_PATH,
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


def _write_step_1_and_step_4_state(
    repo_path: Path,
    version: str = "1.2.3",
    dbt_version: str = "0.12.0",
    step_1_pr_number: int = 100,
    step_4_pr_number: int = 300,
    username: str = "metricflow-user",
) -> None:
    """Write a state file with step 1 and 4 done (for step-5)."""
    state_file_path = repo_path / _STATE_FILE_PATH
    state_file_path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "step_1": {
            "metricflow_package_version": version,
            "branch_name": f"{username}/release_pr/{version}/step_1",
            "pr_number": step_1_pr_number,
            "pr_link": f"https://github.com/dbt-labs/metricflow/pull/{step_1_pr_number}",
        },
        "step_2": None,
        "step_3": None,
        "step_4": {
            "metricflow_package_version": version,
            "dbt_metricflow_package_version": dbt_version,
            "branch_name": f"{username}/release_pr/{version}/step_4",
            "pr_number": step_4_pr_number,
            "pr_link": f"https://github.com/dbt-labs/metricflow/pull/{step_4_pr_number}",
        },
        "step_5": None,
        "step_6": None,
    }
    state_file_path.write_text(json.dumps(state, indent=4) + "\n")


def test_step_5_all_operations_match_snapshot(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    tmp_path: Path,
) -> None:
    """Snapshot of the fake-operation log and final state file for ``step-5`` with ``--yes``."""
    operation_log = FakeOperations()
    git_manager = FakeGitManager(
        changed_file_paths=(_STEP_4_DBT_ABOUT_FILE_PATH,),
        operation_log=operation_log,
    )
    cli_command_runner = FakeCliCommandRunner(operation_log=operation_log)
    github_client = FakeGitHubClient(pr_number=901, operation_log=operation_log)
    github_client_factory = FakeGitHubClientFactory(github_client=github_client, operation_log=operation_log)
    repo_path = _make_metricflow_repo(tmp_path)
    _write_step_1_and_step_4_state(repo_path)
    release_tool_context = _make_context(
        current_directory=repo_path,
        git_manager=git_manager,
        cli_command_runner=cli_command_runner,
        github_client_factory=github_client_factory,
        available_cli_commands=("fossa", "changie", "hatch"),
        operation_log=operation_log,
    )

    result = CliRunner().invoke(
        cli,
        _release_tool_command(repo_path, ["step-5"], yes=True),
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
            "Sequential log from fake git, GitHub, and CLI clients after a successful step-5 with --yes, "
            "with the final release state file appended."
        ),
    )
