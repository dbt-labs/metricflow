from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional, Sequence

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.cli_string import CLIString
from dbt_metricflow.cli.tutorial import dbtMetricFlowTutorialHelper
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner
from tests_metricflow.fixtures.cli_fixtures import MetricFlowCliRunner
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


def run_and_check_cli_command(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
    command_enum: IsolatedCliCommandEnum,
    args: Sequence[str],
    sql_client: Optional[SqlClient] = None,
    expected_exit_code: int = 0,
    expectation_description: Optional[str] = None,
) -> None:
    """Run the given CLI command and check that the output matches the stored snapshot."""
    result = cli_runner.run_command(
        command_enum=command_enum,
        command_args=args,
    )

    if result.exit_code != expected_exit_code:
        assert False, LazyFormat(
            "Command exit code mismatch",
            expected_exit_code=expected_exit_code,
            actual_exit_code=result.exit_code,
            result=result,
        ).evaluated_value

    # Replace incomparable values in snapshots with `***`.
    snapshot_str = result.output
    for prefix in (
        CLIString.LOG_FILE_PREFIX,
        CLIString.ARTIFACT_PATH,
        CLIString.ARTIFACT_MODIFIED_TIME,
    ):
        regex_parts = (r"(?P<prefix>", prefix, r").*")
        snapshot_str = re.sub("".join(regex_parts), repl=r"\g<prefix> ***", string=snapshot_str)

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=snapshot_str,
        sql_engine=sql_client.sql_engine_type if sql_client else None,
        expectation_description=expectation_description,
    )
    assert result.exit_code == expected_exit_code


def create_tutorial_project_files(tmp_directory: Path) -> Path:
    """Create files for the tutorial project in a subdirectory of the given path.

    Returns the path to the tutorial project.
    """
    dbt_project_path = tmp_directory / "mf_tutorial_project"
    logger.debug(LazyFormat("Creating all tutorial files", dbt_project_path=dbt_project_path))
    dbtMetricFlowTutorialHelper.generate_dbt_project(dbt_project_path)

    model_path = dbt_project_path / "models" / "sample_model"
    seed_path = dbt_project_path / "seeds" / "sample_seed"

    dbtMetricFlowTutorialHelper.generate_model_files(model_path=model_path)
    dbtMetricFlowTutorialHelper.generate_seed_files(seed_path=seed_path)
    logger.debug(LazyFormat("Created dbt project", dbt_project_path=dbt_project_path))
    return dbt_project_path


def run_dbt_build(cli_command_runner: IsolatedCliCommandRunner) -> None:
    """Runs `dbt build` using the given runner.

    If there is an error with the build, an exception is raised.
    """
    dbt_build_result = cli_command_runner.run_command(
        command_enum=IsolatedCliCommandEnum.DBT_BUILD,
        command_args=(),
    )
    dbt_build_result.raise_exception_on_failure()
    logger.debug(LazyFormat("`dbt build` successful"))


def create_cli_runner(dbt_project_path: Path) -> MetricFlowCliRunner:
    """Return a CLI runner that is based on the dbt project at the given path."""
    cli_configuration = CLIConfiguration()
    cli_configuration.setup(
        dbt_profiles_path=dbt_project_path, dbt_project_path=dbt_project_path, configure_file_logging=False
    )
    return MetricFlowCliRunner(cli_configuration, str(dbt_project_path))
