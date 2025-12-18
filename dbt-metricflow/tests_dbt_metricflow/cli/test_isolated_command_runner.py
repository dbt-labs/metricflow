from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_snapshot_text_equal,
    make_schema_replacement_function,
)
from metricflow_semantics.toolkit.mf_logging.pretty_print import PrettyFormatDictOption, mf_pformat_dict

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from tests_dbt_metricflow.cli.cli_test_helpers import (
    create_tutorial_project_files,
    run_dbt_build,
)
from tests_dbt_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_dbt_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


def test_isolated_query(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests running an MF query using the isolated runner."""
    with tempfile.TemporaryDirectory() as tmp_directory:
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory))

        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )
        with cli_runner.running_context():
            run_dbt_build(cli_runner)
            command_enum = IsolatedCliCommandEnum.MF_QUERY
            logger.debug(f"{command_enum=}")
            result = cli_runner.run_command(
                command_enum=command_enum,
                command_args=[
                    "--metrics",
                    "transactions",
                ],
            )

        result.raise_exception_on_failure()
        assert_snapshot_text_equal(
            request=request,
            snapshot_configuration=cli_test_configuration,
            group_id="str",
            snapshot_id="result",
            snapshot_text=result.output,
            snapshot_file_extension=".txt",
            expectation_description="A table showing the `transactions` metric.",
            incomparable_strings_replacement_function=make_schema_replacement_function(
                system_schema=cli_test_configuration.mf_system_schema,
                source_schema=cli_test_configuration.mf_source_schema,
            ),
        )


def test_multiple_queries(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests running multiple sequential MF queries using a single runner."""
    with tempfile.TemporaryDirectory() as tmp_directory:
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory))

        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )

        result_dict: Dict[str, str] = {}
        with cli_runner.running_context():
            run_dbt_build(cli_runner)
            command_enum = IsolatedCliCommandEnum.MF_QUERY
            logger.debug(f"{command_enum=}")
            result = cli_runner.run_command(
                command_enum=command_enum,
                command_args=[
                    "--metrics",
                    "transactions",
                ],
            )
            result.raise_exception_on_failure()
            result_dict["transactions_query"] = result.output
            result = cli_runner.run_command(
                command_enum=command_enum,
                command_args=[
                    "--metrics",
                    "quick_buy_transactions",
                ],
            )
            result.raise_exception_on_failure()
            result_dict["quick_buy_transactions_query"] = result.output

        assert_snapshot_text_equal(
            request=request,
            snapshot_configuration=cli_test_configuration,
            group_id="str",
            snapshot_id="result",
            snapshot_text=mf_pformat_dict(
                obj_dict=result_dict, format_option=PrettyFormatDictOption(preserve_raw_strings=True)
            ),
            snapshot_file_extension=".txt",
            expectation_description="2 results showing the`transactions` and `quick_buy_transactions` metrics.",
            incomparable_strings_replacement_function=make_schema_replacement_function(
                system_schema=cli_test_configuration.mf_system_schema,
                source_schema=cli_test_configuration.mf_source_schema,
            ),
        )


def test_environment_variables(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests running an MF CLI command that configures the profile / project location using environment variables."""
    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        dbt_project_path = create_tutorial_project_files(tmp_directory_path / "dbt_projects")

        # Move the `profiles.yml` to a different directory to isolate the two variables.
        dbt_profiles_path = tmp_directory_path / "dbt_profiles"
        os.mkdir(dbt_profiles_path)
        shutil.move(
            src=dbt_project_path / "profiles.yml",
            dst=dbt_profiles_path / "profiles.yml",
        )

        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_profiles_path,
            dbt_project_path=dbt_project_path,
        )
        with cli_runner.running_context():
            run_dbt_build(cli_runner)

        cli_runner_using_environment_variables = IsolatedCliCommandRunner(
            environment_variable_mapping={
                CLIConfiguration.DBT_PROFILES_DIR_ENV_VAR_NAME: str(dbt_profiles_path),
                CLIConfiguration.DBT_PROJECT_DIR_ENV_VAR_NAME: str(dbt_project_path),
            }
        )

        with cli_runner_using_environment_variables.running_context():
            result = cli_runner_using_environment_variables.run_command(
                command_enum=IsolatedCliCommandEnum.MF_QUERY,
                command_args=[
                    "--metrics",
                    "transactions",
                ],
            )
        result.raise_exception_on_failure()
        assert_snapshot_text_equal(
            request=request,
            snapshot_configuration=cli_test_configuration,
            group_id="str",
            snapshot_id="result",
            snapshot_text=result.output,
            snapshot_file_extension=".txt",
            expectation_description="A table showing the `transactions` metric.",
            incomparable_strings_replacement_function=make_schema_replacement_function(
                system_schema=cli_test_configuration.mf_system_schema,
                source_schema=cli_test_configuration.mf_source_schema,
            ),
        )
