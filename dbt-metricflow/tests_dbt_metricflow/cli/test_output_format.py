from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from dbt_metricflow.cli.tutorial import dbtMetricFlowTutorialHelper
from tests_dbt_metricflow.cli.cli_test_helpers import (
    run_and_check_cli_command,
    run_dbt_build,
)
from tests_dbt_metricflow.cli.demo_data_types_project_add_on import DEMO_DATA_TYPES_ADD_ON_PATH_ANCHOR
from tests_dbt_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_dbt_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def cli_runner() -> Iterator[IsolatedCliCommandRunner]:
    """CLI runner using the dbt project for testing CLI output format."""
    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        dbt_project_path = tmp_directory_path / DEMO_DATA_TYPES_ADD_ON_PATH_ANCHOR.directory.name
        copytree_src = DEMO_DATA_TYPES_ADD_ON_PATH_ANCHOR.directory
        copytree_dest = dbt_project_path
        logger.debug(
            LazyFormat(
                "Creating data-types project",
                dbt_project_path=dbt_project_path,
                copytree_src=copytree_src,
                copytree_dest=copytree_dest,
            )
        )
        dbtMetricFlowTutorialHelper.generate_dbt_project(dbt_project_path)
        shutil.copytree(
            src=copytree_src,
            dst=copytree_dest,
            dirs_exist_ok=True,
        )
        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )

        with cli_runner.running_context():
            run_dbt_build(cli_runner)

            yield cli_runner


def test_print_numeric_types(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the default display of numeric types in the output of `mf query`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'numeric'",
            "--order",
            "row",
        ],
    )


def test_print_string(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the default display of strings in the output of `mf query`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__description",
            "--where",
            "{{ Dimension('row__test_group') }} = 'string'",
            "--order",
            "row",
        ],
    )


def test_print_null(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the default display of `NULL` in the output of `mf query`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value,row__description",
            "--where",
            "{{ Dimension('row__test_group') }} = 'null'",
            "--order",
            "row",
        ],
    )


def test_decimals_option(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the output of `mf query --decimals 1 ...`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'numeric'",
            "--order",
            "row",
            "--decimals",
            "1",
        ],
        expectation_description="Non-integer numeric types should be displayed with 1 digit after the decimal point.",
    )


def test_decimals_option_invalid(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the output of `mf query --decimals invalid_value ...`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'numeric'",
            "--order",
            "row",
            "--decimals",
            "invalid_value",
        ],
        expectation_description="An error should be displayed due to the invalid argument value.",
        expected_exit_code=2,
    )


def test_decimals_option_zero(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the output of `mf query --decimals 0 ...`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'numeric'",
            "--order",
            "row",
            "--decimals",
            "0",
        ],
        expectation_description="A decimal point should not appear.",
    )


def test_decimals_option_negative(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the output of `mf query --decimals -1 ...`."""
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'numeric'",
            "--order",
            "row",
            "--decimals",
            "-1",
        ],
        expectation_description="An error should be displayed due to the invalid argument value.",
        expected_exit_code=1,
    )


def test_single_large_number_with_decimals_option(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests how a large number in a single row is displayed as the result of `mf query`.

    Originally added to reproduce a bug with how large numbers were displayed due to use of `tabulate`.
    """
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--metrics",
            "demo_metric",
            "--group-by",
            "row,row__str_value,row__int_value,row__float_value,row__decimal_value",
            "--where",
            "{{ Dimension('row__test_group') }} = 'large_numbers_in_one_row'",
            "--order",
            "row",
            "--decimals",
            "2",
        ],
        expectation_description=(
            "Non-integer numeric values should not be displayed in exponent notation and should have 2 decimals."
        ),
    )
