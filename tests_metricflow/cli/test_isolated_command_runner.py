from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Dict

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.mf_logging.pretty_print import mf_pformat_dict
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.cli.cli_test_helpers import (
    create_tutorial_project_files,
    run_dbt_build,
)
from tests_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner
from tests_metricflow.snapshot_utils import assert_str_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_isolated_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
        assert_str_snapshot_equal(
            request=request,
            mf_test_configuration=mf_test_configuration,
            snapshot_id="result",
            snapshot_str=result.stdout,
            expectation_description="A table showing the `transactions` metric.",
        )


@pytest.mark.slow
def test_multiple_queries(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
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
            result_dict["transactions_query"] = result.stdout
            result = cli_runner.run_command(
                command_enum=command_enum,
                command_args=[
                    "--metrics",
                    "quick_buy_transactions",
                ],
            )
            result.raise_exception_on_failure()
            result_dict["quick_buy_transactions_query"] = result.stdout

        assert_str_snapshot_equal(
            request=request,
            mf_test_configuration=mf_test_configuration,
            snapshot_id="result",
            snapshot_str=mf_pformat_dict(obj_dict=result_dict, preserve_raw_strings=True),
            expectation_description="2 results showing the`transactions` and `quick_buy_transactions` metrics.",
        )
