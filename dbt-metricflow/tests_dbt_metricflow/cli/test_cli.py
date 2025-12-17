"""Tests MF CLI commands e.g. `mf query ...`.

These tests could be parameterized to reduce boilerplate.
Tests are marked as slow because each CLI command is run in a new process, and the dbt adapter needs to be
initialized.
"""
from __future__ import annotations

import logging
import shutil
import tempfile
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_snapshot_text_equal,
    make_schema_replacement_function,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from tests_dbt_metricflow.cli.cli_test_helpers import (
    create_tutorial_project_files,
    run_and_check_cli_command,
    run_dbt_build,
)
from tests_dbt_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_dbt_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


def test_query(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=["--metrics", "transactions", "--group-by", "metric_time", "--order", "metric_time,transactions"],
    )


def test_list_dimensions(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_DIMENSIONS,
        args=["--metrics", "transactions"],
    )


def test_list_metrics(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_METRICS,
        args=[],
    )


def test_get_dimension_values(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_DIMENSION_VALUES,
        args=["--metrics", "transactions", "--dimension", "customer__customer_country"],
    )


@contextmanager
def create_directory(directory_path: str) -> Iterator[None]:
    """Creates a temporary directory (errors if it exists) and removes it."""
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    yield
    shutil.rmtree(path)


def test_validate_configs(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests configuration validation by adding a semantic model file with errors."""
    yaml_contents = textwrap.dedent(
        """\
        semantic_models:
          - name: bad_semantic_model
            model: ref('transactions')
            defaults:
              agg_time_dimension: ds
            primary_entity: bad_semantic_model_primary_entity
            dimensions:
              - name: ds
                type: time
                type_params:
                  time_granularity: day
              - name: bad_dimension
                expr: non_existent_column
                type: categorical
        """
    )
    with tempfile.TemporaryDirectory() as tmp_directory_str:
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory_str))
        with open(dbt_project_path / "models" / "bad_semantic_model.yml", "w") as f:
            f.write(yaml_contents)
        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )
        with cli_runner.running_context():
            run_dbt_build(cli_runner)

            run_and_check_cli_command(
                request=request,
                cli_test_configuration=cli_test_configuration,
                cli_runner=cli_runner,
                command_enum=IsolatedCliCommandEnum.MF_VALIDATE_CONFIGS,
                args=[],
                expectation_description="There should be two validation failures with `bad_semantic_model`.",
                expected_exit_code=1,
            )


def test_health_checks(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_HEALTH_CHECKS,
        args=[],
    )


def test_tutorial_message(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests the message output of the tutorial.

    The tutorial now essentially compiles a semantic manifest and then asks the user to run dbt seed,
    so from an end user perspective it's little more than the output with -m.

    The tutorial currently requires execution from a dbt project path. Rather than go all the way on testing the
    tutorial given the path and dbt project requirements, we simply check the message output. When we allow for
    project path overrides it might warrant a more complete test of the semantic manifest building steps in the
    tutorial flow.
    """
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_TUTORIAL,
        args=["-m"],
    )


def test_list_entities(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_ENTITIES,
        args=["--metrics", "transactions"],
    )


def test_list_saved_queries(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_SAVED_QUERIES,
        args=[],
    )


def test_saved_query(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--saved-query",
            "core_transaction_metrics",
            "--order",
            "metric_time__day,customer__customer_country,transactions,quick_buy_transactions",
        ],
    )


def test_saved_query_with_where(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    where_argument_value = (
        "{{ Dimension('customer__customer_country') }}  = 'US' AND {{ TimeDimension('metric_time') }} <= '2022-03-12'"
    )
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--saved-query",
            "core_transaction_metrics",
            "--where",
            where_argument_value,
            "--order",
            "metric_time__day,customer__customer_country,transactions,quick_buy_transactions",
        ],
    )


def test_saved_query_with_limit(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--saved-query",
            "core_transaction_metrics",
            "--order",
            "metric_time__day,customer__customer_country,transactions,quick_buy_transactions",
            "--limit",
            "3",
        ],
    )


def test_saved_query_explain(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=["--explain", "--saved-query", "core_transaction_metrics"],
    )


def test_saved_query_with_cumulative_metric(  # noqa: D103
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        cli_test_configuration=cli_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=[
            "--saved-query",
            "cumulative_transaction_metrics",
            "--start-time",
            "2022-03-29",
            "--end-time",
            "2022-03-31",
            "--order",
            "metric_time__day,cumulative_transactions,cumulative_transactions_in_last_7_days",
        ],
    )


def test_csv(
    request: FixtureRequest,
    cli_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests writing the results of a query to a file."""
    with tempfile.TemporaryDirectory() as working_directory:
        working_directory_path = Path(working_directory)
        dbt_project_path = create_tutorial_project_files(working_directory_path)

        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )
        with cli_runner.running_context():
            run_dbt_build(cli_runner)
            command_enum = IsolatedCliCommandEnum.MF_QUERY
            csv_filename = "transactions.csv"
            command_args = ["--metrics", "transactions,quick_buy_transactions", "--csv", str(csv_filename)]
            logger.info(
                LazyFormat(
                    "Running query and writing results to a CSV",
                    command_enum=command_enum,
                    csv_filename=csv_filename,
                    command_args=command_args,
                )
            )
            result = cli_runner.run_command(
                command_enum=IsolatedCliCommandEnum.MF_QUERY,
                command_args=command_args,
                working_directory_path=working_directory_path,
            )
            result.raise_exception_on_failure()
            csv_file_path = (working_directory_path / csv_filename).absolute()
            with open(csv_file_path, "r") as csv_file:
                csv_file_contents = csv_file.read()
                assert_snapshot_text_equal(
                    request=request,
                    snapshot_configuration=cli_test_configuration,
                    group_id="str",
                    snapshot_id="result",
                    snapshot_text=csv_file_contents,
                    snapshot_file_extension=".txt",
                    expectation_description="A CSV file containing the values for 2 metrics.",
                    incomparable_strings_replacement_function=make_schema_replacement_function(
                        system_schema=cli_test_configuration.mf_system_schema,
                        source_schema=cli_test_configuration.mf_source_schema,
                    ),
                )
