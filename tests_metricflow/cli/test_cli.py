from __future__ import annotations

import logging
import shutil
import textwrap
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_yaml_files_to_validation_ready_semantic_manifest,
)
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.test_utils import base_semantic_manifest_file
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.main import (
    dimension_values,
    dimensions,
    entities,
    health_checks,
    metrics,
    query,
    tutorial,
    validate_configs,
)
from tests_metricflow.cli.cli_test_helpers import run_and_check_cli_command
from tests_metricflow.fixtures.cli_fixtures import MetricFlowCliRunner

logger = logging.getLogger(__name__)


@pytest.mark.duckdb_only
def test_query(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--metrics", "bookings", "--group-by", "metric_time", "--order", "metric_time,bookings"],
    )


@pytest.mark.duckdb_only
def test_list_dimensions(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=dimensions,
        args=["--metrics", "bookings"],
    )


@pytest.mark.duckdb_only
def test_list_metrics(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=metrics,
        args=[],
    )


@pytest.mark.duckdb_only
def test_get_dimension_values(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=dimension_values,
        args=["--metrics", "bookings", "--dimension", "booking__is_instant"],
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
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_context: CLIConfiguration,
) -> None:
    """Tests config validation from a manifest stored on the filesystem.

    This test is special, because the CLI bypasses the semantic manifest read into the CLIContext and
    validates the config files on disk. It's not entirely clear why we do this, so we should probably
    figure that out and, if possible, stop doing it so we can have this test depend on an injectable
    in-memory semantic manifest instead of whatever is stored in the filesystem.

    At any rate, due to the direct read from disk, we have to store a serialized semantic manifest
    in a temporary location. In order to spin up the CLI this requires us to ALSO have a dbt_project.yml
    on the filesystem in the project path. Since we don't want to clobber whatever semantic_manifest.json is
    in the real filesystem location we do all of this stuff here.
    """
    yaml_contents = textwrap.dedent(
        """\
        semantic_model:
          name: bad_semantic_model
          node_relation:
            schema_name: some_schema
            alias: some_table
          defaults:
            agg_time_dimension: ds
          dimensions:
            - name: country
              type: categorical
        """
    )
    bad_semantic_model = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)
    # JSON-stored manifests from dbt are not transformed, so we run this test on that style of output
    manifest = parse_yaml_files_to_validation_ready_semantic_manifest(
        [EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE, base_semantic_manifest_file(), bad_semantic_model],
        apply_transformations=False,
    ).semantic_manifest

    project_directory = Path().absolute()
    # If the dbt_project.yml doesn't exist in this path location the CLI will throw an exception.
    dummy_project = Path(project_directory, "dbt_project.yml")
    dummy_project.touch()

    try:
        cli_runner = MetricFlowCliRunner(cli_context=cli_context, project_path=str(project_directory))
        target_directory = Path(project_directory, "target")
        with create_directory(target_directory.as_posix()):
            manifest_file = Path(target_directory, "semantic_manifest.json")
            manifest_file.write_text(manifest.json())

            run_and_check_cli_command(
                request=request,
                capsys=capsys,
                mf_test_configuration=mf_test_configuration,
                cli_runner=cli_runner,
                command=validate_configs,
                args=[],
                expectation_description="There should be two validation failures with `bad_semantic_model`.",
                expected_exit_code=1,
            )

    finally:
        dummy_project.unlink()


@pytest.mark.duckdb_only
def test_health_checks(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=health_checks,
        args=[],
    )


def test_tutorial_message(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
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
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=tutorial,
        args=["-m"],
    )


def test_list_entities(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=entities,
        args=["--metrics", "bookings"],
    )


@pytest.mark.duckdb_only
def test_saved_query(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--saved-query", "p0_booking", "--order", "metric_time__day,listing__capacity_latest"],
    )


@pytest.mark.duckdb_only
def test_saved_query_with_where(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=[
            "--saved-query",
            "p0_booking",
            "--order",
            "metric_time__day,listing__capacity_latest",
            "--where",
            "{{ Dimension('listing__capacity_latest') }} > 4",
        ],
    )


@pytest.mark.duckdb_only
def test_saved_query_with_limit(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=[
            "--saved-query",
            "p0_booking",
            "--order",
            "metric_time__day,listing__capacity_latest",
            "--limit",
            "3",
        ],
    )


@pytest.mark.duckdb_only
def test_saved_query_explain(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=["--explain", "--saved-query", "p0_booking", "--order", "metric_time__day,listing__capacity_latest"],
    )


@pytest.mark.duckdb_only
def test_saved_query_with_cumulative_metric(  # noqa: D103
    request: FixtureRequest,
    capsys: pytest.CaptureFixture,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: MetricFlowCliRunner,
) -> None:
    run_and_check_cli_command(
        request=request,
        capsys=capsys,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command=query,
        args=[
            "--saved-query",
            "saved_query_with_cumulative_metric",
            "--order",
            "metric_time__day",
            "--start-time",
            "2020-01-01",
            "--end-time",
            "2020-01-01",
        ],
    )
