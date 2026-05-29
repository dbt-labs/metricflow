from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    DISPLAY_SNAPSHOTS_CLI_FLAG,
    OVERWRITE_SNAPSHOTS_CLI_FLAG,
)

from tests_dbt_metricflow import TESTS_DBT_METRICFLOW_DIRECTORY_ANCHOR
from tests_dbt_metricflow.cli.cli_test_helpers import create_tutorial_project_files, run_dbt_build
from tests_dbt_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner


@pytest.fixture(scope="session")
def cli_test_configuration(request: FixtureRequest) -> MetricFlowTestConfiguration:  # noqa: D103
    return MetricFlowTestConfiguration(
        sql_engine_url="N/A",
        sql_engine_password="N/A",
        mf_system_schema="N/A",
        mf_source_schema="N/A",
        display_snapshots=bool(request.config.getoption(DISPLAY_SNAPSHOTS_CLI_FLAG, default=False)),
        display_graphs=False,
        overwrite_snapshots=bool(request.config.getoption(OVERWRITE_SNAPSHOTS_CLI_FLAG, default=False)),
        use_persistent_source_schema=False,
        snapshot_directory=TESTS_DBT_METRICFLOW_DIRECTORY_ANCHOR.directory.joinpath("snapshots"),
        tests_directory=TESTS_DBT_METRICFLOW_DIRECTORY_ANCHOR.directory,
    )


@pytest.fixture(scope="session")
def cli_runner() -> Iterator[IsolatedCliCommandRunner]:  # noqa: D103
    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory_path))
        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )
        with cli_runner.running_context():
            run_dbt_build(cli_runner)
            yield cli_runner
