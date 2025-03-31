"""Tests MF CLI commands e.g. `mf query ...`.

These tests could be parameterized to reduce boilerplate.
Tests are marked as slow because each CLI command is run in a new process, and the dbt adapter needs to be
initialized.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.cli.cli_test_helpers import (
    create_tutorial_project_files,
    run_and_check_cli_command,
)
from tests_metricflow.cli.isolated_cli_command_interface import IsolatedCliCommandEnum
from tests_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner

logger = logging.getLogger(__name__)


@pytest.mark.slow
def test_missing_semantic_manifest(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests a case where a semantic manifest is not found."""
    with tempfile.TemporaryDirectory() as tmp_directory:
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory))
        # Skip running `dbt build` so that the artifact is not created.
        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )

        with cli_runner.running_context():
            run_and_check_cli_command(
                request=request,
                mf_test_configuration=mf_test_configuration,
                cli_runner=cli_runner,
                command_enum=IsolatedCliCommandEnum.MF_QUERY,
                args=["--metrics", "transactions"],
                expected_exit_code=1,
            )


@pytest.mark.slow
def test_invalid_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    cli_runner: IsolatedCliCommandRunner,
) -> None:
    """Tests a case where a semantic manifest is not found."""
    run_and_check_cli_command(
        request=request,
        mf_test_configuration=mf_test_configuration,
        cli_runner=cli_runner,
        command_enum=IsolatedCliCommandEnum.MF_QUERY,
        args=["--metrics", "invalid_metric_0,invalid_metric_1"],
        expected_exit_code=1,
    )
