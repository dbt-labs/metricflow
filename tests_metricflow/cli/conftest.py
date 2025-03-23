from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Iterator

import pytest

from tests_metricflow.cli.cli_test_helpers import create_tutorial_project_files, run_dbt_build
from tests_metricflow.cli.isolated_cli_command_runner import IsolatedCliCommandRunner


@pytest.fixture(scope="session")
def cli_runner() -> Iterator[IsolatedCliCommandRunner]:  # noqa: D103
    with tempfile.TemporaryDirectory() as tmp_directory:
        tmp_directory_path = Path(tmp_directory)
        dbt_project_path = create_tutorial_project_files(Path(tmp_directory_path))
        cli_runner = IsolatedCliCommandRunner(
            dbt_profiles_path=dbt_project_path,
            dbt_project_path=dbt_project_path,
        )
        run_dbt_build(cli_runner)
        yield cli_runner
