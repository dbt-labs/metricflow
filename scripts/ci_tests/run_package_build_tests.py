"""Script to call from a Github Action to test that packages build properly.

This script should run without installing additional Python packages.
"""
from __future__ import annotations

import argparse
import logging
import tempfile
import venv
from collections.abc import Sequence
from pathlib import Path

from scripts.mf_script_helper import MetricFlowScriptHelper

logger = logging.getLogger(__name__)


def _run_package_build_test(
    package_directory: Path, package_test_script: Path, optional_package_dependencies_to_install: Sequence[str] = ()
) -> None:
    """Run a test to verify that a package is built properly.

    Given the directory where the package is located, this will build the package using `hatch build` and install the
    created Python-wheel files into a clean virtual environment. Finally, the given test script will be run using the
    virtual environment.

    Args:
        package_directory: Root directory where the package is located.
        package_test_script: The path to the script that should be run.
        optional_package_dependencies_to_install: If the given package defines optional dependencies that can be
        installed, install these. e.g. for `dbt-metricflow[dbt-duckdb]`, specify `dbt-duckdb`.
    Returns: None
    Raises: Exception on test failure.
    """
    package_directory_str = package_directory.as_posix()
    package_test_script_str = package_test_script.as_posix()
    logger.info(f"Running package build test for {package_directory_str!r} using {package_test_script_str!r}")

    try:
        with tempfile.TemporaryDirectory() as temporary_directory_str:
            temporary_directory = Path(temporary_directory_str)
            venv_directory = temporary_directory.joinpath("venv")
            logger.info(f"Creating a new venv at {venv_directory.as_posix()!r}")

            venv.create(venv_directory, with_pip=True)
            pip_executable = Path(venv_directory, "bin/pip").as_posix()

            logger.info(f"Building package at {package_directory_str!r}")
            MetricFlowScriptHelper.run_command(["hatch", "clean"], working_directory=package_directory)
            MetricFlowScriptHelper.run_command(["hatch", "build"], working_directory=package_directory)

            logger.info("Installing package in venv using generated wheels")
            paths_to_wheels = _get_wheels_in_directory(package_directory.joinpath("dist"))
            if len(paths_to_wheels) != 1:
                raise RuntimeError(f"Expected exactly one wheel but got {paths_to_wheels}")

            path_to_wheel = paths_to_wheels[0]
            MetricFlowScriptHelper.run_command([pip_executable, "install", path_to_wheel.as_posix()])
            for optional_package_dependency in optional_package_dependencies_to_install:
                MetricFlowScriptHelper.run_command(
                    [pip_executable, "install", f"{path_to_wheel.as_posix()}[{optional_package_dependency}]"]
                )

            logger.info("Running test using venv")
            venv_activate = venv_directory.joinpath("bin", "activate").as_posix()
            MetricFlowScriptHelper.run_shell_command(
                # Using period instead of `source` for compatibility with `sh`.
                f"cd {temporary_directory_str} && . {venv_activate} && python {package_test_script_str}",
            )

            logger.info(f"Test passed {package_test_script_str!r}")
    except Exception as e:
        raise PackageBuildTestFailureException(
            f"Package build test failed for {package_directory_str!r} using {package_test_script_str!r}"
        ) from e


def _get_wheels_in_directory(directory: Path) -> Sequence[Path]:
    paths_to_wheels = []
    for path_item in directory.iterdir():
        if path_item.is_file() and path_item.suffix == ".whl":
            paths_to_wheels.append(path_item)
    return paths_to_wheels


class PackageBuildTestFailureException(Exception):  # noqa: D101
    pass


if __name__ == "__main__":
    MetricFlowScriptHelper.setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--metricflow-repo-directory", help="Path to the `metricflow` repo.", required=True)
    args = parser.parse_args()

    metricflow_repo_directory = Path(args.metricflow_repo_directory).resolve()

    logger.info(f"Using {metricflow_repo_directory=}")

    # Test building the `metricflow` package.
    _run_package_build_test(
        package_directory=metricflow_repo_directory,
        package_test_script=metricflow_repo_directory.joinpath("scripts/ci_tests/metricflow_package_test.py"),
    )

    # Test building the `metricflow-semantics` package.
    _run_package_build_test(
        package_directory=metricflow_repo_directory.joinpath("metricflow-semantics"),
        package_test_script=metricflow_repo_directory.joinpath("scripts/ci_tests/metricflow_semantics_package_test.py"),
    )

    # Test building the `dbt-metricflow` package.
    _run_package_build_test(
        package_directory=metricflow_repo_directory.joinpath("dbt-metricflow"),
        package_test_script=metricflow_repo_directory.joinpath("scripts/ci_tests/dbt_metricflow_package_test.py"),
        optional_package_dependencies_to_install=("dbt-duckdb",),
    )
