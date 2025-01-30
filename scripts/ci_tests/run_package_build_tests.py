"""Script to call from a Github Action to test that packages build properly.

This script should run without installing additional Python packages.
"""
from __future__ import annotations

import argparse
import logging
import tempfile
import venv
from pathlib import Path

from scripts.mf_script_helper import MetricFlowScriptHelper

logger = logging.getLogger(__name__)


def _run_package_build_test(package_directory: Path, package_test_script: Path) -> None:
    """Run a test to verify that a package is built properly.

    Given the directory where the package is located, this will build the package using `hatch build` and install the
    created Python-wheel files into a clean virtual environment. Finally, the given test script will be run using the
    virtual environment.

    Args:
        package_directory: Root directory where the package is located.
        package_test_script: The path to the script that should be run.

    Returns: None
    Raises: Exception on test failure.
    """
    logger.info(f"Running package build test for {str(package_directory)!r} using {str(package_test_script)!r}")
    try:
        with tempfile.TemporaryDirectory() as temporary_directory_str:
            temporary_directory = Path(temporary_directory_str)
            venv_directory = temporary_directory.joinpath("venv")
            logger.info(f"Creating venv at {str(venv_directory)!r}")

            venv.create(venv_directory, with_pip=True)
            pip_executable = Path(venv_directory, "bin/pip")
            python_executable = Path(venv_directory, "bin/python")

            logger.info(f"Building package at {str(package_directory)!r}")
            logger.info(f"Running package build test for {str(package_directory)!r} using {str(package_test_script)!r}")
            MetricFlowScriptHelper.run_command(["hatch", "build"], working_directory=package_directory)

            logger.info("Installing package using generated wheels")
            MetricFlowScriptHelper.run_shell_command(f'{pip_executable} install "{str(package_directory)}"/dist/*.whl')

            logger.info("Running test using installed package in venv")
            MetricFlowScriptHelper.run_command(
                [str(python_executable), str(package_test_script)], working_directory=temporary_directory
            )

            logger.info(f"Test passed {str(package_test_script)!r}")
    except Exception as e:
        raise PackageBuildTestFailureException(
            f"Package build test failed for {str(package_directory)!r} using {str(package_test_script)!r}"
        ) from e


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

    # Test building `metricflow-semantics` package.
    _run_package_build_test(
        package_directory=metricflow_repo_directory.joinpath("metricflow-semantics"),
        package_test_script=metricflow_repo_directory.joinpath("scripts/ci_tests/metricflow_semantics_package_test.py"),
    )

    # Add entry for `dbt-metricflow` once build issues are resolved.
