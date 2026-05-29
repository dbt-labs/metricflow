"""Script to call from a Github Action to test that packages build properly.

This script should run without installing additional Python packages.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from scripts.mf_script_helper import MetricFlowScriptHelper

logger = logging.getLogger(__name__)


def _install_hatch_environment_dependencies(hatch_project_directory: Path, hatch_environment_name: str) -> None:
    """Install hatch environment dependencies for caching in Github Actions."""
    logger.info(
        f"Installing dependencies for  `hatch` environment {hatch_environment_name!r} in {str(hatch_project_directory)!r}"
    )

    MetricFlowScriptHelper.run_command(
        ["hatch", "--verbose", "run", f"{hatch_environment_name}:true"],
        working_directory=hatch_project_directory,
    )


if __name__ == "__main__":
    MetricFlowScriptHelper.setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json-config-file",
        help="Path to text file containing a JSON object that describes the environments to install",
        required=True,
    )
    args = parser.parse_args()

    with open(args.json_config_file, "r") as fp:
        config_file_contents = fp.read()
        logger.info(f"config_file_contents:\n{config_file_contents}")
        environment_config_json = json.loads(config_file_contents)

    logger.info(f"Using {environment_config_json=}")

    # Install dependencies for `hatch` environments.
    for config in environment_config_json["configs"]:
        hatch_project_directory = config["hatch_project_directory"]
        hatch_environment_name = config["hatch_environment_name"]
        _install_hatch_environment_dependencies(Path(hatch_project_directory).resolve(), hatch_environment_name)
