from __future__ import annotations

import logging
from pathlib import Path

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from scripts.mf_script_helper import MetricFlowScriptHelper

logger = logging.getLogger(__name__)


class MetricFlowReleaseTool:
    def __init__(self, mf_repo_root: Path, release_version: str) -> None:
        self._release_version = release_version
        self._mf_repo_root = mf_repo_root
        self._dbt_mf_root = self._mf_repo_root.joinpath("dbt-metricflow")
        self._hatch_command = "/opt/homebrew/bin/hatch"

    def assert_clean_git_repo(self) -> None:
        result = MetricFlowScriptHelper.run_command(
            ["git", "status", "--porcelain"], working_directory=self._mf_repo_root, capture_output=True
        )
        stdout_lines = result.stdout.decode("utf-8").rstrip().split("\n")
        if tuple(stdout_lines) != ("",):
            raise RuntimeError(LazyFormat("Git state is not clean", mf_repo_root=self._mf_repo_root, stdout_lines=stdout_lines))

    def setup_main_branch(self) -> None:
        MetricFlowScriptHelper.run_command(["git", "switch", "main"], working_directory=self._mf_repo_root)
        MetricFlowScriptHelper.run_command(["git", "pull"], working_directory=self._mf_repo_root)

    def check_dbt_metricflow(self) -> None:
        requirements_file_path = self._dbt_mf_root.joinpath("requirements-files", "requirements-metricflow.txt")

        with open(requirements_file_path, "w") as f:
            f.write("metricflow @ {root:parent:uri}\n")

        MetricFlowScriptHelper.run_command([self._hatch_command, "env", "prune"], working_directory=self._dbt_mf_root, env={})
        MetricFlowScriptHelper.run_command(
            [self._hatch_command, "-v", "run", "dev-env:pytest", "-vv", "-n", "auto", "tests_dbt_metricflow"],
            working_directory=self._dbt_mf_root,
            env={},
        )
        MetricFlowScriptHelper.run_command(["git", "checkout", str(requirements_file_path)], working_directory=self._mf_repo_root)


if __name__ == "__main__":
    MetricFlowScriptHelper.setup_logging()
    mf_repo_root = Path("/Users/paul_work/repos/metricflow-release")
    release_tool = MetricFlowReleaseTool(mf_repo_root=mf_repo_root)
    logger.info(LazyFormat("Checking that the repo is clean", mf_repo_root=mf_repo_root))
    release_tool.assert_clean_git_repo()

    logger.info("Updating the `main` branch")
    release_tool.setup_main_branch()

    logger.info("Checking `dbt-metricflow` tests work using the current version of `metricflow`")
    release_tool.check_dbt_metricflow()
