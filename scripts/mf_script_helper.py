from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from subprocess import CompletedProcess
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


class MetricFlowScriptHelper:
    """Helpful utility methods for using Python instead of Bash scripts.

    Methods in this class should not require any packages to be installed.
    """

    @staticmethod
    def setup_logging() -> None:
        """Configure logging to the console."""
        dev_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s"
        logging.basicConfig(level=logging.INFO, format=dev_format)

    @staticmethod
    def run_command(
        command: Sequence[str],
        working_directory: Optional[Path] = None,
        raise_exception_on_error: bool = True,
        capture_output: bool = False,
    ) -> CompletedProcess:
        """Thin wrapper around `subprocess.run` with more string types and log statements.

        Args:
            command: Command / arguments as a sequence of strings.
            working_directory: The working directory where the command should be run.
            raise_exception_on_error: If the command fails, raise an exception.
            capture_output: Same as the argument for `subprocess.run`.

        Returns: The `CompletedProcess` similar to `subprocess.run`
        """
        if working_directory is None:
            logger.info(f"Running {command=}")
        else:
            logger.info(f"In {str(working_directory)!r}: Running {command=}")
        return subprocess.run(
            command, cwd=working_directory, check=raise_exception_on_error, capture_output=capture_output
        )

    @staticmethod
    def run_shell_command(
        shell_command: str, working_directory: Optional[Path] = None, raise_exception_on_error: bool = True
    ) -> CompletedProcess:
        """Similar to `run_command` but using a command that is meant to be executed in the shell.

        Useful for handling file glob arguments.
        """
        if working_directory is None:
            logger.info(f"Running {shell_command=}")
        else:
            logger.info(f"In {str(working_directory)!r}: Running {shell_command=}")
        return subprocess.run(shell_command, shell=True, cwd=working_directory, check=raise_exception_on_error)
