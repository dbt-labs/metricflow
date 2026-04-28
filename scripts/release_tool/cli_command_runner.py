from __future__ import annotations

import logging
import subprocess
import sys
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CliCommandResult:
    """Result of a CLI command execution."""

    # Process exit code.
    returncode: int
    # Captured standard output when ``capture_output`` was ``True``, otherwise empty.
    stdout: bytes = b""


class CliCommandRunner(ABC):
    """Runs CLI commands used by the release tool."""

    @abstractmethod
    def run(
        self,
        command: tuple[str, ...],
        current_directory: Path,
        env: Mapping[str, str] | None = None,
        raise_exception_on_error: bool = True,
        capture_output: bool = False,
    ) -> CliCommandResult:
        """Run a CLI command and return its result."""
        raise NotImplementedError


class MetricFlowCliCommandRunner(CliCommandRunner):
    """CLI command runner backed by ``subprocess.run``."""

    def run(
        self,
        command: tuple[str, ...],
        current_directory: Path,
        env: Mapping[str, str] | None = None,
        raise_exception_on_error: bool = True,
        capture_output: bool = False,
    ) -> CliCommandResult:
        """Run a CLI command in a subprocess and return its result."""
        logger.info(f"In {str(current_directory)!r}: Running {command=}")
        sys.stdout.flush()
        sys.stderr.flush()
        result = subprocess.run(
            command,
            cwd=current_directory,
            check=raise_exception_on_error,
            capture_output=capture_output,
            env=env,
        )
        sys.stdout.flush()
        sys.stderr.flush()
        return CliCommandResult(returncode=result.returncode, stdout=result.stdout or b"")
