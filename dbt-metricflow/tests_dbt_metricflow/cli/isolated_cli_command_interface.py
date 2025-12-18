from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from multiprocessing import Queue
from pathlib import Path
from typing import Optional, Tuple

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class IsolatedCliCommandEnum(Enum):
    """Enumerates the types of CLI commands supported in `IsolatedCliCommandRunner`."""

    # `dbt ...` commands
    DBT_BUILD = "dbt_build"
    # `mf ...` commands
    MF_DIMENSIONS = "mf_dimensions"
    MF_DIMENSION_VALUES = "mf_dimension_values"
    MF_ENTITIES = "mf_entities"
    MF_HEALTH_CHECKS = "mf_health_checks"
    MF_METRICS = "mf_metrics"
    MF_LIST = "mf_list"
    MF_QUERY = "mf_query"
    MF_SAVED_QUERIES = "mf_saved_queries"
    MF_TUTORIAL = "mf_tutorial"
    MF_VALIDATE_CONFIGS = "mf_validate_configs"


class IsolatedCliCommandException(Exception):
    """Raised when there is an error running the command."""

    pass


@dataclass(frozen=True)
class IsolatedCliCommandResult:
    """Contains the result of running a CLI command.

    This class is used instead of `click.testing.Result` as it needs to be pickled for use with `multiprocessing`, and
    there can be issues if the result object contains complex types.
    """

    exit_code: int
    output: str
    # Pass as the formatted version as it's unclear if there would be problems pickling some exception types.
    formatted_exception: Optional[str]
    executor_process_log_path: Path

    def raise_exception_on_failure(self) -> None:
        """Check the exit code and raise an exception with the appropriate context."""
        exception_message = str(
            LazyFormat(
                "Isolated CLI command failed",
                exit_code=self.exit_code,
                output=self.output,
                formatted_exception=self.formatted_exception,
                executor_process_log_path=str(self.executor_process_log_path),
            )
        )
        if self.exit_code == 0:
            return

        raise IsolatedCliCommandException(exception_message)


@dataclass(frozen=True)
class ExecutorProcessStartingParameterSet:
    """When a child process is started, this contains all variables related to how it was set up."""

    log_file_path: Path
    input_queue: Queue[Optional[CommandParameterSet]]
    output_queue: Queue[IsolatedCliCommandResult]
    dbt_profiles_path: Optional[Path]
    dbt_project_path: Optional[Path]
    environment_variables: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class CommandParameterSet:
    """Encapsulates parameters required to run a specific CLI command."""

    working_directory_path: Path
    command_enum: IsolatedCliCommandEnum
    command_args: Tuple[str, ...]
    environment_variables: Tuple[Tuple[str, str], ...]
