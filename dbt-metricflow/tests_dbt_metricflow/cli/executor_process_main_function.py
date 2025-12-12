from __future__ import annotations

import logging
import os
import sys
import traceback
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Iterator, Optional

import click.testing
from dbt.cli.main import dbtRunner
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import TextIO

from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.main import (
    dimension_values,
    dimensions,
    entities,
    health_checks,
    list_command_group,
    metrics,
    query,
    saved_queries,
    tutorial,
    validate_configs,
)
from tests_dbt_metricflow.cli.isolated_cli_command_interface import (
    CommandParameterSet,
    ExecutorProcessStartingParameterSet,
    IsolatedCliCommandEnum,
    IsolatedCliCommandResult,
)

logger = logging.getLogger(__name__)


class ExecutorProcessMainFunction:
    """Encapsulates code that runs in the executor process."""

    def __init__(self, starting_parameter_set: ExecutorProcessStartingParameterSet) -> None:
        """Initializer.

        Args:
            starting_parameter_set: The parameters to use to start the executor process.
        """
        self._starting_parameter_set = starting_parameter_set
        self._mf_cli_runner = click.testing.CliRunner()
        # The `CliConfiguration` object is slow to create since it invokes the `dbt` command runner. Create it once
        # and store it between CLI calls for faster tests.
        self._mf_cli_cfg: Optional[CLIConfiguration] = None
        self._log_level = logging.DEBUG
        # For debugging stdout / stderr / logging library. Set to true to print some example lines.
        self._output_logging_check_lines = False

    def main_loop(self) -> None:
        """The main method that runs in the executor process.

        Runs a loop that gets CLI commands to run from the input queue, then puts the result into the output queue.
        If `None` is passed in the input queue, this loop (and consequently the executor process) will exit.
        """
        log_file_path = self._starting_parameter_set.log_file_path
        input_queue = self._starting_parameter_set.input_queue
        output_queue = self._starting_parameter_set.output_queue

        for key, value in self._starting_parameter_set.environment_variables or ():
            os.environ[key] = value

        root_logger = logging.getLogger()
        root_logger.setLevel(self._log_level)

        try:
            # Put the contents of stdout / stderr / logger in a separate file for easier debugging.
            with self._redirect_output_to_file(log_file_path) as output_file:
                while True:
                    input_work = input_queue.get()
                    logger.debug(LazyFormat("Got item from input queue", input_work=input_work))
                    if input_work is None:
                        logger.debug("Exiting loop due to `None` input.")
                        break
                    os.chdir(input_work.working_directory_path)
                    result = self._run_cli_command(input_work)
                    output_file.flush()
                    output_queue.put(result)
        except BaseException as exception:
            formatted_exception = (
                "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                if exception is not None
                else None
            )

            with open(log_file_path, "r") as log_file:
                log_file_contents = (
                    "Exception caught in executor process - this is unexpected as each command should capture them."
                    "\n\nLog contents:\n\n"
                ) + log_file.read()
            # After the parent process puts an item into the input queue, it waits to get an item from the output queue.
            # Consequently, a result always needs to be put in to the queue before exiting if an item from the input
            # queue was read to avoid a deadlock in the parent process.
            output_queue.put(
                IsolatedCliCommandResult(
                    exit_code=1,
                    output=log_file_contents,
                    formatted_exception=formatted_exception,
                    executor_process_log_path=self._starting_parameter_set.log_file_path,
                )
            )

    @contextmanager
    def _redirect_output_to_file(self, log_file_path: Path) -> Iterator[TextIO]:
        """Provides a context manager the redirects output, stderr, and logging output to the given file.

        Useful for debugging as without the log file, the output is invisible. This method is not thread safe due to
        mutation of global state (i.e. logging configuration, `redirect_*`).
        """
        with (
            open(log_file_path, "w") as log_file,
            redirect_stdout(log_file),
            redirect_stderr(log_file),
        ):
            # Setup logging. Note: a new process does not inherit the logging configuration of the parent process.
            logging_handler = logging.StreamHandler(log_file)
            logging_handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s")
            )
            root_logger = logging.getLogger()
            try:
                root_logger.addHandler(logging_handler)
                if self._output_logging_check_lines:
                    print("Capturing `output` into this file.")
                    print("Capturing `stderr` into this file.", file=sys.stderr)
                    logger.log(
                        level=self._log_level,
                        msg=LazyFormat("Capturing logging into this file.", log_level=self._log_level),
                    )
                yield log_file
            finally:
                root_logger.removeHandler(logging_handler)

    @property
    def _dbt_profiles_path(self) -> Optional[Path]:
        return self._starting_parameter_set.dbt_profiles_path

    @property
    def _dbt_project_path(self) -> Optional[Path]:
        return self._starting_parameter_set.dbt_project_path

    def _get_mf_cli_config(self) -> Optional[CLIConfiguration]:
        """Cache `CLIConfiguration` since it's slow to create."""
        if self._mf_cli_cfg is None:
            try:
                self._mf_cli_cfg = CLIConfiguration()
                self._mf_cli_cfg.setup(
                    dbt_profiles_path=self._dbt_profiles_path, dbt_project_path=self._dbt_project_path
                )
            except Exception:
                logger.exception("Got an exception while creating the CLI configuration.")

        return self._mf_cli_cfg

    def _run_cli_command(self, parameter_set: CommandParameterSet) -> IsolatedCliCommandResult:
        """Run either a `dbt` or `mf` CLI command."""
        if parameter_set.command_enum is IsolatedCliCommandEnum.DBT_BUILD:
            dbt_cli_runner = dbtRunner()

            args = ["build"]
            if self._dbt_profiles_path is not None:
                args.append("--profiles-dir")
                args.append(str(self._dbt_profiles_path))
            if self._dbt_project_path is not None:
                args.append("--project-dir")
                args.append(str(self._dbt_project_path))

            args.extend(parameter_set.command_args)
            dbt_build_result = dbt_cli_runner.invoke(args=args)

            exception = dbt_build_result.exception
            formatted_exception = (
                "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
                if exception is not None
                else None
            )
            return IsolatedCliCommandResult(
                exit_code=0 if dbt_build_result.success else 1,
                output="",
                formatted_exception=formatted_exception,
                executor_process_log_path=self._starting_parameter_set.log_file_path,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_DIMENSIONS:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=dimensions,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_DIMENSION_VALUES:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=dimension_values,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_ENTITIES:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=entities,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_HEALTH_CHECKS:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=health_checks,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_LIST:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=list_command_group,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_METRICS:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=metrics,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_SAVED_QUERIES:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=saved_queries,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_QUERY:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=query,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_TUTORIAL:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=tutorial,
            )
        elif parameter_set.command_enum is IsolatedCliCommandEnum.MF_VALIDATE_CONFIGS:
            return self._run_mf_cli_command(
                parameter_set=parameter_set,
                click_command=validate_configs,
            )
        else:
            assert_values_exhausted(parameter_set.command_enum)

    def _run_mf_cli_command(
        self,
        parameter_set: CommandParameterSet,
        click_command: click.Command,
    ) -> IsolatedCliCommandResult:
        """Runs a MF CLI command."""
        click_result = self._mf_cli_runner.invoke(
            click_command, obj=self._get_mf_cli_config(), args=parameter_set.command_args
        )
        exception = click_result.exception
        formatted_exception = (
            "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
            if exception is not None
            else None
        )
        return IsolatedCliCommandResult(
            exit_code=click_result.exit_code,
            output=click_result.output,
            formatted_exception=formatted_exception,
            executor_process_log_path=self._starting_parameter_set.log_file_path,
        )
