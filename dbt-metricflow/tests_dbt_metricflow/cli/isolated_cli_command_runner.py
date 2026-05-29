from __future__ import annotations

import logging
import multiprocessing
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from multiprocessing.context import SpawnProcess
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator, Mapping, Optional, Sequence

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from tests_dbt_metricflow.cli.executor_process_main_function import ExecutorProcessMainFunction
from tests_dbt_metricflow.cli.isolated_cli_command_interface import (
    CommandParameterSet,
    ExecutorProcessStartingParameterSet,
    IsolatedCliCommandEnum,
    IsolatedCliCommandResult,
)

logger = logging.getLogger(__name__)


class IsolatedCliCommandRunner:
    """Helps to run a CLI command in a separate process for better isolation.

    A separate process is used because methods in the `dbt*` packages mutate global state e.g. state of the SQL engine
    adapters. `mf` CLI commands call methods in `dbt` packages, so this is useful for testing the `mf` CLI with
    different `dbt` projects.

    In the separate process, the command is run and the results are returned to the main process through a shared queue.

    `ProcessPoolExecutor` could be used here, but this uses separate queue and process instances for easier debugging.
    """

    def __init__(
        self,
        dbt_profiles_path: Optional[Path] = None,
        dbt_project_path: Optional[Path] = None,
        environment_variable_mapping: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Initializer.

        Args:
            dbt_profiles_path: The path to the directory containing dbt profiles.
            dbt_project_path: The path to the directory containing the dbt project.
            environment_variable_mapping: Environment variables to set in the process.
        """
        self._dbt_profiles_path = dbt_profiles_path
        self._dbt_project_path = dbt_project_path
        self._environment_variable_mapping = environment_variable_mapping

        # Use this context to use the `spawn` method to create new processes.
        # `spawn` helps avoid seg faults vs. `fork`.
        self._multiprocessing_context = multiprocessing.get_context("spawn")
        self._parent_pid = os.getpid()
        self._running_executor_process_context: Optional[_RunningExecutorProcessContext] = None
        self._delete_temporary_items = logger.getEffectiveLevel() == logging.DEBUG

    @contextmanager
    def running_context(self) -> Iterator[None]:
        """A context manager to help call `start()` / `shutdown()`.

        The context encapsulates the state where the executor is running.
        """
        self.start()
        try:
            yield None
        finally:
            self.shutdown()

    def start(self) -> None:
        """Start the executor process."""
        if self._running_executor_process_context is not None:
            logger.warning(
                f"`start()` was called already on the same {self.__class__.__name__} instance - it should only be "
                f"once. Handling as a no-op."
            )
            return

        temporary_file = NamedTemporaryFile(
            prefix="executor_process_",
            suffix=".log",
            mode="w",
            delete=False,
        )

        log_file_path = Path(temporary_file.name)
        input_queue = self._multiprocessing_context.Queue()
        output_queue = self._multiprocessing_context.Queue()

        if self._environment_variable_mapping is not None:
            environment_variables = tuple((key, value) for key, value in (self._environment_variable_mapping.items()))
        else:
            environment_variables = ()

        executor_process_starting_parameter_set = ExecutorProcessStartingParameterSet(
            log_file_path=log_file_path,
            input_queue=input_queue,
            output_queue=output_queue,
            dbt_profiles_path=self._dbt_profiles_path,
            dbt_project_path=self._dbt_project_path,
            environment_variables=environment_variables,
        )

        executor_process: multiprocessing.context.SpawnProcess = self._multiprocessing_context.Process(
            target=IsolatedCliCommandRunner._process_target,
            args=(executor_process_starting_parameter_set,),
        )
        executor_process.start()
        executor_pid = executor_process.pid

        if executor_pid is None:
            raise RuntimeError(f"Got unexpected {executor_pid=} after starting the executor process")

        self._running_executor_process_context = _RunningExecutorProcessContext(
            process=executor_process,
            executor_process_starting_parameter_set=executor_process_starting_parameter_set,
            executor_pid=executor_pid,
        )
        logger.debug(
            LazyFormat(
                "Started the executor process to execute CLI commands",
                running_executor_process_context=self._running_executor_process_context,
            )
        )

    def run_command(
        self,
        command_enum: IsolatedCliCommandEnum,
        command_args: Sequence[str],
        working_directory_path: Optional[Path] = None,
    ) -> IsolatedCliCommandResult:
        """Run a CLI command by sending it to the executor process.

        Args:
            command_enum: The command to run.
            command_args: The arguments to pass to the command.
            working_directory_path: If supplied, use this as the working directory. Otherwise, a temporary directory
            will be created.

        Returns: The result of the command.
        """
        if self._running_executor_process_context is None:
            raise RuntimeError("Executor process not started - `start()` should have been called first.")
        if not self._running_executor_process_context.is_alive:
            raise RuntimeError(
                "Executor process is not alive when it is expected to be - check logs to see why it is not."
            )

        start_time = time.perf_counter()

        delete_temporary_directory = True
        temporary_directory_path: Optional[Path] = None
        if working_directory_path is None:
            temporary_directory_path = Path(tempfile.mkdtemp())
            working_directory_path = temporary_directory_path
            delete_temporary_directory = True

        if logger.isEnabledFor(logging.DEBUG):
            delete_temporary_directory = False

        if self._environment_variable_mapping is not None:
            environment_variables = tuple((key, value) for key, value in (self._environment_variable_mapping.items()))
        else:
            environment_variables = ()

        try:
            command_parameter_set = CommandParameterSet(
                working_directory_path=working_directory_path,
                command_enum=command_enum,
                command_args=tuple(command_args),
                environment_variables=environment_variables,
            )
            logger.debug(LazyFormat("Put command in input queue", command_parameter_set=command_parameter_set))
            result = self._running_executor_process_context.send_command_and_get_result(command_parameter_set)
            logger.debug("Got result from output queue")

            runtime = f"{time.perf_counter() - start_time:.2f}s"
            if result.exit_code == 0:
                logger.debug(
                    LazyFormat(
                        "Successfully ran CLI command", command_parameter_set=command_parameter_set, runtime=runtime
                    )
                )
            else:
                logger.error(
                    LazyFormat(
                        "CLI command failed",
                        executor_process_log_file=str(
                            self._running_executor_process_context.executor_process_starting_parameter_set.log_file_path
                        ),
                        command_parameter_set=command_parameter_set,
                        result=result,
                        runtime=runtime,
                    )
                )
        finally:
            if delete_temporary_directory and temporary_directory_path is not None:
                logger.debug(
                    LazyFormat("Deleting temporary directory", temporary_directory=str(temporary_directory_path))
                )
                shutil.rmtree(temporary_directory_path)
            else:
                logger.debug(
                    LazyFormat(
                        "Leaving temporary directory untouched due to logging setting",
                        logger_effective_level=logging.getLevelName(logger.getEffectiveLevel()),
                        delete_temporary_directory=delete_temporary_directory,
                        temporary_directory_path=str(temporary_directory_path),
                    )
                )
        return result

    def shutdown(self) -> None:
        """When finished, call this method to stop the executor process if one was started."""
        if self._running_executor_process_context is None:
            logger.warning("Shutdown called, but the executor process was not set. Handling as a no-op")
            return

        logger.debug("Shutdown called, so sending stop message to the executor process")
        # `None` signals to the executor process to stop.
        self._running_executor_process_context.executor_process_starting_parameter_set.input_queue.put(None)
        logger.debug(
            LazyFormat(
                "Waiting for executor process to exit",
                parent_pid=self._parent_pid,
                executor_pid=self._running_executor_process_context.executor_pid,
            )
        )
        self._running_executor_process_context.join()
        logger.debug("Executor process finished")

    @staticmethod
    def _process_target(
        executor_process_starting: ExecutorProcessStartingParameterSet,
    ) -> None:
        """Target method that is run in a new process.

        * All arguments / work / results should be pickleable.
        * Work should be read from the input queue.
        * Results should be written to the output queue.
        """
        runner = ExecutorProcessMainFunction(executor_process_starting)
        runner.main_loop()


@dataclass(frozen=True)
class _RunningExecutorProcessContext:
    """Contains all variables related to how the executor process was set up / should be used."""

    process: SpawnProcess
    executor_process_starting_parameter_set: ExecutorProcessStartingParameterSet
    executor_pid: int

    @property
    def is_alive(self) -> bool:
        """Shortcut to see if the process is running."""
        return self.process.is_alive()

    def send_command_and_get_result(self, command_parameter_set: CommandParameterSet) -> IsolatedCliCommandResult:
        self.executor_process_starting_parameter_set.input_queue.put(command_parameter_set)
        result = self.executor_process_starting_parameter_set.output_queue.get()
        return result

    def join(self) -> None:
        self.process.join()
