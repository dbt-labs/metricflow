from __future__ import annotations

import logging
import sys
import traceback
from abc import abstractmethod
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Generic, Iterator, TypeVar

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from typing_extensions import TextIO

from tests_metricflow.release_validation.process_interface import (
    WorkerInputMessageT,
    WorkerOutputMessageT,
    WorkerProcessInitArgument,
    WorkerTaskResultT,
    WorkerTaskT,
)

logger = logging.getLogger(__name__)

WorkerProcessInitArgumentT = TypeVar("WorkerProcessInitArgumentT", bound=WorkerProcessInitArgument)


class WorkerProcessMainFunction(
    Generic[
        WorkerProcessInitArgumentT,
        WorkerInputMessageT,
        WorkerOutputMessageT,
        WorkerTaskT,
        WorkerTaskResultT,
    ]
):
    """Encapsulates code that runs in the executor process."""

    def __init__(self, init_argument: WorkerProcessInitArgumentT) -> None:
        """Initializer.

        Args:
            init_argument: The parameters to use to start the executor process.
        """
        self._init_argument = init_argument
        # The `CliConfiguration` object is slow to create since it invokes the `dbt` command runner. Create it once
        # and store it between CLI calls for faster tests.
        self._log_level = logging.DEBUG
        # For debugging stdout / stderr / logging library. Set to true to print some example lines.
        self._output_logging_check_lines = False

    @abstractmethod
    def perform_work(self, task: WorkerTaskT) -> WorkerTaskResultT:
        raise NotImplementedError

    def main_loop(self) -> None:
        """The main method that runs in the executor process.

        Runs a loop that gets CLI commands to run from the input queue, then puts the result into the output queue.
        If `None` is passed in the input queue, this loop (and consequently the executor process) will exit.
        """
        log_file_path = self._init_argument.log_file_path
        input_queue = self._init_argument.input_queue
        output_queue = self._init_argument.output_queue

        # for key, value in self._init_argument.environment_variables or ():
        #     os.environ[key] = value

        root_logger = logging.getLogger()
        root_logger.setLevel(self._log_level)

        try:
            # Put the contents of stdout / stderr / logger in a separate file for easier debugging.
            with self._redirect_output_to_file(log_file_path) as output_file:
                while True:
                    input_message = input_queue.get()
                    logger.debug(LazyFormat("Received input message", input_message=input_message))
                    if input_message.stop_flag:
                        logger.debug("Exiting loop due to stop flag")
                        break
                    output_file.flush()

                    worker_task = input_message.worker_task

                    with worker_task.environment_configuration.execution_context():
                        output_message = self.perform_work(input_message)

                    logger.debug(LazyFormat("Sending output message", output_message=output_message))
                    output_queue.put(output_message)
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
                self._init_argument.output_message_class.create_worker_exit_message(
                    formatted_exception=formatted_exception,
                    executor_process_log_path=log_file_path,
                    log_file_contents=log_file_contents,
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
