from __future__ import annotations

import logging
from abc import ABC
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from multiprocessing import Queue
from pathlib import Path
from typing import Generic, Iterator, Mapping, Optional, Tuple, Type, TypeVar

logger = logging.getLogger(__name__)


class WorkExecutionException(Exception):
    """Raised by the worker when there's an exception executing the given work."""

    pass


WorkerInputMessageT = TypeVar("WorkerInputMessageT", bound="WorkerInputMessage")
WorkerOutputMessageT = TypeVar("WorkerOutputMessageT", bound="WorkerOutputMessage")

WorkerTaskT = TypeVar("WorkerTaskT", bound="WorkerTask")
WorkerTaskResultT = TypeVar("WorkerTaskResultT", bound="WorkerTaskResult")


@dataclass(frozen=True)
class WorkerState:
    # Pass as the formatted version as it's unclear if there would be problems pickling some exception types.
    formatted_exception: Optional[str]
    executor_process_log_path: Path
    log_file_contents: str
    worker_exited: bool


T = TypeVar("T")


@dataclass(frozen=True)
class WorkerOutputMessage(Generic[WorkerTaskResultT]):
    """The output produced by the worker process given an input.

    This must be pickle-able as it's used with `multiprocessing`,
    """

    worker_state: Optional[WorkerState] = None
    task_result: Optional[WorkerTaskResultT] = None

    @classmethod
    def create_worker_exit_message(
        cls,
        formatted_exception: Optional[str],
        executor_process_log_path: Path,
        log_file_contents: str,
    ) -> WorkerOutputMessage[WorkerTaskResultT]:
        task_result: Optional[WorkerTaskResultT] = None
        return WorkerOutputMessage(
            worker_state=WorkerState(
                formatted_exception=formatted_exception,
                executor_process_log_path=executor_process_log_path,
                log_file_contents=log_file_contents,
                worker_exited=True,
            ),
            task_result=task_result,
        )


@dataclass(frozen=True)
class TaskEnvironmentConfiguration:
    working_directory_path: Path
    _environment_variables: Tuple[Tuple[str, str], ...]

    @cached_property
    def environment_variables(self) -> Mapping[str, str]:
        return {key: value for key, value in self._environment_variables}

    @contextmanager
    def execution_context(self) -> Iterator[None]:
        raise NotImplementedError


@dataclass(frozen=True)
class WorkerTask(ABC):
    environment_configuration: TaskEnvironmentConfiguration


@dataclass(frozen=True)
class WorkerTaskResult(ABC):
    exit_code: int
    task_log_path: Path


@dataclass(frozen=True)
class WorkerInputMessage(Generic[WorkerTaskT], ABC):
    """Encapsulates parameters required to run a specific CLI command."""

    # Flag to indicate to process to exit.
    stop_flag: bool
    worker_task: Optional[WorkerTaskT] = None


@dataclass(frozen=True)
class WorkerProcessInitArgument(Generic[WorkerInputMessageT, WorkerOutputMessageT]):
    """When a child process is started, this contains all variables related to how it was set up."""

    log_file_path: Path
    input_message_class: Type[WorkerInputMessageT]
    output_message_class: Type[WorkerOutputMessageT]
    input_queue: Queue[WorkerInputMessageT]
    output_queue: Queue[WorkerOutputMessageT]
