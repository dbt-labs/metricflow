from __future__ import annotations

import logging
import multiprocessing

# from tests_metricflow.release_validation.process_interface import (
#     WorkerInputMessage,
#     WorkerOutputMessage,
#     WorkerProcessInitArgument,
#     WorkerTask,
#     WorkerTaskResult,
# )
# from tests_metricflow.release_validation.process_main_function import WorkerProcessMainFunction
from concurrent.futures import Future, ProcessPoolExecutor

from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def square(x: int) -> int:
    return x * x


# @dataclass(frozen=True)
# class SquareTask(WorkerTask):
#     x: int
#
#
# class SquareWorkerInputMessage(WorkerInputMessage[SquareTask]):
#     pass
#
#
# @dataclass(frozen=True)
# class SquareTaskResult(WorkerTaskResult):
#     result: int
#
#
# @dataclass(frozen=True)
# class SquareWorkerOutputMessage(WorkerOutputMessage[SquareTaskResult]):
#     pass
#
#
# @dataclass(frozen=True)
# class SquareWorkerProcessInitArgument(WorkerProcessInitArgument):
#     pass
#
#
# class SquareWorkerProcessMainFunction(
#     WorkerProcessMainFunction[
#         SquareWorkerProcessInitArgument,
#         SquareWorkerInputMessage,
#         SquareWorkerOutputMessage,
#         SquareTask,
#         SquareTaskResult,
#     ]
# ):
#     def perform_work(self, task: SquareTask) -> SquareTaskResult:
#         return SquareTaskResult(
#             exit_code=0,
#             result=task.x * task.x,
#         )


def test_executor() -> None:
    futures: list[Future] = []
    with ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"), max_workers=5) as executor:
        for i in range(10):
            futures.append(executor.submit(square, i))

    for future in futures:
        logger.info(LazyFormat("Got result", result=future.result()))
