from __future__ import annotations

import concurrent.futures
import logging
import time

from metricflow_semantics.dag.id_prefix import DynamicIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)

_ID_PREFIX = "test_id_prefix"


def test_sequential_id_generator_in_threads() -> None:
    """Test generating IDs in multiple threads."""

    def _check_id_task(task_id: str, sleep_time: float) -> None:
        for i in range(10):
            time.sleep(sleep_time)
            generated_id = SequentialIdGenerator.create_next_id(DynamicIdPrefix(_ID_PREFIX))
            logger.debug(LazyFormat("Generated ID", task_id=task_id, i=i, generated_id=generated_id.str_value))

            assert generated_id.str_value == str(f"{_ID_PREFIX}_{i}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(_check_id_task, "task_0", 0.001),
            executor.submit(_check_id_task, "task_1", 0.0015),
        )
        # This will raise the exception that was raised in the task.
        for future in futures:
            future.result()
