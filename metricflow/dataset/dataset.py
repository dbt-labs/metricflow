from __future__ import annotations

import logging

from metricflow.instances import (
    InstanceSet,
)

logger = logging.getLogger(__name__)


class DataSet:
    """Describes a set of data that a source node in the dataflow plan contains."""

    def __init__(self, instance_set: InstanceSet) -> None:  # noqa:
        self._instance_set = instance_set

    @property
    def instance_set(self) -> InstanceSet:
        """Returns the instances contained in this dataset."""
        return self._instance_set

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}()"
