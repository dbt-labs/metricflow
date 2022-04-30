from __future__ import annotations

import logging

from metricflow.instances import (
    InstanceSet,
)
from metricflow.specs import TimeDimensionReference, TimeDimensionSpec
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class DataSet:
    """Describes a set of data that a source node in the dataflow plan contains."""

    def __init__(self, instance_set: InstanceSet) -> None:  # noqa:
        self._instance_set = instance_set

    @property
    def instance_set(self) -> InstanceSet:
        """Returns the instances contained in this dataset."""
        return self._instance_set

    @staticmethod
    def plot_time_dimension_reference() -> TimeDimensionReference:
        """Returns a special reference that means 'the aggregation time dimension for all measures in the data set'

        Or to put in another way, if measures were plotted together on a graph, this is the name of the time dimension
        for the x-axis.
        """
        # TODO: Added underscores for ease of search + replace once we settle on a final name.
        return TimeDimensionReference(element_name="_ts")

    @staticmethod
    def plot_time_dimension_name() -> str:
        """See plot_time_dimension_reference(), but in name form for use in queries."""
        return DataSet.plot_time_dimension_reference().element_name

    @staticmethod
    def plot_time_dimension_spec(time_granularity: TimeGranularity) -> TimeDimensionSpec:
        """Spec that corresponds to DataSet.plot_time_dimension_reference"""
        return TimeDimensionSpec(
            element_name=DataSet.plot_time_dimension_reference().element_name,
            identifier_links=(),
            time_granularity=time_granularity,
        )

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}()"
