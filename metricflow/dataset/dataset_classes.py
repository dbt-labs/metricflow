from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional, Sequence

from dbt_semantic_interfaces.references import SemanticModelReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords
from metricflow_semantics.instances import InstanceSet, TimeDimensionInstance
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


class DataSet(ABC):
    """Describes a set of data that a source node in the dataflow plan contains."""

    def __init__(self, instance_set: InstanceSet) -> None:  # noqa: D107
        self._instance_set = instance_set

    @property
    def instance_set(self) -> InstanceSet:
        """Returns the instances contained in this dataset."""
        return self._instance_set

    @property
    def metric_time_dimension_instances(self) -> Sequence[TimeDimensionInstance]:
        """Extracts all metric time TimeDimensionInstances from the InstanceSet associated with this DataSet."""
        return tuple(
            time_dimension_instance
            for time_dimension_instance in self.instance_set.time_dimension_instances
            if time_dimension_instance.spec.element_name == DataSet.metric_time_dimension_name()
        )

    @property
    def metric_time_instance_for_time_constraint(self) -> TimeDimensionInstance:
        """Returns the metric time instance that should be used for a time constraint if one was requested."""
        metric_time_instances = sorted(
            (
                instance
                for instance in self.metric_time_dimension_instances
                if not instance.spec.has_custom_grain and not instance.spec.date_part
            ),
            key=lambda x: x.spec.base_granularity_sort_key,
        )

        assert (
            len(metric_time_instances) > 0
        ), "No metric time dimensions with standard granularities found in the input data set for this node"

        return metric_time_instances[0]

    @staticmethod
    def metric_time_dimension_reference() -> TimeDimensionReference:
        """Returns a special reference that means 'the aggregation time dimension for all simple-metric inputs in the data set'.

        Or to put in another way, if associated metrics were plotted together on a graph, this is the name of
        the time dimension for the x-axis.
        """
        return TimeDimensionReference(element_name=MetricFlowReservedKeywords.METRIC_TIME.value)

    @staticmethod
    def metric_time_dimension_name() -> str:
        """See metric_time_dimension_reference(), but in name form for use in queries."""
        return DataSet.metric_time_dimension_reference().element_name

    @staticmethod
    def metric_time_dimension_spec(
        time_granularity: Optional[ExpandedTimeGranularity] = None, date_part: Optional[DatePart] = None
    ) -> TimeDimensionSpec:
        """Spec that corresponds to DataSet.metric_time_dimension_reference."""
        return TimeDimensionSpec(
            element_name=DataSet.metric_time_dimension_reference().element_name,
            entity_links=(),
            time_granularity=time_granularity,
            date_part=date_part,
        )

    @property
    @abstractmethod
    def semantic_model_reference(self) -> Optional[SemanticModelReference]:
        """If this data set was created from a semantic model, return the reference."""
        raise NotImplementedError

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}()"
