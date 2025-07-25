from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


@singleton_dataclass()
class MeasureLabel(MetricflowGraphLabel):
    """Used to label measure nodes.

    `measure_name = None` is a label applied to all measure nodes.
    """

    measure_name: Optional[str]

    @staticmethod
    def get_instance(measure_name: Optional[str] = None) -> MeasureLabel:  # noqa: D102
        return MeasureLabel(measure_name=measure_name)


@singleton_dataclass()
class GroupByAttributeLabel(MetricflowGraphLabel):
    """Label for any attribute node that can be used in the group-by argument of an MF query."""

    @staticmethod
    def get_instance() -> GroupByAttributeLabel:  # noqa: D102
        return GroupByAttributeLabel()


@singleton_dataclass()
class ConfiguredEntityLabel(MetricflowGraphLabel):
    """Label for nodes that correspond to entities configured in a semantic model."""

    @staticmethod
    def get_instance() -> ConfiguredEntityLabel:  # noqa: D102
        return ConfiguredEntityLabel()


@singleton_dataclass()
class TimeDimensionLabel(MetricflowGraphLabel):
    """Label for time dimension nodes."""

    @staticmethod
    def get_instance() -> TimeDimensionLabel:  # noqa: D102
        return TimeDimensionLabel()


@singleton_dataclass()
class TimeClusterLabel(MetricflowGraphLabel):
    """Label for nodes that should be clustered together in the `time` section when visualizing the graph."""

    @staticmethod
    def get_instance() -> TimeClusterLabel:  # noqa: D102
        return TimeClusterLabel()


@singleton_dataclass()
class MetricTimeLabel(MetricflowGraphLabel):
    """Label for the node that represents metric-time."""

    @staticmethod
    def get_instance() -> MetricTimeLabel:  # noqa: D102
        return MetricTimeLabel()


@singleton_dataclass()
class MetricLabel(MetricflowGraphLabel):
    """Label for the node that corresponds to a configured metric in the semantic manifest.

    `metric_name = None` is a label applied to all metric nodes.
    """

    metric_name: Optional[str]

    @staticmethod
    def get_instance(metric_name: Optional[str] = None) -> MetricLabel:  # noqa: D102
        return MetricLabel(metric_name=metric_name)


@singleton_dataclass()
class BaseMetricLabel(MetricflowGraphLabel):
    """Label for nodes that represent a non-derived metric."""

    @staticmethod
    def get_instance() -> BaseMetricLabel:  # noqa: D102
        return BaseMetricLabel()


@singleton_dataclass()
class DerivedMetricLabel(MetricflowGraphLabel):
    """Label for nodes that represent a derived metric."""

    @staticmethod
    def get_instance() -> DerivedMetricLabel:  # noqa: D102
        return DerivedMetricLabel()


@singleton_dataclass()
class JoinedModelLabel(MetricflowGraphLabel):
    """Label for nodes that represent a joined semantic model.

    See `JoinedModelNode`.
    """

    @staticmethod
    def get_instance() -> JoinedModelLabel:  # noqa: D102
        return JoinedModelLabel()


@singleton_dataclass()
class LocalModelLabel(MetricflowGraphLabel):
    """Label for nodes that represent a local semantic model.

    See `LocalModelNode`.
    """

    @staticmethod
    def get_instance() -> LocalModelLabel:  # noqa: D102
        return LocalModelLabel()


@singleton_dataclass()
class KeyAttributeLabel(MetricflowGraphLabel):
    """Label for nodes that correspond to the entity-key attribute nodes."""

    @staticmethod
    def get_instance() -> KeyAttributeLabel:  # noqa: D102
        return KeyAttributeLabel()
