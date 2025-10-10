from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.singleton import Singleton

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class GroupByAttributeLabel(MetricFlowGraphLabel, Singleton):
    """Label for any attribute node that can be used in the group-by argument of an MF query."""

    @classmethod
    def get_instance(cls) -> GroupByAttributeLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class ConfiguredEntityLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that correspond to entities configured in a semantic model."""

    @classmethod
    def get_instance(cls) -> ConfiguredEntityLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class TimeDimensionLabel(MetricFlowGraphLabel, Singleton):
    """Label for time dimension nodes."""

    @classmethod
    def get_instance(cls) -> TimeDimensionLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class TimeClusterLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that should be clustered together in the `time` section when visualizing the graph."""

    @classmethod
    def get_instance(cls) -> TimeClusterLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class MetricTimeLabel(MetricFlowGraphLabel, Singleton):
    """Label for the node that represents metric-time."""

    @classmethod
    def get_instance(cls) -> MetricTimeLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class MetricLabel(MetricFlowGraphLabel, Singleton):
    """Label for the node that corresponds to a configured metric in the semantic manifest.

    `metric_name = None` is a label applied to all metric nodes.
    """

    metric_name: Optional[str]

    @classmethod
    def get_instance(cls, metric_name: Optional[str] = None) -> MetricLabel:  # noqa: D102
        return cls._get_instance(metric_name=metric_name)


@fast_frozen_dataclass(order=False)
class SimpleMetricLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that represent a simple metric."""

    @classmethod
    def get_instance(cls) -> SimpleMetricLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class ComplexMetricLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that represent a complex metric."""

    @classmethod
    def get_instance(cls) -> ComplexMetricLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class JoinedModelLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that represent a joined semantic model.

    See `JoinedModelNode`.
    """

    @classmethod
    def get_instance(cls) -> JoinedModelLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class LocalModelLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that represent a local semantic model.

    See `LocalModelNode`.
    """

    @classmethod
    def get_instance(cls) -> LocalModelLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class KeyAttributeLabel(MetricFlowGraphLabel, Singleton):
    """Label for nodes that correspond to the entity-key attribute nodes."""

    @classmethod
    def get_instance(cls) -> KeyAttributeLabel:  # noqa: D102
        return cls._get_instance()
