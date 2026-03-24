from __future__ import annotations

import logging

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.singleton import Singleton

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class CumulativeMetricLabel(MetricFlowGraphLabel, Singleton):
    """Label for an edge from a cumulative-metric node to a simple-metric node.

    This label is helpful for addressing special cases with cumulative metrics (e.g. ability to query by date part).
    """

    @classmethod
    def get_instance(cls) -> CumulativeMetricLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class DenyDatePartLabel(MetricFlowGraphLabel, Singleton):
    """Label for edges that, when added to a path, should not allow querying of the date part."""

    @classmethod
    def get_instance(cls) -> DenyDatePartLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class DenyEntityKeyQueryResolutionLabel(MetricFlowGraphLabel, Singleton):
    """Label for edges that, when added to a path, should not allow querying of only the entity-key attributes.

    e.g. for time-offset metrics, the successor edges have this label as those metrics must be queried with
    `metric_time` and it is not possible to query the metric only for entity-key attributes.
    """

    @classmethod
    def get_instance(cls) -> DenyEntityKeyQueryResolutionLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class ConversionMetricLabel(MetricFlowGraphLabel, Singleton):
    """Label for successor edges from a conversion metric to the input conversion metric node."""

    @classmethod
    def get_instance(cls) -> ConversionMetricLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class DenyVisibleAttributesLabel(MetricFlowGraphLabel, Singleton):
    """Label for edges that, when added to a path, should not affect visibility of group-by items.

    e.g. for conversion metrics, the edge from the conversion-metric node to the conversion-measure node is given this
    label as the conversion measure does not affect the available group-by items for the metric.
    """

    @classmethod
    def get_instance(cls) -> DenyVisibleAttributesLabel:  # noqa: D102
        return cls._get_instance()
