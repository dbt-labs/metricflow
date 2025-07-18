from __future__ import annotations

import logging

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


@singleton_dataclass()
class CumulativeMeasureLabel(MetricflowGraphLabel):
    """Label for an edge from a cumulative metric node to a measure node.

    This label is helpful for addressing special cases with cumulative metrics (e.g. ability to query by date part).
    """

    @staticmethod
    def get_instance() -> CumulativeMeasureLabel:  # noqa: D102
        return CumulativeMeasureLabel()


@singleton_dataclass()
class DenyDatePartLabel(MetricflowGraphLabel):
    """Label for edges that, when added to a path, should not allow querying of the date part."""

    @staticmethod
    def get_instance() -> DenyDatePartLabel:
        return DenyDatePartLabel()


@singleton_dataclass()
class DenyEntityKeyQueryResolutionLabel(MetricflowGraphLabel):
    """Label for edges that, when added to a path, should not allow querying of only the entity-key attributes.

    e.g. for time-offset metrics, the successor edges have this label as those metrics must be queried with
    `metric_time` and it is not possible to query the metric only for entity-key attributes.
    """

    @staticmethod
    def get_instance() -> DenyEntityKeyQueryResolutionLabel:  # noqa: D102
        return DenyEntityKeyQueryResolutionLabel()


@singleton_dataclass()
class ConversionMeasureLabel(MetricflowGraphLabel):
    """Label for successor edges from a conversion metric to the conversion measure node."""

    @staticmethod
    def get_instance() -> ConversionMeasureLabel:  # noqa: D102
        return ConversionMeasureLabel()


@singleton_dataclass()
class DenyVisibleAttributesLabel(MetricflowGraphLabel):
    """Label for edges that, when added to a path, should not affect visibility of group-by items.

    e.g. for conversion metrics, the edge from the conversion-metric node to the conversion-measure node is given this
    label as the conversion measure does not affect the available group-by items for the metric.
    """

    @staticmethod
    def get_instance() -> DenyVisibleAttributesLabel:  # noqa: D102
        return DenyVisibleAttributesLabel()
