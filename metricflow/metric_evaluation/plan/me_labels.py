from __future__ import annotations

import logging

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.singleton import Singleton

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class TopLevelQueryLabel(MetricFlowGraphLabel, Singleton):
    """Label that identifies the `TopLevelQueryNode`."""

    @classmethod
    def get_instance(cls) -> TopLevelQueryLabel:  # noqa: D102
        return cls._get_instance()


@fast_frozen_dataclass(order=False)
class BaseMetricQueryLabel(MetricFlowGraphLabel, Singleton):
    """Label that identifies metric queries that do not have any sources (i.e. queries for non-derived metrics)."""

    @classmethod
    def get_instance(cls) -> BaseMetricQueryLabel:  # noqa: D102
        return cls._get_instance()
