from __future__ import annotations

import logging

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


@singleton_dataclass()
class MetricDefinitionLabel(MetricflowGraphLabel):
    pass
