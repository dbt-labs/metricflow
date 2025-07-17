from __future__ import annotations

import logging

from metricflow_semantics.experimental.semantic_graph.edges.edge_labels import MetricDefinitionLabel
from metricflow_semantics.experimental.semantic_graph.singleton_factory import SingletonFactory

logger = logging.getLogger(__name__)


class SemanticGraphLabelFactory(SingletonFactory):
    @classmethod
    def get_metric_definition_label(cls) -> MetricDefinitionLabel:
        return MetricDefinitionLabel()
