from __future__ import annotations

import logging

from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId

logger = logging.getLogger(__name__)


class ClusterName:
    TIME = "time"
    KEY = "key"
    GROUP_BY_METRIC = "group_by_metric"
    DIMENSION = "dimension"
    METRIC_ATTRIBUTE = "metric_attribute"
    TIME_DIMENSION = "time_dimension"

    @staticmethod
    def get_name_for_dsi_entity(entity_name: str, model_id: SemanticModelId) -> str:
        return f"{model_id}.{entity_name}"
