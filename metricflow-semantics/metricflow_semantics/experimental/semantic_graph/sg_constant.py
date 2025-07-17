from __future__ import annotations

import logging
from abc import ABC

from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class Constant(ABC):
    def __init__(self) -> None:
        raise RuntimeError(
            LazyFormat(
                "Constant class should not be instantiated",
                constant_class_name=self.__class__.__name__,
            )
        )


class ClusterName(Constant):
    TIME = "time"
    KEY = "key"
    GROUP_BY_METRIC = "group_by_metric"
    DIMENSION = "dimension"
    METRIC = "metric"
    TIME_DIMENSION = "time_dimension"

    @staticmethod
    def get_name_for_dsi_entity(entity_name: str, model_id: SemanticModelId) -> str:
        return f"{model_id}.{entity_name}"


class SemanticGraphConstant(Constant):
    MAX_METRIC_RECURSION_DEPTH = 1000
