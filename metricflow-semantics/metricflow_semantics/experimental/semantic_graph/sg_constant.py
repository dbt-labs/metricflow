from __future__ import annotations

import logging
from abc import ABC
from typing import Final, final

from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId

logger = logging.getLogger(__name__)


@final
class ClusterNameFactory(ABC):
    """Creates cluster names for the semantic graph.

    Currently, the cluster name is only used to group related nodes in the semantic graph in graph visualizations.
    Cluster-name generation could be handled as methods on associated classes as well.
    """

    TIME: Final[str] = "time"
    KEY: Final[str] = "key"
    GROUP_BY_METRIC: Final[str] = "group_by_metric"
    DIMENSION: Final[str] = "dimension"
    METRIC: Final[str] = "metric"
    TIME_DIMENSION: Final[str] = "time_dimension"

    @staticmethod
    def get_name_for_configured_entity(entity_name: str, model_id: SemanticModelId) -> str:  # noqa: D102
        return f"{model_id}.{entity_name}"

    @staticmethod
    def get_name_for_model(model_id: SemanticModelId) -> str:  # noqa: D102
        """Return the cluster name that should be used to group nodes associated with a specific semantic model."""
        return model_id.model_name
