from __future__ import annotations

import logging
from typing import Final, final

from typing_extensions import override

from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


@final
class ClusterNameFactory:
    """Creates cluster names for the semantic graph.

    * Currently, the cluster name is only used to group related nodes in the semantic graph for visualizations.
    * Clusters / subgraphs could be later used to improve semantic-graph traversal.
    * Additional refinement is pending (e.g. switch to subgraphs, use `ClusterId`).
    """

    TIME: Final[str] = "time"
    KEY: Final[str] = "key"
    CONFIGURED_ENTITY: Final[str] = "configured_entity"
    DIMENSION: Final[str] = "dimension"
    METRIC: Final[str] = "metric"
    TIME_DIMENSION: Final[str] = "time_dimension"

    @override
    def __init__(self) -> None:
        """Can't override `__new__` as that results in a type-check error."""
        raise TypeError(LazyFormat("Class should not be instantiated", cls=self.__class__))

    @staticmethod
    def get_name_for_configured_entity(entity_name: str, model_id: SemanticModelId) -> str:  # noqa: D102
        return f"{model_id}.{entity_name}"

    @staticmethod
    def get_name_for_model(model_id: SemanticModelId) -> str:  # noqa: D102
        """Return the cluster name that should be used to group nodes associated with a specific semantic model."""
        return model_id.model_name
