from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


@singleton_dataclass()
class MetricflowGraphNodeDescriptor:
    """Descriptor for a node to allow for lookups by strings."""

    node_name: str
    # A name to cluster nodes in a graph. Currently only used for visualization.
    cluster_name: Optional[str]

    @staticmethod
    def get_instance(node_name: str, cluster_name: Optional[str] = None) -> MetricflowGraphNodeDescriptor:  # noqa: D102
        return MetricflowGraphNodeDescriptor(
            node_name=node_name,
            cluster_name=cluster_name,
        )
