from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class MetricFlowGraphNodeDescriptor:
    """Descriptor for a node to allow for lookups by strings."""

    node_name: str
    # A name to cluster nodes in a graph. Currently only used for visualization.
    cluster_name: Optional[str]
