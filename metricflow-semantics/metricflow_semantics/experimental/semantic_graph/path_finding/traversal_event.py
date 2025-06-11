from __future__ import annotations

import logging
from typing import Generic

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.path_finding.graph_path import EdgeT, MutableGraphPath, NodeT

logger = logging.getLogger(__name__)


class StopPathExplorationReason(OrderedEnum):
    VISIT_TARGET_NODE = "visit_target_node"
    VISIT_FINISHED_NODE = "visit_finished_node"


@fast_frozen_dataclass()
class StopPathExplorationEvent(Generic[NodeT, EdgeT]):
    stop_reason: StopPathExplorationReason
    current_path: MutableGraphPath[NodeT, EdgeT]
