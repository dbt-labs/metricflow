from __future__ import annotations

import logging
from typing import Generic

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.path_finding.graph_path import EdgeT, MutableGraphPath, NodeT
from metricflow_semantics.experimental.orderd_enum import OrderedEnum

logger = logging.getLogger(__name__)


class WalkStopReason(OrderedEnum):
    VISIT_TARGET_NODE = "visit_target_node"
    VISIT_FINISHED_NODE = "visit_finished_node"


@fast_frozen_dataclass()
class WalkStopEvent(Generic[NodeT, EdgeT]):
    stop_reason: WalkStopReason
    current_path: MutableGraphPath[NodeT, EdgeT]
