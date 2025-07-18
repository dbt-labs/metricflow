from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import Generic

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.mf_graph.mutable_graph import NodeT
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder_stat import PathFinderStat
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet, OrderedSet

logger = logging.getLogger(__name__)


@dataclass
class PathFinderResult(ABC):
    path_finder_stat_delta: PathFinderStat


@dataclass
class FindCommonReachableTargetsResult(Generic[NodeT], PathFinderResult):
    descendant_nodes: MutableOrderedSet[NodeT]
    reachable_target_nodes: MutableOrderedSet[NodeT]


@dataclass
class FindReachableTargetsResult(Generic[NodeT], PathFinderResult):
    reachable_target_nodes: OrderedSet[NodeT]
    collected_labels: OrderedSet[MetricflowGraphLabel]
    finish_iteration_index: int


@dataclass
class FindReachableTargetsSimpleResult(Generic[NodeT], PathFinderResult):
    reachable_targets: FrozenOrderedSet[NodeT]


# @dataclass
# class FindCommonReachableTargetsResult(Generic[NodeT]):
#     common_reachable_targets: MutableOrderedSet[NodeT]


@dataclass
class FindDescendantsResult(Generic[NodeT], PathFinderResult):
    descendant_nodes: MutableOrderedSet[NodeT]
