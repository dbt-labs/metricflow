from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import Generic

from metricflow_semantics.experimental.mf_graph.mutable_graph import NodeT
from metricflow_semantics.experimental.ordered_set import MutableOrderedSet
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder_stat import PathFinderStat

logger = logging.getLogger(__name__)


@dataclass
class PathFinderResult(ABC):
    path_finder_stat: PathFinderStat


@dataclass
class FindReachableTargetsResult(Generic[NodeT], PathFinderResult):
    descendant_nodes: MutableOrderedSet[NodeT]
    reachable_targets: MutableOrderedSet[NodeT]


@dataclass
class FindCommonReachableTargetsResult(Generic[NodeT]):
    common_reachable_targets: MutableOrderedSet[NodeT]
