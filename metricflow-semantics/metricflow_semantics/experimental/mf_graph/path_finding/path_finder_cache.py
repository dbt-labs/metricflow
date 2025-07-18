from __future__ import annotations

import logging
from collections.abc import Set
from dataclasses import dataclass
from functools import cached_property
from typing import Generic

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.cache.mf_cache import CompositeCache, MetricflowCache, WeakValueDictCache
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.graph_id import MetricflowGraphId
from metricflow_semantics.experimental.mf_graph.mutable_graph import EdgeT, NodeT
from metricflow_semantics.experimental.mf_graph.path_finding.graph_path import MutablePathT
from metricflow_semantics.experimental.mf_graph.path_finding.path_finder_result import (
    FindCommonReachableTargetsResult,
)
from metricflow_semantics.experimental.mf_graph.path_finding.weight_function import WeightFunction
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class FindCommonReachableTargetsCacheKey(Generic[NodeT, EdgeT, MutablePathT]):
    graph_id: MetricflowGraphId
    source_nodes: FrozenOrderedSet[NodeT]
    candidate_target_nodes: Set[NodeT]
    weight_function: WeightFunction[NodeT, EdgeT, MutablePathT]
    max_path_weight: int


@dataclass
class PathFinderCache(Generic[NodeT, EdgeT, MutablePathT], CompositeCache):
    find_common_reachable_targets_cache: WeakValueDictCache[
        FindCommonReachableTargetsCacheKey, FindCommonReachableTargetsResult[NodeT]
    ] = WeakValueDictCache()

    @override
    @cached_property
    def caches(self) -> AnyLengthTuple[MetricflowCache]:
        return (self.find_common_reachable_targets_cache,)
