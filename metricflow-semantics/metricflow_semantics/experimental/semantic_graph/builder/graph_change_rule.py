from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_computation_path import (
    AttributeComputationPath,
)
from metricflow_semantics.experimental.semantic_graph.manifest_object_lookup import ManifestObjectLookup
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.experimental.semantic_graph.path_finding.path_finder import MetricflowGraphPathFinder
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph, SemanticGraph

logger = logging.getLogger(__name__)


@dataclass
class RuleInput:
    semantic_graph: MutableSemanticGraph


class SemanticGraphChangeType(OrderedEnum):
    ADD_NODE = "add_node"
    ADD_EDGE = "add_edge"


@fast_frozen_dataclass()
class SemanticGraphChangeSet:
    node_additions: FrozenOrderedSet[SemanticGraphNode]
    edge_additions: FrozenOrderedSet[SemanticGraphEdge]


# RuleInputT = TypeVar("RuleInputT", bound=RuleInput, covariant=True)


@fast_frozen_dataclass()
class SubgraphGeneratorArgumentSet:
    manifest_object_lookup: ManifestObjectLookup
    path_finder: MetricflowGraphPathFinder[SemanticGraphNode, SemanticGraphEdge, AttributeComputationPath]


class SemanticSubgraphGenerator(ABC):
    def __init__(self, argument_set: SubgraphGeneratorArgumentSet) -> None:
        self._manifest_object_lookup = argument_set.manifest_object_lookup
        self._path_finder = argument_set.path_finder

    @abstractmethod
    def generate_subgraph(self, current_graph: SemanticGraph) -> MutableSemanticGraph:
        raise NotImplementedError
