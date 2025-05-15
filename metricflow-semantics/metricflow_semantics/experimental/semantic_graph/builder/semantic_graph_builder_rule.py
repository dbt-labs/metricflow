from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar, Union, Sequence

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.semantic_graph.edges.semantic_graph_edge import SemanticGraphEdge
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import SemanticGraphNode
from metricflow_semantics.experimental.semantic_graph.semantic_graph import MutableSemanticGraph
from metricflow_semantics.experimental.semantic_graph.semantic_model_lookup import ManifestObjectLookup

logger = logging.getLogger(__name__)


@dataclass
class RuleInput:
    semantic_graph: MutableSemanticGraph


class GraphChangeType(OrderedEnum):
    ADD_NODE = "add_node"
    ADD_EDGE = "add_edge"


@fast_frozen_dataclass()
class GraphChange:
    change_type: GraphChangeType
    target_item: Union[SemanticGraphNode, SemanticGraphEdge]


# RuleInputT = TypeVar("RuleInputT", bound=RuleInput, covariant=True)


class SemanticGraphBuilderRule(ABC):
    def __init__(self, manifest_object_lookup: ManifestObjectLookup) -> None:
        self._manifest_object_lookup = manifest_object_lookup

    @abstractmethod
    def update_graph(self, semantic_graph: MutableSemanticGraph) -> Sequence[GraphChange]:
        raise NotImplementedError
