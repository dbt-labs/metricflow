from __future__ import annotations

import dataclasses
import logging
import typing
from dataclasses import dataclass
from typing import Iterable

from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet

if typing.TYPE_CHECKING:
    from metricflow_semantics.semantic_graph.nodes.attribute_nodes import (
        CategoricalDimensionAttributeNode,
        KeyAttributeNode,
        TimeAttributeNode,
    )
    from metricflow_semantics.semantic_graph.nodes.entity_nodes import (
        ComplexMetricNode,
        ConfiguredEntityNode,
        JoinedModelNode,
        LocalModelNode,
        MetricTimeNode,
        SimpleMetricNode,
        TimeDimensionNode,
        TimeNode,
    )
    from metricflow_semantics.semantic_graph.sg_interfaces import SemanticGraphNode

logger = logging.getLogger(__name__)


@dataclass
class SemanticGraphNodeTypedCollection:
    """A mutable collection of the nodes in a semantic graph, grouped by type."""

    configured_entity_nodes: MutableOrderedSet[ConfiguredEntityNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    joined_model_nodes: MutableOrderedSet[JoinedModelNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    local_model_nodes: MutableOrderedSet[LocalModelNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    time_dimension_nodes: MutableOrderedSet[TimeDimensionNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    metric_time_nodes: MutableOrderedSet[MetricTimeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    time_nodes: MutableOrderedSet[TimeNode] = dataclasses.field(default_factory=lambda: MutableOrderedSet())
    simple_metric_nodes: MutableOrderedSet[SimpleMetricNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    complex_metric_nodes: MutableOrderedSet[ComplexMetricNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )

    time_attribute_nodes: MutableOrderedSet[TimeAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    key_attribute_nodes: MutableOrderedSet[KeyAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    categorical_dimension_attribute_nodes: MutableOrderedSet[CategoricalDimensionAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )

    @staticmethod
    def create(nodes: Iterable[SemanticGraphNode]) -> SemanticGraphNodeTypedCollection:  # noqa: D102
        node_collection = SemanticGraphNodeTypedCollection()
        for node in nodes:
            node.add_to_typed_collection(node_collection)
        return node_collection
