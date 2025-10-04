from __future__ import annotations

import dataclasses
import logging
import typing
from dataclasses import dataclass

from metricflow_semantics.experimental.ordered_set import MutableOrderedSet

if typing.TYPE_CHECKING:
    from metricflow_semantics.experimental.semantic_graph.nodes.attribute_nodes import (
        CategoricalDimensionAttributeNode,
        KeyAttributeNode,
        TimeAttributeNode,
    )
    from metricflow_semantics.experimental.semantic_graph.nodes.entity_nodes import (
        ComplexMetricNode,
        ConfiguredEntityNode,
        JoinedModelNode,
        LocalModelNode,
        MeasureNode,
        MetricTimeNode,
        SimpleMetricNode,
        TimeDimensionNode,
        TimeNode,
    )

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
    measure_nodes: MutableOrderedSet[MeasureNode] = dataclasses.field(default_factory=lambda: MutableOrderedSet())

    time_attribute_nodes: MutableOrderedSet[TimeAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    key_attribute_nodes: MutableOrderedSet[KeyAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
    categorical_dimension_attribute_nodes: MutableOrderedSet[CategoricalDimensionAttributeNode] = dataclasses.field(
        default_factory=lambda: MutableOrderedSet()
    )
