from __future__ import annotations

import logging
from abc import ABC
from functools import cached_property
from typing import Optional

from typing_extensions import override

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.experimental.mf_graph.mf_graph import MetricflowGraphEdge, MetricflowGraphNode
from metricflow_semantics.experimental.semantic_graph.attribute_computation import (
    AttributeRecipeUpdate,
    AttributeRecipeUpdateSource,
)
from metricflow_semantics.mf_logging.pretty_formattable import MetricFlowPrettyFormattable
from metricflow_semantics.mf_logging.pretty_formatter import (
    PrettyFormatContext,
)

logger = logging.getLogger(__name__)


class SemanticGraphNode(MetricflowGraphNode, AttributeRecipeUpdateSource, MetricFlowPrettyFormattable, ABC):
    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return self.node_descriptor.node_name

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_update is not None:
            properties.extend(self.recipe_update.displayed_properties)
        properties.extend(DisplayedProperty("label", label) for label in self.labels)
        return tuple(properties)

    @cached_property
    def dunder_name_element(self) -> Optional[str]:
        return self.recipe_update.add_dunder_name_element


class SemanticGraphEdge(MetricflowGraphEdge[SemanticGraphNode], AttributeRecipeUpdateSource, ABC):
    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        formatter = format_context.formatter
        return formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "tail_node": self._tail_node,
                "head_node": self._head_node,
            },
        )

    @override
    @cached_property
    def displayed_properties(self) -> AnyLengthTuple[DisplayedProperty]:
        properties: list[DisplayedProperty] = list(super().displayed_properties)
        if self.recipe_update is not None:
            properties.extend(self.recipe_update.displayed_properties)

        return tuple(properties)

    @cached_property
    def recipe_update_for_path_addition(self) -> AttributeRecipeUpdate:
        return AttributeRecipeUpdate()
