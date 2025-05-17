from __future__ import annotations

import logging
from functools import cached_property

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from typing_extensions import override

from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.experimental.mf_graph.comparable import ComparisonKey
from metricflow_semantics.experimental.orderd_enum import OrderedEnum
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet
from metricflow_semantics.experimental.semantic_graph.nodes.semantic_graph_node import (
    SemanticGraphEdge,
    SemanticGraphNode,
)
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty

logger = logging.getLogger(__name__)


class AttributeEdgeType(OrderedEnum):
    ATTRIBUTE_TO_ENTITY = "attribute_to_entity"
    ENTITY_TO_ATTRIBUTE = "entity_to_attribute"

    @property
    def inverse(self) -> AttributeEdgeType:
        if self is AttributeEdgeType.ENTITY_TO_ATTRIBUTE:
            return AttributeEdgeType.ATTRIBUTE_TO_ENTITY
        elif self is AttributeEdgeType.ATTRIBUTE_TO_ENTITY:
            return AttributeEdgeType.ENTITY_TO_ATTRIBUTE
        else:
            assert_values_exhausted(self)


@fast_frozen_dataclass(order=False)
class EntityAttributeEdge(SemanticGraphEdge):
    attribute_edge_type: AttributeEdgeType
    linkable_element_properties: FrozenOrderedSet[LinkableElementProperty]

    @staticmethod
    def get_instance(
        tail_node: SemanticGraphNode,
        head_node: SemanticGraphNode,
        attribute_edge_type: AttributeEdgeType,
        weight: int = 1,
        linkable_element_properties: FrozenOrderedSet[LinkableElementProperty] = FrozenOrderedSet(),
    ) -> EntityAttributeEdge:
        return EntityAttributeEdge(
            _tail_node=tail_node,
            _head_node=head_node,
            attribute_edge_type=attribute_edge_type,
            linkable_element_properties=linkable_element_properties,
            _weight=0,
        )

    # @override
    # @property
    # def dot_attributes(self) -> DotEdgeAttributeSet:
    #     table_builder = GraphvizHtmlTableBuilder(table_cell_padding=10)
    #     with table_builder.new_row_builder() as row_builder:
    #         row_builder.add_column(GraphvizHtmlText(self.attribute_edge_type.value))
    #     label = "\n".join(table_builder.build())
    #     return super().dot_attributes.merge(DotEdgeAttributeSet.create(label=label))

    @override
    @cached_property
    def comparison_key(self) -> ComparisonKey:
        return (self._tail_node, self._head_node, self.attribute_edge_type, tuple(self.linkable_element_properties))

    @override
    @cached_property
    def inverse(self) -> EntityAttributeEdge:
        return EntityAttributeEdge.get_instance(
            tail_node=self._head_node,
            head_node=self._tail_node,
            linkable_element_properties=self.linkable_element_properties,
            attribute_edge_type=self.attribute_edge_type.inverse,
        )
