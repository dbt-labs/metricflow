from __future__ import annotations

import logging
from collections.abc import Sequence

from dbt_semantic_interfaces.references import MeasureReference, MetricReference

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item
from metricflow_semantics.experimental.metricflow_exception import MetricflowAssertionError
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.annotated_spec_linkable_element_set import (
    AnnotatedSpecLinkableElementSet,
)
from metricflow_semantics.experimental.semantic_graph.attribute_resolution.attribute_resolver import AttributeResolver
from metricflow_semantics.experimental.semantic_graph.nodes.node_label import MeasureAttributeLabel
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.model.semantics.linkable_element_set_base import BaseLinkableElementSet
from metricflow_semantics.model.semantics.linkable_spec_resolver import LinkableSpecResolver

logger = logging.getLogger(__name__)


class SemanticGraphLinkableSpecResolver(LinkableSpecResolver):
    """An implementation of `LinkableSpecResolver` using the semantic graph."""

    def __init__(self, attribute_resolver: AttributeResolver) -> None:  # noqa: D107
        self._attribute_resolver = attribute_resolver
        self._semantic_graph = attribute_resolver.semantic_graph

    def get_linkable_element_set_for_measure(
        self, measure_reference: MeasureReference, element_filter: LinkableElementFilter
    ) -> BaseLinkableElementSet:
        matching_nodes = self._semantic_graph.nodes_with_label(
            MeasureAttributeLabel(measure_name=measure_reference.element_name)
        )
        source_node = mf_first_item(
            matching_nodes,
            lambda: MetricflowAssertionError(
                LazyFormat(
                    "Did not find exactly 1 node in the semantic graph for the given measure",
                    measure_reference=measure_reference,
                    matching_nodes=matching_nodes,
                )
            ),
        )
        annotated_specs = self._attribute_resolver.resolve_specs_from_source_node(source_node)
        return AnnotatedSpecLinkableElementSet(annotated_specs=annotated_specs)

    def get_linkable_elements_for_distinct_values_query(
        self, element_filter: LinkableElementFilter
    ) -> BaseLinkableElementSet:
        raise NotImplementedError

    def get_linkable_elements_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        element_filter: LinkableElementFilter = LinkableElementFilter(),
    ) -> BaseLinkableElementSet:
        raise NotImplementedError
