from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Set
from functools import cached_property
from typing import Generic, Iterable, Optional

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialId, SequentialIdGenerator
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.metric_spec import MetricModifier, MetricSpec
from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, OrderedSet
from metricflow_semantics.toolkit.dataclass_helpers import fast_frozen_dataclass
from metricflow_semantics.toolkit.mf_graph.comparable import ComparisonKey
from metricflow_semantics.toolkit.mf_graph.graph_labeling import MetricFlowGraphLabel
from metricflow_semantics.toolkit.mf_graph.mf_graph import MetricFlowGraphNode
from metricflow_semantics.toolkit.mf_graph.node_descriptor import MetricFlowGraphNodeDescriptor
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_formatter import PrettyFormatContext
from metricflow_semantics.toolkit.visitor import Visitable, VisitorOutputT
from typing_extensions import Self, override

from metricflow.metric_evaluation.plan.me_labels import BaseMetricQueryLabel, TopLevelQueryLabel
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement, MetricQueryPropertySet

logger = logging.getLogger(__name__)


@fast_frozen_dataclass(order=False)
class MetricQueryNode(MetricFlowGraphNode, Visitable, ABC):
    """Represents a query for a specific set of metrics.

    This maps to a SQL query. The inputs to the node represent the dependencies (e.g. the subqueries that compute the
    input metrics for a derived metric) and the outputs of the node represent the metrics that are computed or passed
    through from the dependencies.
    """

    # A unique node ID to identify the node in the plan.
    node_id: SequentialId
    # The query properties that are associated with the outputs of this node. This is later used to generate the
    # appropriate dataflow nodes. This is needed on a per-query basis as some modifiers for input metrics of a
    # derived metric (e.g. filters and time offsets) can require different query properties.
    query_properties: MetricQueryPropertySet

    @abstractmethod
    def pruned(self, allowed_specs: Set[MetricSpec]) -> Self:
        """Create a copy of this node where outputs that are not in the provided set are removed."""
        raise NotImplementedError

    @property
    @abstractmethod
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        """Return the specs for the metrics output by this node (both computed and passthrough)."""
        raise NotImplementedError

    @property
    @abstractmethod
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        """Similar to `output_metric_specs`, but as `MetricQueryElement`s."""
        raise NotImplementedError

    @abstractmethod
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Helps implement visitors for type-specific handling."""
        raise NotImplementedError

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return (self.node_id, self.query_properties)

    def _create_output_query_element(self, metric_spec: MetricSpec) -> MetricQueryElement:
        """Create a query element for one output metric from this node."""
        return MetricQueryElement.create(
            metric_spec=metric_spec,
            group_by_item_specs=self.query_properties.group_by_item_specs,
            predicate_pushdown_state=self.query_properties.predicate_pushdown_state,
        )


class MetricQueryNodeVisitor(Generic[VisitorOutputT], ABC):
    """A visitor interface for type-safe handling of different node types."""

    @abstractmethod
    def visit_simple_metrics_query_node(self, node: SimpleMetricsQueryNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_cumulative_metric_query_node(self, node: CumulativeMetricQueryNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_conversion_metric_query_node(self, node: ConversionMetricQueryNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_derived_metrics_query_node(self, node: DerivedMetricsQueryNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_top_level_query_node(self, node: TopLevelQueryNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


@fast_frozen_dataclass(order=False)
class BaseMetricQueryNode(MetricQueryNode, ABC):
    """Represents a query containing metrics that do not depend on other metric queries."""

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricFlowGraphLabel]:
        return super().labels.union((BaseMetricQueryLabel.get_instance(),))


@fast_frozen_dataclass(order=False)
class SimpleMetricsQueryNode(BaseMetricQueryNode):
    """Represents a query for simple metrics.

    This represents a SQL query that reads from a single semantic model. In many cases, it's possible to compute
    multiple simple metrics. However, modifiers such as filters can require separate queries.
    """

    model_id: SemanticModelId
    metric_specs: FrozenOrderedSet[MetricSpec]

    @staticmethod
    def create(
        model_id: SemanticModelId, metric_specs: Iterable[MetricSpec], query_properties: MetricQueryPropertySet
    ) -> SimpleMetricsQueryNode:
        """Create a node that computes one or more simple metrics from the same semantic model."""
        return SimpleMetricsQueryNode(
            model_id=model_id,
            node_id=SequentialIdGenerator.create_next_id(StaticIdPrefix.METRIC_EVALUATION_NODE__SIMPLE_METRICS_QUERY),
            metric_specs=FrozenOrderedSet.from_iterable(metric_specs),
            query_properties=query_properties,
        )

    def __post_init__(self) -> None:  # noqa: D105
        if not __debug__:
            return

        modifier_to_specs: defaultdict[MetricModifier, list[MetricSpec]] = defaultdict(list)
        for metric_spec in self.metric_specs:
            modifier_to_specs[metric_spec.metric_modifier].append(metric_spec)

        assert len(modifier_to_specs) == 1, LazyFormat(
            "All metric specs should map to exactly one modifier due to SQL query limitations (e.g. each "
            "unique filter requires a separate SQL query with the appropriate `WHERE` clause).",
            modifier_to_specs=modifier_to_specs,
        )

    @override
    def pruned(self, allowed_specs: Set[MetricSpec]) -> SimpleMetricsQueryNode:
        filtered_metric_specs = FrozenOrderedSet(
            (metric_spec for metric_spec in self.metric_specs if metric_spec in allowed_specs)
        )
        if len(filtered_metric_specs) == 0:
            raise RuntimeError(
                LazyFormat(
                    "Can't return a copy if all metric specs are filtered out",
                    metric_specs=self.metric_specs,
                    allowed_specs=allowed_specs,
                )
            )
        if self.metric_specs == filtered_metric_specs:
            return self

        return SimpleMetricsQueryNode.create(self.model_id, filtered_metric_specs, self.query_properties)

    @cached_property
    @override
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_id.str_value, cluster_name=None)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return super().comparison_key + (self.metric_specs,)

    @cached_property
    @override
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        return self.metric_specs

    @override
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_simple_metrics_query_node(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "node_name": self.node_descriptor.node_name,
                "model_id": self.model_id,
                "metric_specs": self.metric_specs,
                "query_properties": self.query_properties,
            },
        )

    @cached_property
    @override
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        return FrozenOrderedSet(self._create_output_query_element(metric_spec) for metric_spec in self.metric_specs)


@fast_frozen_dataclass(order=False)
class CumulativeMetricQueryNode(BaseMetricQueryNode):
    """Represents a query for a cumulative metric.

    Currently, each cumulative metric is computed in a separate SQL query for simplicity.
    """

    metric_spec: MetricSpec

    @staticmethod
    def create(metric_spec: MetricSpec, query_properties: MetricQueryPropertySet) -> CumulativeMetricQueryNode:
        """Create a node that computes one cumulative metric."""
        return CumulativeMetricQueryNode(
            node_id=SequentialIdGenerator.create_next_id(
                StaticIdPrefix.METRIC_EVALUATION_NODE__CUMULATIVE_METRIC_QUERY
            ),
            metric_spec=metric_spec,
            query_properties=query_properties,
        )

    @override
    def pruned(self, allowed_specs: Set[MetricSpec]) -> CumulativeMetricQueryNode:
        if self.metric_spec not in allowed_specs:
            raise RuntimeError(
                LazyFormat(
                    "Can't return a copy if all metric specs are filtered out",
                    metric_spec=self.metric_spec,
                    allowed_specs=allowed_specs,
                )
            )
        return self

    @cached_property
    @override
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_id.str_value, cluster_name=None)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return super().comparison_key + (self.metric_spec,)

    @cached_property
    @override
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        return FrozenOrderedSet((self.metric_spec,))

    @override
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_cumulative_metric_query_node(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "node_name": self.node_descriptor.node_name,
                "metric_spec": self.metric_spec,
                "query_properties": self.query_properties,
            },
        )

    @cached_property
    @override
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        return FrozenOrderedSet((self._create_output_query_element(self.metric_spec),))


@fast_frozen_dataclass(order=False)
class ConversionMetricQueryNode(BaseMetricQueryNode):
    """Represents a query for a conversion metric.

    Currently, each conversion metric is computed in a separate SQL query for simplicity.
    """

    metric_spec: MetricSpec

    @staticmethod
    def create(metric_spec: MetricSpec, query_properties: MetricQueryPropertySet) -> ConversionMetricQueryNode:
        """Create a node that computes one conversion metric."""
        return ConversionMetricQueryNode(
            node_id=SequentialIdGenerator.create_next_id(
                StaticIdPrefix.METRIC_EVALUATION_NODE__CONVERSION_METRIC_QUERY
            ),
            metric_spec=metric_spec,
            query_properties=query_properties,
        )

    @override
    def pruned(self, allowed_specs: Set[MetricSpec]) -> ConversionMetricQueryNode:
        if self.metric_spec not in allowed_specs:
            raise RuntimeError(
                LazyFormat(
                    "Can't return a copy if all metric specs are filtered out",
                    metric_spec=self.metric_spec,
                    allowed_specs=allowed_specs,
                )
            )
        return self

    @cached_property
    @override
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_id.str_value, cluster_name=None)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return super().comparison_key + (self.metric_spec,)

    @cached_property
    @override
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        return FrozenOrderedSet((self.metric_spec,))

    @override
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_conversion_metric_query_node(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "node_name": self.node_descriptor.node_name,
                "metric_spec": self.metric_spec,
                "query_properties": self.query_properties,
            },
        )

    @cached_property
    @override
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        return FrozenOrderedSet((self._create_output_query_element(self.metric_spec),))


@fast_frozen_dataclass(order=False)
class DerivedMetricsQueryNode(MetricQueryNode):
    """Represents a query for derived metric."""

    # The derived metrics that are computed in this query.
    computed_metric_specs: FrozenOrderedSet[MetricSpec]
    # The metrics that are passed through unchanged from one of the input queries.
    passthrough_metric_specs: FrozenOrderedSet[MetricSpec]

    def __post_init__(self) -> None:  # noqa: D105
        if not __debug__:
            return
        assert len(self.computed_metric_specs) > 0
        for passthrough_metric_spec in self.passthrough_metric_specs:
            assert passthrough_metric_spec.metric_modifier.alias is None, LazyFormat(
                "Passthrough metrics with an alias are not supported to simplify alias-collision handling",
                passthrough_metric_spec=passthrough_metric_spec,
            )

    @staticmethod
    def create(
        computed_metric_specs: Iterable[MetricSpec],
        passthrough_metric_specs: Iterable[MetricSpec],
        query_properties: MetricQueryPropertySet,
    ) -> DerivedMetricsQueryNode:
        """Create a node that computes selected derived metrics plus passthrough metrics."""
        return DerivedMetricsQueryNode(
            node_id=SequentialIdGenerator.create_next_id(StaticIdPrefix.METRIC_EVALUATION_NODE__DERIVED_METRIC_QUERY),
            computed_metric_specs=FrozenOrderedSet.from_iterable(computed_metric_specs),
            passthrough_metric_specs=FrozenOrderedSet.from_iterable(passthrough_metric_specs),
            query_properties=query_properties,
        )

    @override
    def pruned(self, allowed_specs: Set[MetricSpec]) -> DerivedMetricsQueryNode:
        filtered_computed_metric_specs = self.computed_metric_specs.intersection(allowed_specs)
        if not filtered_computed_metric_specs:
            raise RuntimeError(
                LazyFormat(
                    "Can't return a copy if all computed metric specs are filtered out",
                    computed_metric_specs=self.computed_metric_specs,
                    allowed_specs=allowed_specs,
                )
            )

        filtered_passthrough_metric_specs = FrozenOrderedSet(
            passthrough_metric_spec
            for passthrough_metric_spec in self.passthrough_metric_specs
            if passthrough_metric_spec in allowed_specs
        )

        if self.passthrough_metric_specs == filtered_passthrough_metric_specs:
            return self

        return DerivedMetricsQueryNode.create(
            computed_metric_specs=filtered_computed_metric_specs,
            passthrough_metric_specs=filtered_passthrough_metric_specs,
            query_properties=self.query_properties,
        )

    @cached_property
    @override
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_id.str_value, cluster_name=None)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return super().comparison_key + (
            self.computed_metric_specs,
            self.passthrough_metric_specs,
        )

    @cached_property
    @override
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        return FrozenOrderedSet(itertools.chain(self.computed_metric_specs, self.passthrough_metric_specs))

    @override
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_derived_metrics_query_node(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "node_name": self.node_descriptor.node_name,
                "computed_metric_specs": self.computed_metric_specs,
                "passthrough_metric_specs": self.passthrough_metric_specs,
                "query_properties": self.query_properties,
            },
        )

    @cached_property
    @override
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        return FrozenOrderedSet(
            (
                self._create_output_query_element(metric_spec)
                for metric_spec in itertools.chain(self.computed_metric_specs, self.passthrough_metric_specs)
            )
        )


@fast_frozen_dataclass(order=False)
class TopLevelQueryNode(MetricQueryNode):
    """Describes the metrics queried at the top-level.

    The top-level generally represents the metrics that are queried by the user. This node provides a single
    entry point for dependency traversal.
    """

    # The actual computation of the metrics is modeled through the dependencies, so this node can be modeled as only
    # passing through metrics computed in subqueries.
    passthrough_metric_specs: FrozenOrderedSet[MetricSpec]

    def __post_init__(self) -> None:  # noqa: D105
        assert len(self.passthrough_metric_specs) > 0, "A top-level query must have at least one metric"

    @staticmethod
    def create(
        passthrough_metric_specs: Iterable[MetricSpec], query_properties: MetricQueryPropertySet
    ) -> TopLevelQueryNode:
        """Create the top-level query node that exposes requested metrics."""
        return TopLevelQueryNode(
            node_id=SequentialIdGenerator.create_next_id(StaticIdPrefix.METRIC_EVALUATION_NODE__TOP_LEVEL_QUERY),
            passthrough_metric_specs=FrozenOrderedSet.from_iterable(passthrough_metric_specs),
            query_properties=query_properties,
        )

    @override
    def pruned(self, allowed_specs: Set[MetricSpec]) -> TopLevelQueryNode:
        filtered_passthrough_metric_specs = FrozenOrderedSet(
            (metric_spec for metric_spec in self.passthrough_metric_specs if metric_spec in allowed_specs)
        )
        if len(filtered_passthrough_metric_specs) == 0:
            raise RuntimeError(
                LazyFormat(
                    "Can't return a copy if all metric specs are filtered out",
                    passthrough_metric_specs=self.passthrough_metric_specs,
                    allowed_specs=allowed_specs,
                )
            )
        if self.passthrough_metric_specs == filtered_passthrough_metric_specs:
            return self

        return TopLevelQueryNode.create(
            passthrough_metric_specs=filtered_passthrough_metric_specs,
            query_properties=self.query_properties,
        )

    @cached_property
    @override
    def node_descriptor(self) -> MetricFlowGraphNodeDescriptor:
        return MetricFlowGraphNodeDescriptor(node_name=self.node_id.str_value, cluster_name=None)

    @cached_property
    @override
    def comparison_key(self) -> ComparisonKey:
        return super().comparison_key + (self.passthrough_metric_specs,)

    @cached_property
    @override
    def output_metric_specs(self) -> OrderedSet[MetricSpec]:
        return self.passthrough_metric_specs

    @cached_property
    @override
    def labels(self) -> OrderedSet[MetricFlowGraphLabel]:
        return super().labels.union((TopLevelQueryLabel.get_instance(),))

    @override
    def accept(self, visitor: MetricQueryNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_top_level_query_node(self)

    @override
    def pretty_format(self, format_context: PrettyFormatContext) -> Optional[str]:
        return format_context.formatter.pretty_format_object_by_parts(
            class_name=self.__class__.__name__,
            field_mapping={
                "node_name": self.node_descriptor.node_name,
                "passthrough_metric_specs": self.passthrough_metric_specs,
                "query_properties": self.query_properties,
            },
        )

    @cached_property
    @override
    def output_query_elements(self) -> OrderedSet[MetricQueryElement]:
        return FrozenOrderedSet(
            self._create_output_query_element(metric_spec) for metric_spec in self.passthrough_metric_specs
        )
