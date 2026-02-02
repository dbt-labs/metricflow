from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Mapping, Optional

from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DagId, DagNode, MetricFlowDag
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.toolkit.mf_type_aliases import AnyLengthTuple, MappingItemsTuple
from metricflow_semantics.toolkit.visitor import VisitorOutputT
from typing_extensions import override

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class MetricEvaluationNode(DagNode, ABC):
    @abstractmethod
    def accept(self, visitor: MetricEvaluationNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError


@dataclass(frozen=True, eq=False)
class MetricSubqueryNode(MetricEvaluationNode):
    computed_metric_specs: AnyLengthTuple[MetricSpec]
    passed_metric_specs: AnyLengthTuple[MetricSpec]
    parent_metric_spec_and_parent_node_items: MappingItemsTuple[MetricSpec, MetricEvaluationNode]

    @staticmethod
    def create(
        computed_metric_specs: Iterable[MetricSpec],
        passed_metric_specs: Iterable[MetricSpec],
        parent_metric_spec_to_parent_node: Mapping[MetricSpec, MetricEvaluationNode],
    ) -> MetricSubqueryNode:
        return MetricSubqueryNode(
            parent_nodes=tuple(parent_metric_spec_to_parent_node.values()),
            computed_metric_specs=tuple(computed_metric_specs),
            passed_metric_specs=tuple(passed_metric_specs),
            parent_metric_spec_and_parent_node_items=tuple(
                (parent_metric_spec, parent_node)
                for parent_metric_spec, parent_node in parent_metric_spec_to_parent_node.items()
            ),
        )

    @property
    @override
    def description(self) -> str:
        return "Metric Subquery"

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.METRIC_EVALUATION_NODE__METRIC_SUBQUERY

    @override
    def accept(self, visitor: MetricEvaluationNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_metric_subquery_node(self)

    @cached_property
    def parent_metric_spec_to_parent_node(self) -> Mapping[MetricSpec, MetricEvaluationNode]:
        return dict(self.parent_metric_spec_and_parent_node_items)


@dataclass(frozen=True, eq=False)
class TopLevelQueryNode(MetricEvaluationNode):
    @staticmethod
    def create(parent_nodes: Iterable[MetricSubqueryNode]) -> TopLevelQueryNode:
        return TopLevelQueryNode(parent_nodes=tuple(parent_nodes))

    @property
    @override
    def description(self) -> str:
        return "Top-Level Query Node"

    @classmethod
    @override
    def id_prefix(cls) -> IdPrefix:
        return StaticIdPrefix.METRIC_EVALUATION_NODE__QUERY

    @override
    def accept(self, visitor: MetricEvaluationNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        return visitor.visit_top_level_query_node(self)


class MetricEvaluationNodeVisitor(Generic[VisitorOutputT], ABC):
    @abstractmethod
    def visit_metric_subquery_node(self, node: MetricSubqueryNode) -> VisitorOutputT:
        raise NotImplementedError

    @abstractmethod
    def visit_top_level_query_node(self, node: TopLevelQueryNode) -> VisitorOutputT:
        raise NotImplementedError


class MetricEvaluationPlan(MetricFlowDag[MetricEvaluationNode]):
    def __init__(self, sink_node: TopLevelQueryNode, plan_id: Optional[DagId] = None):  # noqa: D107
        super().__init__(
            dag_id=plan_id or DagId.from_id_prefix(StaticIdPrefix.DATAFLOW_PLAN_PREFIX),
            sink_nodes=(sink_node,),
        )
