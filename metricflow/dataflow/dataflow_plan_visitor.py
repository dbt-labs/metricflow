from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from typing import Generic

from metricflow_semantics.visitor import VisitorOutputT
from typing_extensions import override

if typing.TYPE_CHECKING:
    from metricflow.dataflow.dataflow_plan import DataflowPlanNode
    from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
    from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
    from metricflow.dataflow.nodes.alias_specs import AliasSpecsNode
    from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
    from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
    from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
    from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
    from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
    from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
    from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
    from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
    from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
    from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
    from metricflow.dataflow.nodes.min_max import MinMaxNode
    from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
    from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
    from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
    from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
    from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
    from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
    from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode


class DataflowPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a dataflow plan.

    Follows the visitor pattern: https://en.wikipedia.org/wiki/Visitor_pattern
    All visit* methods are similar and one exists for every type of node in the dataflow plan. The appropriate method
    will be called with DataflowPlanNode.accept().
    """

    @abstractmethod
    def visit_source_node(self, node: ReadSqlSourceNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_filter_elements_node(self, node: FilterElementsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_min_max_node(self, node: MinMaxNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError

    @abstractmethod
    def visit_alias_specs_node(self, node: AliasSpecsNode) -> VisitorOutputT:  # noqa: D102
        raise NotImplementedError


class DataflowPlanNodeVisitorWithDefaultHandler(DataflowPlanNodeVisitor[VisitorOutputT], Generic[VisitorOutputT]):
    """Similar to `DataflowPlanNodeVisitor`, but with an abstract default handler that gets called for each node.

    This could potentially replace `DataflowPlanNodeVisitor`.
    """

    @abstractmethod
    def _default_handler(self, node: DataflowPlanNode) -> VisitorOutputT:
        raise NotImplementedError

    @override
    def visit_source_node(self, node: ReadSqlSourceNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_filter_elements_node(self, node: FilterElementsNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_metric_time_dimension_transform_node(  # noqa: D102
        self, node: MetricTimeDimensionTransformNode
    ) -> VisitorOutputT:
        return self._default_handler(node)

    @override
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_min_max_node(self, node: MinMaxNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)

    @override
    def visit_alias_specs_node(self, node: AliasSpecsNode) -> VisitorOutputT:  # noqa: D102
        return self._default_handler(node)
