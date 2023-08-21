"""Nodes for building a dataflow plan."""

from __future__ import annotations

import logging
import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, List, Optional, Sequence, Tuple, Type, TypeVar, Union

import jinja2
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.protocols.metric import MetricTimeWindow
from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dag.id_generation import (
    DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX,
    DATAFLOW_NODE_COMBINE_METRICS_ID_PREFIX,
    DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX,
    DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX,
    DATAFLOW_NODE_JOIN_AGGREGATED_MEASURES_BY_GROUPBY_COLUMNS_PREFIX,
    DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX,
    DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX,
    DATAFLOW_NODE_JOIN_TO_TIME_SPINE_ID_PREFIX,
    DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX,
    DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX,
    DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX,
    DATAFLOW_NODE_SEMI_ADDITIVE_JOIN_ID_PREFIX,
    DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME,
    DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX,
    DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX,
)
from metricflow.dag.mf_dag import DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow.dataflow.builder.partitions import (
    PartitionDimensionJoinDescription,
    PartitionTimeDimensionJoinDescription,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.filters.time_constraint import TimeRangeConstraint
from metricflow.specs.specs import (
    InstanceSpecSet,
    LinklessEntitySpec,
    MetricInputMeasureSpec,
    MetricSpec,
    OrderBySpec,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.sql.sql_plan import SqlJoinType
from metricflow.visitor import Visitable, VisitorOutputT

logger = logging.getLogger(__name__)

NodeSelfT = TypeVar("NodeSelfT", bound="DataflowPlanNode")


class DataflowPlanNode(DagNode, Visitable, ABC):
    """A node in the graph representation of the dataflow.

    Each node in the graph performs an operation from the data that comes from the parent nodes, and the result is
    passed to the child nodes. The flow of data starts from source nodes, and ends at sink nodes.
    """

    def __init__(self, node_id: NodeId, parent_nodes: List[DataflowPlanNode]) -> None:
        """Constructor.

        Args:
            node_id: the ID for the node
            parent_nodes: data comes from the parent nodes.
        """
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    def parent_nodes(self) -> Sequence[DataflowPlanNode]:
        """Return the nodes where data for this node comes from."""
        return self._parent_nodes

    @abstractmethod
    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        raise NotImplementedError

    @abstractmethod
    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:
        """Returns true if this node the same functionality as the other node.

        In other words, this returns true if  all parameters (aside from parent_nodes) are the same.
        """
        raise NotImplementedError

    @abstractmethod
    def with_new_parents(self: NodeSelfT, new_parent_nodes: Sequence[BaseOutput]) -> NodeSelfT:
        """Creates a node with the same behavior as this node, but with a different set of parents.

        typing.Self would be useful here, but not available in Python 3.8.
        """
        raise NotImplementedError

    @property
    def node_type(self) -> Type:  # noqa: D
        # TODO: Remove.
        return self.__class__


class DataflowPlanNodeVisitor(Generic[VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a dataflow plan.

    Follows the visitor pattern: https://en.wikipedia.org/wiki/Visitor_pattern
    All visit* methods are similar and one exists for every type of node in the dataflow plan. The appropriate method
    will be called with DataflowPlanNode.accept().
    """

    @abstractmethod
    def visit_source_node(self, node: ReadSqlSourceNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_where_constraint_node(self, node: WhereConstraintNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_combine_metrics_node(self, node: CombineMetricsNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_metric_time_dimension_transform_node(  # noqa: D
        self, node: MetricTimeDimensionTransformNode
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> VisitorOutputT:  # noqa: D
        pass


class BaseOutput(DataflowPlanNode, ABC):
    """A node that outputs data in a "base" format.

    The base format is where the columns represent un-aggregated measures, dimensions, and entities.
    """

    pass


class ReadSqlSourceNode(BaseOutput):
    """A source node where data from an SQL table or SQL query is read and output."""

    def __init__(self, data_set: SqlDataSet) -> None:
        """Constructor.

        Args:
            data_set: dataset describing the SQL table / SQL query
        """
        self._dataset = data_set
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_source_node(self)

    @property
    def data_set(self) -> SqlDataSet:
        """Return the data set that this source represents and is passed to the child nodes."""
        return self._dataset

    def __str__(self) -> str:  # noqa: D
        return jinja2.Template(
            textwrap.dedent(
                """\
                <{{ class_name }} data_set={{ data_set }} />
                """
            )
        ).render(class_name=self.__class__.__name__, data_set=str(self.data_set))

    @property
    def description(self) -> str:  # noqa: D
        return f"""Read From {self.data_set}"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("data_set", self.data_set),
        ]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and other_node.data_set == self.data_set

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ReadSqlSourceNode:  # noqa: D
        assert len(new_parent_nodes) == 0
        return ReadSqlSourceNode(data_set=self.data_set)


@dataclass(frozen=True)
class ValidityWindowJoinDescription:
    """Encapsulates details about join constraints around validity windows."""

    window_start_dimension: TimeDimensionSpec
    window_end_dimension: TimeDimensionSpec


@dataclass(frozen=True)
class JoinDescription:
    """Describes how data from a node should be joined to data from another node."""

    join_node: BaseOutput
    join_on_entity: LinklessEntitySpec

    join_on_partition_dimensions: Tuple[PartitionDimensionJoinDescription, ...]
    join_on_partition_time_dimensions: Tuple[PartitionTimeDimensionJoinDescription, ...]

    validity_window: Optional[ValidityWindowJoinDescription] = None


class JoinToBaseOutputNode(BaseOutput):
    """A node that joins data from other nodes to a standard output node, one by one via entity."""

    def __init__(
        self,
        left_node: BaseOutput,
        join_targets: List[JoinDescription],
        node_id: Optional[NodeId] = None,
    ) -> None:
        """Constructor.

        Args:
            left_node: node with standard output
            join_targets: other sources that should be joined to this node.
            node_id: Override the node ID with this value
        """
        self._left_node = left_node
        self._join_targets = join_targets

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode] = [self._left_node]
        for join_target in self._join_targets:
            parent_nodes.append(join_target.join_node)
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_to_base_output_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Join Standard Outputs"""

    @property
    def left_node(self) -> BaseOutput:  # noqa: D
        return self._left_node

    @property
    def join_targets(self) -> List[JoinDescription]:  # noqa: D
        return self._join_targets

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty(f"join{i}_for_node_id_{join_description.join_node.node_id}", join_description)
            for i, join_description in enumerate(self._join_targets)
        ]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        if not isinstance(other_node, self.__class__) or len(self.join_targets) != len(other_node.join_targets):
            return False

        for i in range(len(self.join_targets)):
            if (
                self.join_targets[i].join_on_entity != other_node.join_targets[i].join_on_entity
                or self.join_targets[i].join_on_partition_dimensions
                != other_node.join_targets[i].join_on_partition_dimensions
                or self.join_targets[i].join_on_partition_time_dimensions
                != other_node.join_targets[i].join_on_partition_time_dimensions
                or self.join_targets[i].validity_window != other_node.join_targets[i].validity_window
            ):
                return False
        return True

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> JoinToBaseOutputNode:  # noqa: D
        assert len(new_parent_nodes) > 1
        new_left_node = new_parent_nodes[0]
        new_join_nodes = new_parent_nodes[1:]
        assert len(new_join_nodes) == len(self._join_targets)

        return JoinToBaseOutputNode(
            left_node=new_left_node,
            join_targets=[
                JoinDescription(
                    join_node=new_join_nodes[i],
                    join_on_entity=old_join_target.join_on_entity,
                    join_on_partition_dimensions=old_join_target.join_on_partition_dimensions,
                    join_on_partition_time_dimensions=old_join_target.join_on_partition_time_dimensions,
                    validity_window=old_join_target.validity_window,
                )
                for i, old_join_target in enumerate(self._join_targets)
            ],
        )


class JoinOverTimeRangeNode(BaseOutput):
    """A node that allows for cumulative metric computation by doing a self join across a cumulative date range."""

    def __init__(
        self,
        parent_node: BaseOutput,
        window: Optional[MetricTimeWindow],
        grain_to_date: Optional[TimeGranularity],
        node_id: Optional[NodeId] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node with standard output
            window: time window to join over
            grain_to_date: indicates time range should start from the beginning of this time granularity
            (eg month to day)
            node_id: Override the node ID with this value
            time_range_constraint: time range to aggregate over
        """
        if window and grain_to_date:
            raise RuntimeError(
                f"This node cannot be initialized with both window and grain_to_date set. This configuration should "
                f"have been prevented by model validation. window: {window}. grain_to_date: {grain_to_date}."
            )
        self._parent_node = parent_node
        self._grain_to_date = grain_to_date
        self._window = window
        self.time_range_constraint = time_range_constraint

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode] = [self._parent_node]
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_over_time_range_node(self)

    @property
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D
        return self._grain_to_date

    @property
    def description(self) -> str:  # noqa: D
        return """Join Self Over Time Range"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    @property
    def window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        return self._window

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.grain_to_date == self.grain_to_date
            and other_node.window == self.window
            and other_node.time_range_constraint == self.time_range_constraint
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> JoinOverTimeRangeNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return JoinOverTimeRangeNode(
            parent_node=new_parent_nodes[0],
            window=self.window,
            grain_to_date=self.grain_to_date,
            time_range_constraint=self.time_range_constraint,
        )


class AggregatedMeasuresOutput(BaseOutput, ABC):
    """A node that outputs data where the measures are aggregated.

    The measures are aggregated with respect to the present entities and dimensions.
    """

    pass


class AggregateMeasuresNode(AggregatedMeasuresOutput):
    """A node that aggregates the measures by the associated group by elements.

    In the event that one or more of the aggregated input measures has an alias assigned to it, any output query
    resulting from an operation on this node must apply the alias and transform the measure instances accordingly,
    otherwise this join could produce a query with two identically named measure columns with, e.g., different
    constraints applied to the measure.
    """

    def __init__(self, parent_node: BaseOutput, metric_input_measure_specs: Tuple[MetricInputMeasureSpec, ...]) -> None:
        """Initializer for AggregateMeasuresNode.

        The input measure specs are required for downstream nodes to be aware of any input measures with
        user-provided aliases, such as we might encounter with constrained and unconstrained versions of the
        same input measure.
        """
        self._parent_node = parent_node
        self._metric_input_measure_specs = metric_input_measure_specs

        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_aggregate_measures_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Aggregate Measures"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    @property
    def metric_input_measure_specs(self) -> Tuple[MetricInputMeasureSpec, ...]:
        """Iterable of specs for measure inputs to downstream metrics.

        Used for assigning aliases to output columns produced by aggregated measures.
        """
        return self._metric_input_measure_specs

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.metric_input_measure_specs == self.metric_input_measure_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> AggregateMeasuresNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return AggregateMeasuresNode(
            parent_node=new_parent_nodes[0],
            metric_input_measure_specs=self.metric_input_measure_specs,
        )


class JoinAggregatedMeasuresByGroupByColumnsNode(AggregatedMeasuresOutput):
    """A node that joins aggregated measures with group by elements.

    This is designed to link two separate semantic models with measures aggregated by the complete set of group by
    elements shared across both measures. Due to the way the DataflowPlan currently processes joins, this means
    each separate semantic model will be pre-aggregated, and this final join will be run across fully aggregated
    sets of input data. As such, all this requires is the list of aggregated measure outputs, since they can be
    transformed into a SqlDataSet containing the complete list of non-measure specs for joining.
    """

    def __init__(
        self,
        parent_nodes: Sequence[BaseOutput],
    ):
        """Constructor.

        Args:
            parent_nodes: sequence of nodes that output aggregated measures
        """
        if len(parent_nodes) < 2:
            raise ValueError(
                "This node is designed for joining 2 or more aggregated nodes together, but "
                f"we got {len(parent_nodes)}"
            )
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(parent_nodes))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_AGGREGATED_MEASURES_BY_GROUPBY_COLUMNS_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_aggregated_measures_by_groupby_columns_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Join Aggregated Measures with Standard Outputs"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("Join aggregated measure nodes: ", f"{[node.node_id for node in self.parent_nodes]}")
        ]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__)

    def with_new_parents(  # noqa: D
        self, new_parent_nodes: Sequence[BaseOutput]
    ) -> JoinAggregatedMeasuresByGroupByColumnsNode:
        return JoinAggregatedMeasuresByGroupByColumnsNode(
            parent_nodes=new_parent_nodes,
        )


class SemiAdditiveJoinNode(BaseOutput):
    """A node that performs a row filter by aggregating a given non-additive dimension.

    This is designed to filter a dataset down to singular non-additive time dimension values by aggregating
    the time dimension with MAX/MIN then joining back to the original dataset on that aggregated value.
    Additionally, an optional sequence of entities can be passed in to group by and join on during the filtering.

    For example, if we have a data set that includes "account_balances,user,date", we can build a
    "latest_account_balance_by_user" data set using this node by filtering the data set such that for each user,
    we get the MAX(date) and return the account_balance corresponding to that date.

    Data transformation example,
    | date       | account_balance | user |                             | date       | account_balance | user |
    |:-----------|-----------------|-----:|     entity_specs:       |:-----------|-----------------|-----:|
    | 2019-12-31 |            1000 |    u1|       - user                | 2020-01-03 |            2000 |    u1|
    | 2020-01-03 |            2000 |    u1| ->  time_dimension_spec: -> | 2020-01-12              1500 |    u2|
    | 2020-01-09 |            3000 |    u2|       - date                | 2020-01-12 |            1000 |    u3|
    | 2020-01-12 |            1500 |    u2|     agg_by_function:
    | 2020-01-12 |            1000 |    u3|       - MAX


    Similarly, if we don't provide any entity_specs, it would end up performing the aggregation filter only on the
    time_dimension_spec without any grouping by any entities.

    Data transformation example,
    | date       | account_balance | user |                             | date       | account_balance |
    |:-----------|-----------------|-----:|     entity_specs:       |:-----------|----------------:|
    | 2019-12-31 |            1000 |    u1|                             | 2020-01-12 |            2500 |
    | 2020-01-03 |            2000 |    u1| ->  time_dimension_spec: ->
    | 2020-01-09 |            3000 |    u2|       - date
    | 2020-01-12 |            1500 |    u2|     agg_by_function:
    | 2020-01-12 |            1000 |    u3|       - MAX


    Additionally, we can aggregate against 'windows' of a dataset. For example, if we group by a non_additive time dimension,
    we perform the MIN/MAX filtering on the granularity window provided in that group by instead of the full time range.

    Data transformation example,
    | date       | account_balance | user |                                  | date       | account_balance |
    |:-----------|-----------------|-----:|     entity_specs:            |:-----------|----------------:|
    | 2019-12-31 |            1500 |    u1|     time_dimension_spec:         | 2019-12-31 |            1500 |
    | 2020-01-03 |            2000 |    u1| ->    - date                  -> | 2020-01-07 |            3000 |
    | 2020-01-09 |            3000 |    u2|     agg_by_function:             | 2020-01-14 |            3250 |
    | 2020-01-12 |            1500 |    u2|       - MIN
    | 2020-01-14 |            1250 |    u3|     queried_time_dimension_spec:
    | 2020-01-14 |            2000 |    u2|       - date__week
    | 2020-01-15 |            4000 |    u1|
    """

    def __init__(
        self,
        parent_node: BaseOutput,
        entity_specs: Sequence[LinklessEntitySpec],
        time_dimension_spec: TimeDimensionSpec,
        agg_by_function: AggregationType,
        queried_time_dimension_spec: Optional[TimeDimensionSpec] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node with standard output
            entity_specs: the entities to group the join by
            time_dimension_spec: the time dimension used for row filtering via an aggregation
            agg_by_function: the aggregation function used on the time dimension
            queried_time_dimension_spec: The group by provided in the query used to build the windows we want to filter on.
        """
        self._parent_node = parent_node
        self._entity_specs = entity_specs
        self._time_dimension_spec = time_dimension_spec
        self._agg_by_function = agg_by_function
        self._queried_time_dimension_spec = queried_time_dimension_spec

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode] = [self._parent_node]
        super().__init__(node_id=self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_SEMI_ADDITIVE_JOIN_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_semi_additive_join_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"""Join on {self.agg_by_function.name}({self.time_dimension_spec.element_name}) and {[i.element_name for i in self.entity_specs]} grouping by {self.queried_time_dimension_spec.element_name if self.queried_time_dimension_spec else None}"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    @property
    def entity_specs(self) -> Sequence[LinklessEntitySpec]:  # noqa: D
        return self._entity_specs

    @property
    def time_dimension_spec(self) -> TimeDimensionSpec:  # noqa: D
        return self._time_dimension_spec

    @property
    def agg_by_function(self) -> AggregationType:  # noqa: D
        return self._agg_by_function

    @property
    def queried_time_dimension_spec(self) -> Optional[TimeDimensionSpec]:  # noqa: D
        return self._queried_time_dimension_spec

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        if not isinstance(other_node, self.__class__):
            return False

        return (
            isinstance(other_node, self.__class__)
            and other_node.entity_specs == self.entity_specs
            and other_node.time_dimension_spec == self.time_dimension_spec
            and other_node.agg_by_function == self.agg_by_function
            and other_node.queried_time_dimension_spec == self.queried_time_dimension_spec
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> SemiAdditiveJoinNode:  # noqa: D
        assert len(new_parent_nodes) == 1

        return SemiAdditiveJoinNode(
            parent_node=new_parent_nodes[0],
            entity_specs=self.entity_specs,
            time_dimension_spec=self.time_dimension_spec,
            agg_by_function=self.agg_by_function,
            queried_time_dimension_spec=self.queried_time_dimension_spec,
        )


class ComputedMetricsOutput(BaseOutput, ABC):
    """A node that outputs data that contains metrics computed from measures."""

    pass


class JoinToTimeSpineNode(BaseOutput, ABC):
    """Join parent dataset to time spine dataset."""

    def __init__(
        self,
        parent_node: BaseOutput,
        metric_time_dimension_specs: List[TimeDimensionSpec],
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        offset_window: Optional[MetricTimeWindow] = None,
        offset_to_grain: Optional[TimeGranularity] = None,
    ) -> None:  # noqa: D
        """Constructor.

        Args:
            parent_node: Node that returns desired dataset to join to time spine.
            metric_time_dimension_specs: Metric time dimensions requested in query. Used to determine granularities.
            time_range_constraint: Time range to constrain the time spine to.
            offset_window: Time window to offset the parent dataset by when joining to time spine.
            offset_to_grain: Granularity period to offset the parent dataset to when joining to time spine.

        Passing both offset_window and offset_to_grain not allowed.
        """
        assert not (
            offset_window and offset_to_grain
        ), "Can't set both offset_window and offset_to_grain when joining to time spine. Choose one or the other."
        self._parent_node = parent_node
        self._metric_time_dimension_specs = metric_time_dimension_specs
        self._offset_window = offset_window
        self._offset_to_grain = offset_to_grain
        self._time_range_constraint = time_range_constraint

        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_TO_TIME_SPINE_ID_PREFIX

    @property
    def metric_time_dimension_specs(self) -> List[TimeDimensionSpec]:  # noqa: D
        """Time dimension specs to use when creating time spine table."""
        return self._metric_time_dimension_specs

    @property
    def time_range_constraint(self) -> Optional[TimeRangeConstraint]:  # noqa: D
        """Time range constraint to apply when querying time spine table."""
        return self._time_range_constraint

    @property
    def offset_window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        """Time range constraint to apply when querying time spine table."""
        return self._offset_window

    @property
    def offset_to_grain(self) -> Optional[TimeGranularity]:  # noqa: D
        """Time range constraint to apply when querying time spine table."""
        return self._offset_to_grain

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_to_time_spine_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Join to Time Spine Dataset"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("time_range_constraint", self._time_range_constraint),
            DisplayedProperty("offset_window", self._offset_window),
            DisplayedProperty("offset_to_grain", self._offset_to_grain),
        ]

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.time_range_constraint == self.time_range_constraint
            and other_node.offset_window == self.offset_window
            and other_node.offset_to_grain == self.offset_to_grain
            and other_node.metric_time_dimension_specs == self.metric_time_dimension_specs
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> JoinToTimeSpineNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return JoinToTimeSpineNode(
            parent_node=new_parent_nodes[0],
            metric_time_dimension_specs=self.metric_time_dimension_specs,
            time_range_constraint=self.time_range_constraint,
            offset_window=self.offset_window,
            offset_to_grain=self.offset_to_grain,
        )


class ComputeMetricsNode(ComputedMetricsOutput):
    """A node that computes metrics from input measures. Dimensions / entities are passed through."""

    def __init__(self, parent_node: BaseOutput, metric_specs: List[MetricSpec]) -> None:  # noqa: D
        """Constructor.

        Args:
            parent_node: Node where data is coming from.
            metric_specs: The specs for the metrics that this should compute.
        """
        self._parent_node = parent_node
        self._metric_specs = metric_specs
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX

    @property
    def metric_specs(self) -> List[MetricSpec]:  # noqa: D
        """The metric instances that this node is supposed to compute and should have in the output."""
        return self._metric_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_compute_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Compute Metrics via Expressions"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("metric_spec", metric_spec) for metric_spec in self._metric_specs
        ]

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        if not isinstance(other_node, self.__class__):
            return False

        if other_node.metric_specs != self.metric_specs:
            return False

        return isinstance(other_node, self.__class__) and other_node.metric_specs == self.metric_specs

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ComputeMetricsNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return ComputeMetricsNode(
            parent_node=new_parent_nodes[0],
            metric_specs=self.metric_specs,
        )


class OrderByLimitNode(ComputedMetricsOutput):
    """A node that re-orders the input data with a limit."""

    def __init__(
        self,
        order_by_specs: List[OrderBySpec],
        parent_node: Union[BaseOutput, ComputedMetricsOutput],
        limit: Optional[int] = None,
    ) -> None:
        """Constructor.

        Args:
            order_by_specs: describes how to order the incoming data.
            limit: number of rows to limit.
            parent_node: self-explanatory.
        """
        self._order_by_specs = order_by_specs
        self._limit = limit
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX

    @property
    def order_by_specs(self) -> List[OrderBySpec]:
        """The elements that this node should order the input data."""
        return self._order_by_specs

    @property
    def limit(self) -> Optional[int]:
        """The number of rows to limit by."""
        return self._limit

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_order_by_limit_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Order By {[order_by_spec.instance_spec.qualified_name for order_by_spec in self._order_by_specs]}" + (
            f" Limit {self._limit}" if self.limit else ""
        )

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("order_by_spec", order_by_spec) for order_by_spec in self._order_by_specs]
            + [DisplayedProperty("limit", str(self.limit))]
        )

    @property
    def parent_node(self) -> Union[BaseOutput, ComputedMetricsOutput]:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.order_by_specs == self.order_by_specs
            and other_node.limit == self.limit
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> OrderByLimitNode:  # noqa: D
        assert len(new_parent_nodes) == 1

        return OrderByLimitNode(
            parent_node=new_parent_nodes[0],
            order_by_specs=self.order_by_specs,
            limit=self.limit,
        )


class MetricTimeDimensionTransformNode(BaseOutput):
    """A node transforms the input data set so that it contains the metric time dimension and relevant measures.

    The metric time dimension is used later to aggregate all measures in the data set.

    Input: a data set containing measures along with the associated aggregation time dimension.

    Output: a data set similar to the input data set, but includes the configured aggregation time dimension as the
    metric time dimension and only contains measures that are defined to use it.
    """

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        aggregation_time_dimension_reference: TimeDimensionReference,
    ) -> None:
        self._aggregation_time_dimension_reference = aggregation_time_dimension_reference
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME

    @property
    def aggregation_time_dimension_reference(self) -> TimeDimensionReference:
        """The time dimension that measures in the input should be aggregated to."""
        return self._aggregation_time_dimension_reference

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_metric_time_dimension_transform_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Metric Time Dimension '{self.aggregation_time_dimension_reference.element_name}'" ""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("aggregation_time_dimension", self.aggregation_time_dimension_reference.element_name)
        ]

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return (
            isinstance(other_node, self.__class__)
            and other_node.aggregation_time_dimension_reference == self.aggregation_time_dimension_reference
        )

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> MetricTimeDimensionTransformNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return MetricTimeDimensionTransformNode(
            parent_node=new_parent_nodes[0],
            aggregation_time_dimension_reference=self.aggregation_time_dimension_reference,
        )


class SinkNodeVisitor(Generic[VisitorOutputT], ABC):
    """Similar to DataflowPlanNodeVisitor, but only for sink nodes."""

    @abstractmethod
    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> VisitorOutputT:  # noqa: D
        pass


class SinkOutput(DataflowPlanNode, ABC):
    """A node where incoming data goes out of the graph."""

    @abstractmethod
    def accept_sink_node_visitor(self, visitor: SinkNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        pass

    @property
    @abstractmethod
    def parent_node(self) -> BaseOutput:  # noqa: D
        pass


class WriteToResultDataframeNode(SinkOutput):
    """A node where incoming data gets written to a dataframe."""

    def __init__(self, parent_node: BaseOutput) -> None:  # noqa: D
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_dataframe_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Write to Dataframe"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def accept_sink_node_visitor(self, visitor: SinkNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_dataframe_node(self)

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__)

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> WriteToResultDataframeNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return WriteToResultDataframeNode(parent_node=new_parent_nodes[0])


class WriteToResultTableNode(SinkOutput):
    """A node where incoming data gets written to a table."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        output_sql_table: SqlTable,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node that outputs the computed metrics.
            output_sql_table: the table where the computed metrics should be written to.
        """
        self._parent_node = parent_node
        self._output_sql_table = output_sql_table
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_table_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Write to Table"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def accept_sink_node_visitor(self, visitor: SinkNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_table_node(self)

    @property
    def output_sql_table(self) -> SqlTable:  # noqa: D
        return self._output_sql_table

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and other_node.output_sql_table == self.output_sql_table

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> WriteToResultTableNode:  # noqa: D
        return WriteToResultTableNode(
            parent_node=new_parent_nodes[0],
            output_sql_table=self.output_sql_table,
        )


class FilterElementsNode(BaseOutput):
    """Only passes the listed elements."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        include_specs: InstanceSpecSet,
        replace_description: Optional[str] = None,
    ) -> None:
        self._include_specs = include_specs
        self._replace_description = replace_description
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX

    @property
    def include_specs(self) -> InstanceSpecSet:
        """Returns the specs for the elements that it should pass."""
        return self._include_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_pass_elements_filter_node(self)

    @property
    def description(self) -> str:  # noqa: D
        if self._replace_description:
            return self._replace_description

        formatted_str = textwrap.indent(
            pformat_big_objects([x.qualified_name for x in self._include_specs.all_specs]), prefix="  "
        )
        return f"Pass Only Elements:\n{formatted_str}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        additional_properties = []
        if not self._replace_description:
            additional_properties = [
                DisplayedProperty("include_spec", include_spec) for include_spec in self._include_specs.all_specs
            ]
        return super().displayed_properties + additional_properties

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and other_node.include_specs == self.include_specs

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> FilterElementsNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return FilterElementsNode(
            parent_node=new_parent_nodes[0],
            include_specs=self.include_specs,
            replace_description=self._replace_description,
        )


class WhereConstraintNode(AggregatedMeasuresOutput):
    """Remove rows using a WHERE clause."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        where_constraint: WhereFilterSpec,
    ) -> None:
        self._where = where_constraint
        self.parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    @property
    def where(self) -> WhereFilterSpec:
        """Returns the specs for the elements that it should pass."""
        return self._where

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D
        # Can't put the where condition here as it can cause rendering issues when there are SQL execution parameters.
        # e.g. "Constrain Output with WHERE listing__country = :1"
        return "Constrain Output with WHERE"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("where_condition", self.where)]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and other_node.where == self.where

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> WhereConstraintNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return WhereConstraintNode(
            parent_node=new_parent_nodes[0],
            where_constraint=self.where,
        )


class CombineMetricsNode(ComputedMetricsOutput):
    """Combines metrics from different nodes into a single output."""

    def __init__(  # noqa: D
        self,
        parent_nodes: Sequence[Union[BaseOutput, ComputedMetricsOutput]],
        join_type: SqlJoinType = SqlJoinType.FULL_OUTER,
    ) -> None:
        self._join_type = join_type
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(parent_nodes))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_COMBINE_METRICS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_combine_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Combine Metrics"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:
        """Prints details about the join types and how the node will behave."""
        custom_properties = [DisplayedProperty("join type", self.join_type)]
        if self.join_type is SqlJoinType.FULL_OUTER:
            custom_properties.append(
                DisplayedProperty("de-duplication method", "post-join aggregation across all dimensions")
            )

        return super().displayed_properties + custom_properties

    @property
    def join_type(self) -> SqlJoinType:
        """The type of join used for combining metrics."""
        return self._join_type

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and other_node.join_type == self.join_type

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> CombineMetricsNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return CombineMetricsNode(
            parent_nodes=new_parent_nodes,
            join_type=self.join_type,
        )


class ConstrainTimeRangeNode(AggregatedMeasuresOutput, BaseOutput):
    """Constrains the time range of the input data set.

    For example, if the input data set had "sales by date", then this would restrict the data set so that it only
    includes sales for a specific range of dates.
    """

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput,
        time_range_constraint: TimeRangeConstraint,
    ) -> None:
        self._time_range_constraint = time_range_constraint
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_constrain_time_range_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return (
            f"Constrain Time Range to [{self.time_range_constraint.start_time.isoformat()}, "
            f"{self.time_range_constraint.end_time.isoformat()}]"
        )

    @property
    def time_range_constraint(self) -> TimeRangeConstraint:  # noqa: D
        return self._time_range_constraint

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("time_range_start", self.time_range_constraint.start_time.isoformat()),
            DisplayedProperty("time_range_end", self.time_range_constraint.end_time.isoformat()),
        ]

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D
        return isinstance(other_node, self.__class__) and self.time_range_constraint == other_node.time_range_constraint

    def with_new_parents(self, new_parent_nodes: Sequence[BaseOutput]) -> ConstrainTimeRangeNode:  # noqa: D
        assert len(new_parent_nodes) == 1
        return ConstrainTimeRangeNode(
            parent_node=new_parent_nodes[0],
            time_range_constraint=self.time_range_constraint,
        )


class DataflowPlan(MetricFlowDag[SinkOutput]):
    """Describes the flow of metric data as it goes from source nodes to sink nodes in the graph."""

    def __init__(self, plan_id: str, sink_output_nodes: List[SinkOutput]) -> None:  # noqa: D
        if len(sink_output_nodes) == 0:
            raise RuntimeError("Can't create a dataflow plan without sink node(s).")
        self._sink_output_nodes = sink_output_nodes
        super().__init__(dag_id=plan_id, sink_nodes=sink_output_nodes)

    @property
    def sink_output_nodes(self) -> List[SinkOutput]:  # noqa: D
        return self._sink_output_nodes

    @property
    def sink_output_node(self) -> SinkOutput:  # noqa: D
        assert len(self._sink_output_nodes) == 1, f"Only 1 sink node supported. Got: {self._sink_output_nodes}"
        return self._sink_output_nodes[0]
