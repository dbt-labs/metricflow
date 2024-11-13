from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.type_enums import AggregationType
from metricflow_semantics.dag.id_prefix import IdPrefix, StaticIdPrefix
from metricflow_semantics.dag.mf_dag import DisplayedProperty
from metricflow_semantics.specs.entity_spec import LinklessEntitySpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.visitor import VisitorOutputT

from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.dataflow_plan_visitor import DataflowPlanNodeVisitor


@dataclass(frozen=True, eq=False)
class SemiAdditiveJoinNode(DataflowPlanNode):
    """A node that performs a row filter by aggregating a given non-additive dimension.

    This is designed to filter a dataset down to singular non-additive time dimension values by aggregating
    the time dimension with MAX/MIN then joining back to the original dataset on that aggregated value.
    Additionally, an optional sequence of entities can be passed in to group by and join on during the filtering.

    For example, if we have a data set that includes "account_balances,user,date", we can build a
    "latest_account_balance_by_user" data set using this node by filtering the data set such that for each user,
    we get the MAX(date) and return the account_balance corresponding to that date.

    Data transformation example,
    | date       | account_balance | user |                             | date       | account_balance | user |
    |:-----------|-----------------|-----:|     entity_specs:           |:-----------|-----------------|-----:|
    | 2019-12-31 |            1000 |    u1|       - user                | 2020-01-03 |            2000 |    u1|
    | 2020-01-03 |            2000 |    u1| ->  time_dimension_spec: -> | 2020-01-12 |            1500 |    u2|
    | 2020-01-09 |            3000 |    u2|       - date                | 2020-01-12 |            1000 |    u3|
    | 2020-01-12 |            1500 |    u2|     agg_by_function:
    | 2020-01-12 |            1000 |    u3|       - MAX


    Similarly, if we don't provide any entity_specs, it would end up performing the aggregation filter only on the
    time_dimension_spec without any grouping by any entities.

    Data transformation example,
    | date       | account_balance | user |                             | date       | account_balance |
    |:-----------|-----------------|-----:|     entity_specs:           |:-----------|----------------:|
    | 2019-12-31 |            1000 |    u1|                             | 2020-01-12 |            2500 |
    | 2020-01-03 |            2000 |    u1| ->  time_dimension_spec: ->
    | 2020-01-09 |            3000 |    u2|       - date
    | 2020-01-12 |            1500 |    u2|     agg_by_function:
    | 2020-01-12 |            1000 |    u3|       - MAX


    Additionally, we can aggregate against 'windows' of a dataset. For example, if we group by a non_additive time dimension,
    we perform the MIN/MAX filtering on the granularity window provided in that group by instead of the full time range.

    Data transformation example,
    | date       | account_balance | user |                                  | date       | account_balance |
    |:-----------|-----------------|-----:|     entity_specs:                |:-----------|----------------:|
    | 2019-12-31 |            1500 |    u1|     time_dimension_spec:         | 2019-12-31 |            1500 |
    | 2020-01-03 |            2000 |    u1| ->    - date                  -> | 2020-01-07 |            3000 |
    | 2020-01-09 |            3000 |    u2|     agg_by_function:             | 2020-01-14 |            3250 |
    | 2020-01-12 |            1500 |    u2|       - MIN
    | 2020-01-14 |            1250 |    u3|     queried_time_dimension_spec:
    | 2020-01-14 |            2000 |    u2|       - date__week
    | 2020-01-15 |            4000 |    u1|

    Attributes:
        entity_specs: The entities to group the join by.
        time_dimension_spec: The time dimension used for row filtering via an aggregation.
        agg_by_function: The aggregation function used on the time dimension.
        queried_time_dimension_spec: The group by provided in the query used to build the windows we want to filter on.
    """

    entity_specs: Sequence[LinklessEntitySpec]
    time_dimension_spec: TimeDimensionSpec
    agg_by_function: AggregationType
    queried_time_dimension_spec: Optional[TimeDimensionSpec]

    def __post_init__(self) -> None:  # noqa: D105
        super().__post_init__()
        assert len(self.parent_nodes) == 1

    @staticmethod
    def create(  # noqa: D102
        parent_node: DataflowPlanNode,
        entity_specs: Sequence[LinklessEntitySpec],
        time_dimension_spec: TimeDimensionSpec,
        agg_by_function: AggregationType,
        queried_time_dimension_spec: Optional[TimeDimensionSpec] = None,
    ) -> SemiAdditiveJoinNode:
        return SemiAdditiveJoinNode(
            parent_nodes=(parent_node,),
            entity_specs=tuple(entity_specs),
            time_dimension_spec=time_dimension_spec,
            agg_by_function=agg_by_function,
            queried_time_dimension_spec=queried_time_dimension_spec,
        )

    @classmethod
    def id_prefix(cls) -> IdPrefix:  # noqa: D102
        return StaticIdPrefix.DATAFLOW_NODE_SEMI_ADDITIVE_JOIN_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_semi_additive_join_node(self)

    @property
    def description(self) -> str:  # noqa: D102
        return f"""Join on {self.agg_by_function.name}({self.time_dimension_spec.element_name}) and {[i.element_name for i in self.entity_specs]} grouping by {self.queried_time_dimension_spec.element_name if self.queried_time_dimension_spec else None}"""

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D102
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> Sequence[DisplayedProperty]:  # noqa: D102
        return super().displayed_properties

    def functionally_identical(self, other_node: DataflowPlanNode) -> bool:  # noqa: D102
        if not isinstance(other_node, self.__class__):
            return False

        return (
            other_node.entity_specs == self.entity_specs
            and other_node.time_dimension_spec == self.time_dimension_spec
            and other_node.agg_by_function == self.agg_by_function
            and other_node.queried_time_dimension_spec == self.queried_time_dimension_spec
        )

    def with_new_parents(self, new_parent_nodes: Sequence[DataflowPlanNode]) -> SemiAdditiveJoinNode:  # noqa: D102
        assert len(new_parent_nodes) == 1

        return SemiAdditiveJoinNode.create(
            parent_node=new_parent_nodes[0],
            entity_specs=self.entity_specs,
            time_dimension_spec=self.time_dimension_spec,
            agg_by_function=self.agg_by_function,
            queried_time_dimension_spec=self.queried_time_dimension_spec,
        )
