from __future__ import annotations

import logging

from metricflow.dataflow.builder.costing import DefaultCost, DefaultCostFunction
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    FilterElementsNode,
    JoinDescription,
    JoinToBaseOutputNode,
)
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    InstanceSpecSet,
    LinklessEntitySpec,
    MeasureSpec,
    MetricInputMeasureSpec,
)
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

logger = logging.getLogger(__name__)


def test_costing(consistent_id_object_repository: ConsistentIdObjectRepository) -> None:  # noqa: D
    bookings_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    listings_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]

    bookings_spec = MeasureSpec(
        element_name="bookings",
    )
    bookings_filtered = FilterElementsNode(
        parent_node=bookings_node,
        include_specs=InstanceSpecSet(
            measure_specs=(bookings_spec,),
            entity_specs=(
                EntitySpec(
                    element_name="listing",
                    entity_links=(),
                ),
            ),
        ),
    )

    listings_filtered = FilterElementsNode(
        parent_node=listings_node,
        include_specs=InstanceSpecSet(
            dimension_specs=(
                DimensionSpec(
                    element_name="country_latest",
                    entity_links=(),
                ),
            ),
            entity_specs=(
                EntitySpec(
                    element_name="listing",
                    entity_links=(),
                ),
            ),
        ),
    )

    join_node = JoinToBaseOutputNode(
        left_node=bookings_filtered,
        join_targets=[
            JoinDescription(
                join_node=listings_filtered,
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            )
        ],
    )

    bookings_aggregated = AggregateMeasuresNode(
        parent_node=join_node, metric_input_measure_specs=(MetricInputMeasureSpec(measure_spec=bookings_spec),)
    )

    cost_function = DefaultCostFunction()
    cost = cost_function.calculate_cost(bookings_aggregated)

    assert cost == DefaultCost(num_joins=1, num_aggregations=1)
