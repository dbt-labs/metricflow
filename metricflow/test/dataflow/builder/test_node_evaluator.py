import logging
from typing import Sequence

import pytest

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    NodeEvaluatorForLinkableInstances,
    LinkableInstanceSatisfiabilityEvaluation,
    JoinLinkableInstancesRecipe,
)
from metricflow.dataflow.builder.partitions import PartitionTimeDimensionJoinDescription
from metricflow.model.semantic_model import SemanticModel
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.node_processor import PreDimensionJoinNodeProcessor
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.specs import (
    DimensionSpec,
    IdentifierSpec,
    LinklessIdentifierSpec,
    TimeDimensionSpec,
    LinkableInstanceSpec,
)
from metricflow.time.time_granularity import TimeGranularity
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

logger = logging.getLogger(__name__)


@pytest.fixture
def node_evaluator(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
    dataflow_plan_builder: DataflowPlanBuilder[DataSourceDataSet],
    time_spine_source: TimeSpineSource,
) -> NodeEvaluatorForLinkableInstances:  # noqa: D
    """Return a node evaluator using the nodes in data_source_name_to_nodes"""
    node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )

    source_nodes = tuple(consistent_id_object_repository.simple_model_read_nodes.values())

    return NodeEvaluatorForLinkableInstances(
        data_source_semantics=simple_semantic_model.data_source_semantics,
        # Use all nodes in the simple model as candidates for joins.
        nodes_available_for_joins=source_nodes,
        node_data_set_resolver=node_data_set_resolver,
    )


def make_multihop_node_evaluator(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    multihop_semantic_model: SemanticModel,
    desired_linkable_specs: Sequence[LinkableInstanceSpec],
    time_spine_source: TimeSpineSource,
) -> NodeEvaluatorForLinkableInstances:  # noqa: D
    """Return a node evaluator using the nodes in multihop_data_source_name_to_nodes"""
    node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DefaultColumnAssociationResolver(multihop_semantic_model),
        semantic_model=multihop_semantic_model,
        time_spine_source=time_spine_source,
    )

    source_nodes = tuple(consistent_id_object_repository.multihop_model_read_nodes.values())
    node_processor = PreDimensionJoinNodeProcessor(
        data_source_semantics=multihop_semantic_model.data_source_semantics,
        node_data_set_resolver=node_data_set_resolver,
    )

    nodes_available_for_joins = node_processor.remove_unnecessary_nodes(
        desired_linkable_specs=desired_linkable_specs,
        nodes=source_nodes,
        primary_time_dimension_reference=multihop_semantic_model.data_source_semantics.primary_time_dimension_reference,
    )

    nodes_available_for_joins = node_processor.add_multi_hop_joins(
        desired_linkable_specs=desired_linkable_specs, nodes=nodes_available_for_joins
    )

    return NodeEvaluatorForLinkableInstances(
        data_source_semantics=multihop_semantic_model.data_source_semantics,
        nodes_available_for_joins=nodes_available_for_joins,
        node_data_set_resolver=node_data_set_resolver,
    )


def test_node_evaluator_with_no_linkable_specs(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    evaluation = node_evaluator.evaluate_node(required_linkable_specs=[], start_node=bookings_source_node)
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(), joinable_linkable_specs=(), join_recipes=(), unjoinable_linkable_specs=()
    )


def test_node_evaluator_with_unjoinable_specs(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="verification_type",
                identifier_links=(LinklessIdentifierSpec.from_element_name("verification"),),
            )
        ],
        start_node=bookings_source_node,
    )
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(),
        join_recipes=(),
        unjoinable_linkable_specs=(
            DimensionSpec(
                element_name="verification_type",
                identifier_links=(LinklessIdentifierSpec.from_element_name("verification"),),
            ),
        ),
    )


def test_node_evaluator_with_local_spec(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec in available in the start node."""
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[DimensionSpec(element_name="is_instant", identifier_links=())],
        start_node=bookings_source_node,
    )
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(DimensionSpec(element_name="is_instant", identifier_links=()),),
        joinable_linkable_specs=(),
        join_recipes=(),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_local_spec_using_primary_identifier(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec with an identifier link is available in the start node."""
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["users_latest"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="home_state_latest", identifier_links=(LinklessIdentifierSpec.from_element_name("user"),)
            )
        ],
        start_node=bookings_source_node,
    )

    assert evaluation == (
        LinkableInstanceSatisfiabilityEvaluation(
            local_linkable_specs=(
                DimensionSpec(
                    element_name="home_state_latest",
                    identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                ),
            ),
            joinable_linkable_specs=(),
            join_recipes=(),
            unjoinable_linkable_specs=(),
        )
    )


def test_node_evaluator_with_local_spec_using_primary_composite_identifier(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Similar to test_node_evaluator_with_local_spec_using_primary_identifier, but with a composite identifier"""
    bookings_source_node = consistent_id_object_repository.composite_model_read_nodes["users_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="country", identifier_links=(LinklessIdentifierSpec.from_element_name("user_team"),)
            )
        ],
        start_node=bookings_source_node,
    )

    assert (
        LinkableInstanceSatisfiabilityEvaluation(
            local_linkable_specs=(
                DimensionSpec(
                    element_name="country",
                    identifier_links=(LinklessIdentifierSpec(element_name="user_team", identifier_links=()),),
                ),
            ),
            joinable_linkable_specs=(),
            join_recipes=(),
            unjoinable_linkable_specs=(),
        )
        == evaluation
    )


def test_node_evaluator_with_joined_spec(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec is available if another node is joined."""
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(element_name="is_instant", identifier_links=()),
            DimensionSpec(
                element_name="country_latest",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
            ),
            DimensionSpec(
                element_name="capacity_latest",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
            ),
        ],
        start_node=bookings_source_node,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(DimensionSpec(element_name="is_instant", identifier_links=()),),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="country_latest",
                identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
            ),
            DimensionSpec(
                element_name="capacity_latest",
                identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["listings_latest"],
                join_on_identifier=LinklessIdentifierSpec(element_name="listing", identifier_links=()),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="country_latest",
                        identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
                    ),
                    DimensionSpec(
                        element_name="capacity_latest",
                        identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_joined_spec_on_unique_id(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Similar to test_node_evaluator_with_joined_spec() but using a unique identifier."""
    listings_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="company_name",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="user"),),
            ),
        ],
        start_node=listings_node,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="company_name",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["companies"],
                join_on_identifier=LinklessIdentifierSpec(element_name="user", identifier_links=()),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="company_name",
                        identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multiple_joined_specs(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where multiple nodes need to be joined to get all linkable specs."""
    views_source = consistent_id_object_repository.simple_model_read_nodes["views_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="home_state_latest",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="user"),),
            ),
            IdentifierSpec(
                element_name="user",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="listing"),),
            ),
        ],
        start_node=views_source,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            IdentifierSpec(
                element_name="user",
                identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
            ),
            DimensionSpec(
                element_name="home_state_latest",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["listings_latest"],
                join_on_identifier=LinklessIdentifierSpec(element_name="listing", identifier_links=()),
                satisfiable_linkable_specs=[
                    IdentifierSpec(
                        element_name="user",
                        identifier_links=(LinklessIdentifierSpec(element_name="listing", identifier_links=()),),
                    )
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            ),
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["users_latest"],
                join_on_identifier=LinklessIdentifierSpec(element_name="user", identifier_links=()),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state_latest",
                        identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                    )
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multihop_joined_spec(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    multi_hop_join_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    """Tests the case where multiple nodes need to be joined to get all linkable specs."""
    txn_source = consistent_id_object_repository.multihop_model_read_nodes["account_month_txns"]

    linkable_specs = [
        DimensionSpec(
            element_name="customer_name",
            identifier_links=(
                LinklessIdentifierSpec.from_element_name(element_name="account_id"),
                LinklessIdentifierSpec.from_element_name(element_name="customer_id"),
            ),
        ),
    ]

    multihop_node_evaluator = make_multihop_node_evaluator(
        consistent_id_object_repository=consistent_id_object_repository,
        multihop_semantic_model=multi_hop_join_semantic_model,
        desired_linkable_specs=linkable_specs,
        time_spine_source=time_spine_source,
    )

    evaluation = multihop_node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        start_node=txn_source,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="customer_name",
                identifier_links=(
                    LinklessIdentifierSpec.from_element_name(element_name="account_id"),
                    LinklessIdentifierSpec.from_element_name(element_name="customer_id"),
                ),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=evaluation.join_recipes[0].node_to_join,
                join_on_identifier=LinklessIdentifierSpec(element_name="account_id", identifier_links=()),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="customer_name",
                        identifier_links=(
                            LinklessIdentifierSpec(element_name="account_id", identifier_links=()),
                            LinklessIdentifierSpec(element_name="customer_id", identifier_links=()),
                        ),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(
                    PartitionTimeDimensionJoinDescription(
                        start_node_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned", identifier_links=(), time_granularity=TimeGranularity.DAY
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            identifier_links=(),
                            time_granularity=TimeGranularity.DAY,
                        ),
                    ),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_partition_joined_spec(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the joined node required a partitioned join."""
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="home_state",
                identifier_links=(LinklessIdentifierSpec.from_element_name(element_name="user"),),
            ),
        ],
        start_node=consistent_id_object_repository.simple_model_read_nodes["id_verifications"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="home_state",
                identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["users_ds_source"],
                join_on_identifier=LinklessIdentifierSpec(element_name="user", identifier_links=()),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state",
                        identifier_links=(LinklessIdentifierSpec(element_name="user", identifier_links=()),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(
                    PartitionTimeDimensionJoinDescription(
                        start_node_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            identifier_links=(),
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            identifier_links=(),
                        ),
                    ),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )
