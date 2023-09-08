from __future__ import annotations

import logging
from typing import Sequence

import pytest
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    JoinLinkableInstancesRecipe,
    LinkableInstanceSatisfiabilityEvaluation,
    NodeEvaluatorForLinkableInstances,
)
from metricflow.dataflow.builder.partitions import PartitionTimeDimensionJoinDescription
from metricflow.dataflow.dataflow_plan import BaseOutput, ValidityWindowJoinDescription
from metricflow.dataset.dataset import DataSet
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.node_processor import PreDimensionJoinNodeProcessor
from metricflow.specs.specs import (
    DimensionSpec,
    EntityReference,
    EntitySpec,
    LinkableInstanceSpec,
    LinklessEntitySpec,
    TimeDimensionSpec,
)
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

logger = logging.getLogger(__name__)


@pytest.fixture
def node_evaluator(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    dataflow_plan_builder: DataflowPlanBuilder,
) -> NodeEvaluatorForLinkableInstances:  # noqa: D
    """Return a node evaluator using the nodes in semantic_model_name_to_nodes."""
    node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    source_nodes = tuple(consistent_id_object_repository.simple_model_read_nodes.values())

    return NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=simple_semantic_manifest_lookup.semantic_model_lookup,
        # Use all nodes in the simple model as candidates for joins.
        nodes_available_for_joins=source_nodes,
        node_data_set_resolver=node_data_set_resolver,
    )


def make_multihop_node_evaluator(
    model_source_nodes: Sequence[BaseOutput],
    semantic_manifest_lookup_with_multihop_links: SemanticManifestLookup,
    desired_linkable_specs: Sequence[LinkableInstanceSpec],
) -> NodeEvaluatorForLinkableInstances:  # noqa: D
    """Return a node evaluator using the nodes in multihop_semantic_model_name_to_nodes."""
    node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup_with_multihop_links),
        semantic_manifest_lookup=semantic_manifest_lookup_with_multihop_links,
    )

    node_processor = PreDimensionJoinNodeProcessor(
        semantic_model_lookup=semantic_manifest_lookup_with_multihop_links.semantic_model_lookup,
        node_data_set_resolver=node_data_set_resolver,
    )

    nodes_available_for_joins = node_processor.remove_unnecessary_nodes(
        desired_linkable_specs=desired_linkable_specs,
        nodes=model_source_nodes,
        metric_time_dimension_reference=DataSet.metric_time_dimension_reference(),
    )

    nodes_available_for_joins = node_processor.add_multi_hop_joins(
        desired_linkable_specs=desired_linkable_specs, nodes=nodes_available_for_joins
    )

    return NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=semantic_manifest_lookup_with_multihop_links.semantic_model_lookup,
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
                entity_links=(EntityReference(element_name="verification"),),
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
                entity_links=(EntityReference(element_name="verification"),),
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
        required_linkable_specs=[DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),))],
        start_node=bookings_source_node,
    )
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        joinable_linkable_specs=(),
        join_recipes=(),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_local_spec_using_primary_entity(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec with an entity link is available in the start node."""
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["users_latest"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference(element_name="user"),))
        ],
        start_node=bookings_source_node,
    )

    assert evaluation == (
        LinkableInstanceSatisfiabilityEvaluation(
            local_linkable_specs=(
                DimensionSpec(
                    element_name="home_state_latest",
                    entity_links=(EntityReference(element_name="user"),),
                ),
            ),
            joinable_linkable_specs=(),
            join_recipes=(),
            unjoinable_linkable_specs=(),
        )
    )


def test_node_evaluator_with_joined_spec(  # noqa: D
    consistent_id_object_repository: ConsistentIdObjectRepository,
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec is available if another node is joined."""
    bookings_source_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),
            DimensionSpec(
                element_name="country_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
            DimensionSpec(
                element_name="capacity_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ],
        start_node=bookings_source_node,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="country_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
            DimensionSpec(
                element_name="capacity_latest",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["listings_latest"],
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="country_latest",
                        entity_links=(EntityReference(element_name="listing"),),
                    ),
                    DimensionSpec(
                        element_name="capacity_latest",
                        entity_links=(EntityReference(element_name="listing"),),
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
    """Similar to test_node_evaluator_with_joined_spec() but using a unique entity."""
    listings_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="company_name",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ],
        start_node=listings_node,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="company_name",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["companies"],
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="company_name",
                        entity_links=(EntityReference(element_name="user"),),
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
                entity_links=(EntityReference(element_name="user"),),
            ),
            EntitySpec(
                element_name="user",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ],
        start_node=views_source,
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            EntitySpec(
                element_name="user",
                entity_links=(EntityReference(element_name="listing"),),
            ),
            DimensionSpec(
                element_name="home_state_latest",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["listings_latest"],
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    EntitySpec(
                        element_name="user",
                        entity_links=(EntityReference(element_name="listing"),),
                    )
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            ),
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["users_latest"],
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state_latest",
                        entity_links=(EntityReference(element_name="user"),),
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
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where multiple nodes need to be joined to get all linkable specs."""
    txn_source = consistent_id_object_repository.multihop_model_read_nodes["account_month_txns"]

    linkable_specs = [
        DimensionSpec(
            element_name="customer_name",
            entity_links=(
                EntityReference(element_name="account_id"),
                EntityReference(element_name="customer_id"),
            ),
        ),
    ]

    multihop_node_evaluator = make_multihop_node_evaluator(
        model_source_nodes=consistent_id_object_repository.multihop_model_source_nodes,
        semantic_manifest_lookup_with_multihop_links=multi_hop_join_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
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
                entity_links=(
                    EntityReference(element_name="account_id"),
                    EntityReference(element_name="customer_id"),
                ),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=evaluation.join_recipes[0].node_to_join,
                join_on_entity=LinklessEntitySpec.from_element_name("account_id"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="customer_name",
                        entity_links=(
                            EntityReference(element_name="account_id"),
                            EntityReference(element_name="customer_id"),
                        ),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(
                    PartitionTimeDimensionJoinDescription(
                        start_node_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned", entity_links=(), time_granularity=TimeGranularity.DAY
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            entity_links=(),
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
                entity_links=(EntityReference(element_name="user"),),
            ),
        ],
        start_node=consistent_id_object_repository.simple_model_read_nodes["id_verifications"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="home_state",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.simple_model_read_nodes["users_ds_source"],
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state",
                        entity_links=(EntityReference(element_name="user"),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(
                    PartitionTimeDimensionJoinDescription(
                        start_node_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            entity_links=(),
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            entity_links=(),
                        ),
                    ),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_scd_target(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is an SCD with a validity window filter."""
    node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=DunderColumnAssociationResolver(scd_semantic_manifest_lookup),
        semantic_manifest_lookup=scd_semantic_manifest_lookup,
    )

    source_nodes = tuple(consistent_id_object_repository.scd_model_read_nodes.values())

    node_evaluator = NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup,
        # Use all nodes in the simple model as candidates for joins.
        nodes_available_for_joins=source_nodes,
        node_data_set_resolver=node_data_set_resolver,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="is_lux",
                entity_links=(EntityReference(element_name="listing"),),
            )
        ],
        start_node=consistent_id_object_repository.scd_model_read_nodes["bookings_source"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="is_lux",
                entity_links=(EntityReference(element_name="listing"),),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=consistent_id_object_repository.scd_model_read_nodes["listings"],
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="is_lux",
                        entity_links=(EntityReference(element_name="listing"),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                validity_window=ValidityWindowJoinDescription(
                    window_start_dimension=TimeDimensionSpec(element_name="window_start", entity_links=()),
                    window_end_dimension=TimeDimensionSpec(element_name="window_end", entity_links=()),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multi_hop_scd_target(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is an SCD reached through another node.

    The validity window should have an entity link, the validity window join is mediated by an intervening
    node, and so we need to refer to that column via the link prefix.
    """
    linkable_specs = [DimensionSpec.from_name("listing__lux_listing__is_confirmed_lux")]
    node_evaluator = make_multihop_node_evaluator(
        model_source_nodes=consistent_id_object_repository.scd_model_source_nodes,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        start_node=consistent_id_object_repository.scd_model_read_nodes["bookings_source"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="is_confirmed_lux",
                entity_links=(
                    EntityReference(element_name="listing"),
                    EntityReference(element_name="lux_listing"),
                ),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=evaluation.join_recipes[0].node_to_join,
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="is_confirmed_lux",
                        entity_links=(
                            EntityReference(element_name="listing"),
                            EntityReference(element_name="lux_listing"),
                        ),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                validity_window=ValidityWindowJoinDescription(
                    window_start_dimension=TimeDimensionSpec(
                        element_name="window_start", entity_links=(EntityReference(element_name="lux_listing"),)
                    ),
                    window_end_dimension=TimeDimensionSpec(
                        element_name="window_end", entity_links=(EntityReference(element_name="lux_listing"),)
                    ),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multi_hop_through_scd(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is reached via an SCD.

    The validity window should NOT have any entity links, as the validity window join is not mediated by an
    intervening node and therefore the column name does not use the link prefix.
    """
    linkable_specs = [DimensionSpec.from_name("listing__user__home_state_latest")]
    node_evaluator = make_multihop_node_evaluator(
        model_source_nodes=consistent_id_object_repository.scd_model_source_nodes,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        start_node=consistent_id_object_repository.scd_model_read_nodes["bookings_source"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(
            DimensionSpec(
                element_name="home_state_latest",
                entity_links=(
                    EntityReference(element_name="listing"),
                    EntityReference(element_name="user"),
                ),
            ),
        ),
        join_recipes=(
            JoinLinkableInstancesRecipe(
                node_to_join=evaluation.join_recipes[0].node_to_join,
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state_latest",
                        entity_links=(
                            EntityReference(element_name="listing"),
                            EntityReference(element_name="user"),
                        ),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                validity_window=ValidityWindowJoinDescription(
                    window_start_dimension=TimeDimensionSpec(element_name="window_start", entity_links=()),
                    window_end_dimension=TimeDimensionSpec(element_name="window_end", entity_links=()),
                ),
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_invalid_multi_hop_scd(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is reached via an illegal SCD <-> SCD join.

    This will return an empty result because the linkable spec is not joinable in this model.
    """
    linkable_specs = [DimensionSpec.from_name("listing__user__account_type")]
    node_evaluator = make_multihop_node_evaluator(
        model_source_nodes=consistent_id_object_repository.scd_model_source_nodes,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        start_node=consistent_id_object_repository.scd_model_read_nodes["bookings_source"],
    )

    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(),
        joinable_linkable_specs=(),
        join_recipes=(),
        unjoinable_linkable_specs=(
            DimensionSpec(
                element_name="account_type",
                entity_links=(
                    EntityReference(element_name="listing"),
                    EntityReference(element_name="user"),
                ),
            ),
        ),
    )
