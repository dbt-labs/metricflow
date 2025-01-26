from __future__ import annotations

import logging
from typing import Mapping, Sequence

import pytest
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.entity_spec import EntitySpec, LinklessEntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.sql.sql_join_type import SqlJoinType
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.node_evaluator import (
    JoinLinkableInstancesRecipe,
    LinkableInstanceSatisfiabilityEvaluation,
    NodeEvaluatorForLinkableInstances,
)
from metricflow.dataflow.builder.partitions import PartitionTimeDimensionJoinDescription
from metricflow.dataflow.builder.source_node import SourceNodeSet
from metricflow.dataflow.nodes.join_to_base import ValidityWindowJoinDescription
from metricflow.dataset.dataset_classes import DataSet
from metricflow.plan_conversion.node_processor import PreJoinNodeProcessor
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


@pytest.fixture
def node_evaluator(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
) -> NodeEvaluatorForLinkableInstances:
    """Return a node evaluator using the nodes in semantic_model_name_to_nodes."""
    mf_engine_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]

    node_data_set_resolver: DataflowNodeToSqlSubqueryVisitor = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=mf_engine_fixture.column_association_resolver,
        semantic_manifest_lookup=mf_engine_fixture.semantic_manifest_lookup,
    )

    return NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=mf_engine_test_fixture_mapping[
            SemanticManifestSetup.SIMPLE_MANIFEST
        ].semantic_manifest_lookup.semantic_model_lookup,
        nodes_available_for_joins=tuple(mf_engine_fixture.read_node_mapping.values()),
        node_data_set_resolver=node_data_set_resolver,
        time_spine_metric_time_nodes=mf_engine_fixture.source_node_set.time_spine_metric_time_nodes_tuple,
    )


def make_multihop_node_evaluator(
    source_node_set: SourceNodeSet,
    semantic_manifest_lookup_with_multihop_links: SemanticManifestLookup,
    desired_linkable_specs: Sequence[LinkableInstanceSpec],
) -> NodeEvaluatorForLinkableInstances:
    """Return a node evaluator using the nodes in multihop_semantic_model_name_to_nodes."""
    node_data_set_resolver: DataflowNodeToSqlSubqueryVisitor = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=DunderColumnAssociationResolver(),
        semantic_manifest_lookup=semantic_manifest_lookup_with_multihop_links,
    )

    node_processor = PreJoinNodeProcessor(
        semantic_model_lookup=semantic_manifest_lookup_with_multihop_links.semantic_model_lookup,
        node_data_set_resolver=node_data_set_resolver,
    )

    nodes_available_for_joins = node_processor.remove_unnecessary_nodes(
        desired_linkable_specs=desired_linkable_specs,
        nodes=source_node_set.source_nodes_for_metric_queries,
        metric_time_dimension_reference=DataSet.metric_time_dimension_reference(),
        time_spine_metric_time_nodes=source_node_set.time_spine_metric_time_nodes_tuple,
    )

    nodes_available_for_joins = list(
        node_processor.add_multi_hop_joins(
            desired_linkable_specs=desired_linkable_specs,
            nodes=nodes_available_for_joins,
            join_type=SqlJoinType.LEFT_OUTER,
        )
    )

    return NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=semantic_manifest_lookup_with_multihop_links.semantic_model_lookup,
        nodes_available_for_joins=nodes_available_for_joins,
        node_data_set_resolver=node_data_set_resolver,
        time_spine_metric_time_nodes=source_node_set.time_spine_metric_time_nodes_tuple,
    )


def test_node_evaluator_with_no_linkable_specs(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    bookings_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[], left_node=bookings_source_node, default_join_type=SqlJoinType.LEFT_OUTER
    )
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(), joinable_linkable_specs=(), join_recipes=(), unjoinable_linkable_specs=()
    )


def test_node_evaluator_with_unjoinable_specs(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    bookings_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="verification_type",
                entity_links=(EntityReference(element_name="verification"),),
            )
        ],
        left_node=bookings_source_node,
        default_join_type=SqlJoinType.LEFT_OUTER,
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


def test_node_evaluator_with_local_spec(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec in available in the start node."""
    bookings_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),))],
        left_node=bookings_source_node,
        default_join_type=SqlJoinType.LEFT_OUTER,
    )
    assert evaluation == LinkableInstanceSatisfiabilityEvaluation(
        local_linkable_specs=(DimensionSpec(element_name="is_instant", entity_links=(EntityReference("booking"),)),),
        joinable_linkable_specs=(),
        join_recipes=(),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_local_spec_using_primary_entity(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec with an entity link is available in the start node."""
    bookings_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "users_latest"
    ]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference(element_name="user"),))
        ],
        left_node=bookings_source_node,
        default_join_type=SqlJoinType.LEFT_OUTER,
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


def test_node_evaluator_with_joined_spec(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where the requested linkable spec is available if another node is joined."""
    bookings_source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
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
        left_node=bookings_source_node,
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
                    "listings_latest"
                ],
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
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_joined_spec_on_unique_id(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Similar to test_node_evaluator_with_joined_spec() but using a unique entity."""
    listings_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "listings_latest"
    ]
    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="company_name",
                entity_links=(EntityReference(element_name="user"),),
            ),
        ],
        left_node=listings_node,
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
                    "companies"
                ],
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="company_name",
                        entity_links=(EntityReference(element_name="user"),),
                    ),
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multiple_joined_specs(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    node_evaluator: NodeEvaluatorForLinkableInstances,
) -> None:
    """Tests the case where multiple nodes need to be joined to get all linkable specs."""
    views_source = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "views_source"
    ]
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
        left_node=views_source,
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
                    "listings_latest"
                ],
                join_on_entity=LinklessEntitySpec.from_element_name("listing"),
                satisfiable_linkable_specs=[
                    EntitySpec(
                        element_name="user",
                        entity_links=(EntityReference(element_name="listing"),),
                    )
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
            JoinLinkableInstancesRecipe(
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
                    "users_latest"
                ],
                join_on_entity=LinklessEntitySpec.from_element_name("user"),
                satisfiable_linkable_specs=[
                    DimensionSpec(
                        element_name="home_state_latest",
                        entity_links=(EntityReference(element_name="user"),),
                    )
                ],
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multihop_joined_spec(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    partitioned_multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where multiple nodes need to be joined to get all linkable specs."""
    txn_source = mf_engine_test_fixture_mapping[
        SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
    ].read_node_mapping["account_month_txns"]

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
        source_node_set=mf_engine_test_fixture_mapping[
            SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
        ].source_node_set,
        semantic_manifest_lookup_with_multihop_links=partitioned_multi_hop_join_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = multihop_node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs, left_node=txn_source, default_join_type=SqlJoinType.LEFT_OUTER
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
                            element_name="ds_partitioned",
                            entity_links=(),
                            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            entity_links=(),
                            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                        ),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_partition_joined_spec(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
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
        left_node=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
            "id_verifications"
        ],
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
                    "users_ds_source"
                ],
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
                            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                        ),
                        node_to_join_time_dimension_spec=TimeDimensionSpec(
                            element_name="ds_partitioned",
                            entity_links=(),
                            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                        ),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_scd_target(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is an SCD with a validity window filter."""
    node_data_set_resolver: DataflowNodeToSqlSubqueryVisitor = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=DunderColumnAssociationResolver(),
        semantic_manifest_lookup=scd_semantic_manifest_lookup,
    )

    mf_engine_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST]

    node_evaluator = NodeEvaluatorForLinkableInstances(
        semantic_model_lookup=scd_semantic_manifest_lookup.semantic_model_lookup,
        # Use all nodes in the simple model as candidates for joins.
        nodes_available_for_joins=tuple(mf_engine_fixture.read_node_mapping.values()),
        node_data_set_resolver=node_data_set_resolver,
        time_spine_metric_time_nodes=mf_engine_fixture.source_node_set.time_spine_metric_time_nodes_tuple,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=[
            DimensionSpec(
                element_name="is_lux",
                entity_links=(EntityReference(element_name="listing"),),
            )
        ],
        left_node=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].read_node_mapping[
            "bookings_source"
        ],
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                node_to_join=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].read_node_mapping[
                    "listings"
                ],
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
                    window_start_dimension=TimeDimensionSpec(
                        element_name="window_start",
                        entity_links=(),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                    window_end_dimension=TimeDimensionSpec(
                        element_name="window_end",
                        entity_links=(),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multi_hop_scd_target(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is an SCD reached through another node.

    The validity window should have an entity link, the validity window join is mediated by an intervening
    node, and so we need to refer to that column via the link prefix.
    """
    linkable_specs = [
        DimensionSpec(
            element_name="is_confirmed_lux", entity_links=(EntityReference("listing"), EntityReference("lux_listing"))
        )
    ]
    node_evaluator = make_multihop_node_evaluator(
        source_node_set=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].source_node_set,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        left_node=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].read_node_mapping[
            "bookings_source"
        ],
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                        element_name="window_start",
                        entity_links=(EntityReference(element_name="lux_listing"),),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                    window_end_dimension=TimeDimensionSpec(
                        element_name="window_end",
                        entity_links=(EntityReference(element_name="lux_listing"),),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_multi_hop_through_scd(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is reached via an SCD.

    The validity window should NOT have any entity links, as the validity window join is not mediated by an
    intervening node and therefore the column name does not use the link prefix.
    """
    linkable_specs = [
        DimensionSpec(
            element_name="home_state_latest", entity_links=(EntityReference("listing"), EntityReference("user"))
        )
    ]
    node_evaluator = make_multihop_node_evaluator(
        source_node_set=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].source_node_set,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        left_node=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].read_node_mapping[
            "bookings_source"
        ],
        default_join_type=SqlJoinType.LEFT_OUTER,
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
                    window_start_dimension=TimeDimensionSpec(
                        element_name="window_start",
                        entity_links=(),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                    window_end_dimension=TimeDimensionSpec(
                        element_name="window_end",
                        entity_links=(),
                        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    ),
                ),
                join_type=SqlJoinType.LEFT_OUTER,
            ),
        ),
        unjoinable_linkable_specs=(),
    )


def test_node_evaluator_with_invalid_multi_hop_scd(
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    scd_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Tests the case where the joined node is reached via an illegal SCD <-> SCD join.

    This will return an empty result because the linkable spec is not joinable in this model.
    """
    linkable_specs = [
        DimensionSpec(element_name="account_type", entity_links=(EntityReference("listing"), EntityReference("user")))
    ]
    node_evaluator = make_multihop_node_evaluator(
        source_node_set=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].source_node_set,
        semantic_manifest_lookup_with_multihop_links=scd_semantic_manifest_lookup,
        desired_linkable_specs=linkable_specs,
    )

    evaluation = node_evaluator.evaluate_node(
        required_linkable_specs=linkable_specs,
        left_node=mf_engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].read_node_mapping[
            "bookings_source"
        ],
        default_join_type=SqlJoinType.LEFT_OUTER,
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
