"""Tests for operations on dataflow plans and dataflow plan nodes."""

from __future__ import annotations

from dbt_semantic_interfaces.references import EntityReference, SemanticModelReference
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder


def test_source_semantic_models_accessor(
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests source semantic models access for a simple query plan."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
        )
    )

    assert dataflow_plan.source_semantic_models == frozenset(
        [SemanticModelReference(semantic_model_name="bookings_source")]
    )


def test_multi_hop_joined_source_semantic_models_accessor(
    dataflow_plan_builder: DataflowPlanBuilder,
) -> None:
    """Tests source semantic models access for a multi-hop join plan."""
    dataflow_plan = dataflow_plan_builder.build_plan(
        MetricFlowQuerySpec(
            metric_specs=(MetricSpec(element_name="bookings"),),
            dimension_specs=(
                DimensionSpec(
                    element_name="home_state_latest",
                    entity_links=(
                        EntityReference(element_name="listing"),
                        EntityReference(element_name="user"),
                    ),
                ),
            ),
        )
    )

    assert dataflow_plan.source_semantic_models == frozenset(
        [
            SemanticModelReference(semantic_model_name="bookings_source"),
            SemanticModelReference(semantic_model_name="listings_latest"),
            SemanticModelReference(semantic_model_name="users_latest"),
        ]
    )
