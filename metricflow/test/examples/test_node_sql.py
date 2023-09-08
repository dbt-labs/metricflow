from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.references import SemanticModelReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import FilterElementsNode, MetricTimeDimensionTransformNode, ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.specs import InstanceSpecSet, TimeDimensionReference, TimeDimensionSpec
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer

logger = logging.getLogger(__name__)


@pytest.mark.skip("Example for developers.")
def test_view_sql_generated_at_a_node(
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Example that shows how to view generated SQL for nodes in a dataflow plan."""
    bookings_semantic_model = simple_semantic_manifest_lookup.semantic_model_lookup.get_by_reference(
        SemanticModelReference(semantic_model_name="bookings_source")
    )
    assert bookings_semantic_model
    column_association_resolver = DunderColumnAssociationResolver(
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )
    to_data_set_converter = SemanticModelToDataSetConverter(column_association_resolver)

    to_sql_plan_converter = DataflowToSqlQueryPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(simple_semantic_manifest_lookup),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )
    sql_renderer: SqlQueryPlanRenderer = sql_client.sql_query_plan_renderer
    node_output_resolver = DataflowPlanNodeOutputDataSetResolver(
        column_association_resolver=column_association_resolver,
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    # Show SQL and spec set at a source node.
    bookings_source_data_set = to_data_set_converter.create_sql_source_data_set(bookings_semantic_model)
    read_source_node = ReadSqlSourceNode(bookings_source_data_set)
    sql_plan_at_read_node = to_sql_plan_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id="example_sql_plan",
        dataflow_plan_node=read_source_node,
        optimization_level=SqlQueryOptimizationLevel.O4,
    )
    sql_at_read_node = sql_renderer.render_sql_query_plan(sql_plan_at_read_node).sql
    spec_set_at_read_node = node_output_resolver.get_output_data_set(read_source_node).instance_set.spec_set
    logger.info(f"SQL generated at {read_source_node} is:\n\n{sql_at_read_node}")
    logger.info(f"Spec set at {read_source_node} is:\n\n{pformat_big_objects(spec_set_at_read_node)}")

    metric_time_node = MetricTimeDimensionTransformNode(
        parent_node=read_source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )

    # Show SQL and spec set at a filter node.
    filter_elements_node = FilterElementsNode(
        parent_node=metric_time_node,
        include_specs=InstanceSpecSet(
            time_dimension_specs=(
                TimeDimensionSpec(element_name="metric_time", entity_links=(), time_granularity=TimeGranularity.DAY),
            ),
        ),
    )
    sql_plan_at_filter_elements_node = to_sql_plan_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id="example_sql_plan",
        dataflow_plan_node=filter_elements_node,
        optimization_level=SqlQueryOptimizationLevel.O4,
    )
    sql_at_filter_elements_node = sql_renderer.render_sql_query_plan(sql_plan_at_filter_elements_node).sql
    spec_set_at_filter_elements_node = node_output_resolver.get_output_data_set(
        filter_elements_node
    ).instance_set.spec_set
    logger.info(f"SQL generated at {filter_elements_node} is:\n\n{sql_at_filter_elements_node}")
    logger.info(f"Spec set at {filter_elements_node} is:\n\n{pformat_big_objects(spec_set_at_filter_elements_node)}")
