from __future__ import annotations

import logging

import pytest
from dbt_semantic_interfaces.references import SemanticModelReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.mf_logging.pretty_print import mf_pformat

from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.plan_conversion.to_sql_plan.dataflow_to_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.render.sql_plan_renderer import SqlPlanRenderer

logger = logging.getLogger(__name__)


@pytest.mark.skip("Example for developers.")
def test_view_sql_generated_at_a_node(
    sql_client: SqlClient,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """Example that shows how to view generated SQL for nodes in a dataflow plan."""
    bookings_source_reference = SemanticModelReference(semantic_model_name="bookings_source")
    column_association_resolver = DunderColumnAssociationResolver()
    to_data_set_converter = SemanticModelToDataSetConverter(
        column_association_resolver=column_association_resolver,
        manifest_lookup=simple_semantic_manifest_lookup,
    )

    to_sql_plan_converter = DataflowToSqlPlanConverter(
        column_association_resolver=DunderColumnAssociationResolver(),
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )
    sql_renderer: SqlPlanRenderer = sql_client.sql_plan_renderer
    node_output_resolver = DataflowNodeToSqlSubqueryVisitor(
        column_association_resolver=column_association_resolver,
        semantic_manifest_lookup=simple_semantic_manifest_lookup,
    )

    # Show SQL and spec set at a source node.
    bookings_source_data_set = to_data_set_converter.create_sql_source_data_set(bookings_source_reference)
    read_source_node = ReadSqlSourceNode.create(bookings_source_data_set)
    conversion_result = to_sql_plan_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        dataflow_plan_node=read_source_node,
    )
    sql_plan_at_read_node = conversion_result.sql_plan
    sql_at_read_node = sql_renderer.render_sql_plan(sql_plan_at_read_node).sql
    spec_set_at_read_node = node_output_resolver.get_output_data_set(read_source_node).instance_set.spec_set
    logger.debug(LazyFormat(lambda: f"SQL generated at {read_source_node} is:\n\n{sql_at_read_node}"))
    logger.debug(LazyFormat(lambda: f"Spec set at {read_source_node} is:\n\n{mf_pformat(spec_set_at_read_node)}"))

    metric_time_node = MetricTimeDimensionTransformNode.create(
        parent_node=read_source_node,
        aggregation_time_dimension_reference=TimeDimensionReference(element_name="ds"),
    )

    # Show SQL and spec set at a filter node.
    filter_elements_node = FilterElementsNode.create(
        parent_node=metric_time_node,
        include_specs=InstanceSpecSet(
            time_dimension_specs=(
                TimeDimensionSpec(
                    element_name="metric_time",
                    entity_links=(),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                ),
            ),
        ),
    )
    conversion_result = to_sql_plan_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        dataflow_plan_node=filter_elements_node,
    )
    sql_plan_at_filter_elements_node = conversion_result.sql_plan
    sql_at_filter_elements_node = sql_renderer.render_sql_plan(sql_plan_at_filter_elements_node).sql
    spec_set_at_filter_elements_node = node_output_resolver.get_output_data_set(
        filter_elements_node
    ).instance_set.spec_set
    logger.debug(LazyFormat(lambda: f"SQL generated at {filter_elements_node} is:\n\n{sql_at_filter_elements_node}"))
    logger.debug(
        LazyFormat(lambda: f"Spec set at {filter_elements_node} is:\n\n{mf_pformat(spec_set_at_filter_elements_node)}")
    )
