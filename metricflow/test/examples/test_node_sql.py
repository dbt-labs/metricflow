import logging

import pytest

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import ReadSqlSourceNode, FilterElementsNode, MetricTimeDimensionTransformNode
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.instances import DataSourceReference
from metricflow.model.semantic_model import SemanticModel
from metricflow.object_utils import pformat_big_objects
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs import TimeDimensionSpec, TimeDimensionReference
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@pytest.mark.skip("Example for developers.")
def test_view_sql_generated_at_a_node(
    sql_client: SqlClient,
    simple_semantic_model: SemanticModel,
    time_spine_source: TimeSpineSource,
) -> None:
    """Example that shows how to view generated SQL for nodes in a dataflow plan."""
    bookings_data_source = simple_semantic_model.data_source_semantics.get_by_reference(
        DataSourceReference(data_source_name="bookings_source")
    )
    assert bookings_data_source
    column_association_resolver = DefaultColumnAssociationResolver(
        semantic_model=simple_semantic_model,
    )
    to_data_set_converter = DataSourceToDataSetConverter(column_association_resolver)

    to_sql_plan_converter = DataflowToSqlQueryPlanConverter[SqlDataSet](
        column_association_resolver=DefaultColumnAssociationResolver(simple_semantic_model),
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )
    sql_renderer: SqlQueryPlanRenderer = sql_client.sql_engine_attributes.sql_query_plan_renderer
    node_output_resolver = DataflowPlanNodeOutputDataSetResolver[SqlDataSet](
        column_association_resolver=column_association_resolver,
        semantic_model=simple_semantic_model,
        time_spine_source=time_spine_source,
    )

    # Show SQL and spec set at a source node.
    bookings_source_data_set = to_data_set_converter.create_sql_source_data_set(bookings_data_source)
    read_source_node = ReadSqlSourceNode[SqlDataSet](bookings_source_data_set)
    sql_plan_at_read_node = to_sql_plan_converter.convert_to_sql_query_plan(
        sql_engine_attributes=sql_client.sql_engine_attributes,
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
        include_specs=(
            TimeDimensionSpec(element_name="metric_time", identifier_links=(), time_granularity=TimeGranularity.DAY),
        ),
    )
    sql_plan_at_filter_elements_node = to_sql_plan_converter.convert_to_sql_query_plan(
        sql_engine_attributes=sql_client.sql_engine_attributes,
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
