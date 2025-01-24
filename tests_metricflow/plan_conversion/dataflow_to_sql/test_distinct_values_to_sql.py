from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilter
from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.plan_conversion.test_dataflow_to_sql_plan import convert_and_check


@pytest.mark.sql_engine_snapshot
def test_dimensions_requiring_join(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests querying 2 dimensions that require a join."""
    dimension_specs = (
        DimensionSpec(element_name="home_state_latest", entity_links=(EntityReference(element_name="user"),)),
        DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference(element_name="listing"),)),
    )
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(
        query_spec=MetricFlowQuerySpec(dimension_specs=dimension_specs)
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )


@pytest.mark.sql_engine_snapshot
def test_dimension_values_with_a_join_and_a_filter(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    query_parser: MetricFlowQueryParser,
    dataflow_plan_builder: DataflowPlanBuilder,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
) -> None:
    """Tests querying 2 dimensions that require a join and a filter."""
    query_spec = query_parser.parse_and_validate_query(
        group_by_names=("user__home_state_latest", "listing__is_lux_latest"),
        where_constraints=[
            PydanticWhereFilter(
                where_sql_template="{{ Dimension('user__home_state_latest') }} = 'us'",
            )
        ],
    ).query_spec
    dataflow_plan = dataflow_plan_builder.build_plan_for_distinct_values(query_spec)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=dataflow_plan.sink_node,
    )
