from __future__ import annotations

import logging
from typing import FrozenSet, Mapping

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_str_snapshot_equal,
    make_schema_replacement_function,
)
from metricflow_semantics.toolkit.string_helpers import mf_indent

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.dataflow_plan_analyzer import DataflowPlanAnalyzer
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.sql.optimizer.optimization_levels import SqlGenerationOptionSet, SqlOptimizationLevel
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup

logger = logging.getLogger(__name__)


def convert_and_check(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    node: DataflowPlanNode,
    nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
) -> None:
    """Convert the dataflow plan to SQL and compare with snapshots."""
    # Generate without CTEs
    optimizers = SqlGenerationOptionSet.options_for_level(
        SqlOptimizationLevel.O5, use_column_alias_in_group_by=False
    ).optimizers
    conversion_result = dataflow_to_sql_converter.convert_using_specifics(
        dataflow_plan_node=node,
        sql_query_plan_id=None,
        optimizers=optimizers,
        nodes_to_convert_to_cte=frozenset(),
        spec_output_order=(),
    )
    sql_plan_without_cte = conversion_result.sql_plan

    # Generate with CTEs
    conversion_result = dataflow_to_sql_converter.convert_using_specifics(
        dataflow_plan_node=node,
        sql_query_plan_id=None,
        optimizers=optimizers,
        nodes_to_convert_to_cte=nodes_to_convert_to_cte,
        spec_output_order=(),
    )
    sql_plan_with_cte = conversion_result.sql_plan
    renderer = DefaultSqlPlanRenderer()

    lines = [
        "sql_without_cte:",
        mf_indent(renderer.render_sql_plan(sql_plan_without_cte).sql),
        "\n",
        "sql_with_cte:",
        mf_indent(renderer.render_sql_plan(sql_plan_with_cte).sql),
    ]

    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str="\n".join(lines),
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_configuration.mf_system_schema, source_schema=mf_test_configuration.mf_source_schema
        ),
    )


def test_cte_for_simple_dataflow_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Test a simple case for generating a CTE for a specific dataflow plan node."""
    simple_metric_input_spec = SimpleMetricInputSpec(
        element_name="bookings",
    )
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    filter_node = FilterElementsNode.create(
        parent_node=source_node, include_specs=InstanceSpecSet(simple_metric_input_specs=(simple_metric_input_spec,))
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        node=filter_node,
        nodes_to_convert_to_cte=frozenset(
            [
                source_node,
            ]
        ),
    )


def test_cte_for_shared_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
) -> None:
    """Check common branches in a query that uses derived metrics defined from metrics that are also in the query."""
    parse_result = query_parser.parse_and_validate_query(
        metric_names=("bookings", "views", "bookings_per_view"),
        group_by_names=("metric_time", "listing__country_latest"),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parse_result.query_spec)
    common_nodes = DataflowPlanAnalyzer.find_common_branches(dataflow_plan)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        node=dataflow_plan.sink_node,
        nodes_to_convert_to_cte=frozenset(common_nodes),
    )
