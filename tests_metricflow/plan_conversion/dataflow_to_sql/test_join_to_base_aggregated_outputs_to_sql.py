from __future__ import annotations

from typing import Mapping, Optional, Sequence

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import GroupByItemSet
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.simple_metric_input_spec import SimpleMetricInputSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow.dataflow.builder.aggregation_helper import NullFillValueMapping
from metricflow.dataflow.dataflow_plan import DataflowPlanNode
from metricflow.dataflow.nodes.aggregate_simple_metric_inputs import AggregateSimpleMetricInputsNode
from metricflow.dataflow.nodes.filter_elements import SelectorNode
from metricflow.dataflow.nodes.join_to_base_aggregated_outputs import JoinToBaseAggregatedOutputsNode
from metricflow.dataflow.nodes.where_filter import WhereFilterNode
from metricflow.plan_conversion.to_sql_plan.dataflow_to_sql import DataflowToSqlPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.optimizer.optimization_levels import SqlOptimizationLevel
from metricflow_semantic_interfaces.references import EntityReference
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.plan_conversion.test_dataflow_to_sql_plan import convert_and_check

_COUNTRY = DimensionSpec(element_name="country_latest", entity_links=(EntityReference("listing"),))
_IS_LUX = DimensionSpec(element_name="is_lux_latest", entity_links=(EntityReference("listing"),))
_CAPACITY = DimensionSpec(element_name="capacity_latest", entity_links=(EntityReference("listing"),))
_CREATED_DAY = TimeDimensionSpec(
    element_name="ds",
    entity_links=(EntityReference("listing"),),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
)


def _aggregated_node(
    source_node: DataflowPlanNode,
    metric_name: str,
    dimension_specs: Sequence[DimensionSpec],
    time_dimension_specs: Sequence[TimeDimensionSpec] = (),
) -> AggregateSimpleMetricInputsNode:
    return AggregateSimpleMetricInputsNode.create(
        parent_node=SelectorNode.create(
            parent_node=source_node,
            include_specs=InstanceSpecSet(
                simple_metric_input_specs=(SimpleMetricInputSpec(element_name=metric_name),),
                dimension_specs=tuple(dimension_specs),
                time_dimension_specs=tuple(time_dimension_specs),
            ),
        ),
        null_fill_value_mapping=NullFillValueMapping.create(),
    )


@pytest.fixture
def listings_source_node(  # noqa: D103
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> DataflowPlanNode:
    return mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping["listings_latest"]


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_join_to_base_matches_null_time_keys(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY,), (_CREATED_DAY,)),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", (_COUNTRY,), (_CREATED_DAY,))],
    )
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_join_to_base_on_multiple_shared_group_bys(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY, _IS_LUX, _CAPACITY)),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", (_COUNTRY, _IS_LUX))],
    )
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=node,
    )


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_join_to_base_with_two_declared_inputs(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY, _IS_LUX)),
        declared_input_nodes=[
            _aggregated_node(listings_source_node, "listings", (_COUNTRY,)),
            _aggregated_node(listings_source_node, "smallest_listing", (_IS_LUX,)),
        ],
    )
    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        sql_client=sql_client,
        node=node,
    )


def _execute(
    node: DataflowPlanNode,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    optimization_level: SqlOptimizationLevel,
) -> Sequence[Mapping[str, object]]:
    sql_plan = dataflow_to_sql_converter.convert_to_sql_plan(
        sql_engine_type=sql_client.sql_engine_type,
        sql_query_plan_id=DagId.from_str("plan0"),
        dataflow_plan_node=node,
        optimization_level=optimization_level,
    ).sql_plan
    data_table = sql_client.query(sql_client.sql_plan_renderer.render_sql_plan(sql_plan).sql)
    return sorted(
        (dict(zip(data_table.column_names, row)) for row in data_table.rows),
        key=repr,
    )


def _row(
    country: Optional[str], is_lux: Optional[bool], largest_listing: Optional[int], listings: Optional[int]
) -> Mapping[str, object]:
    return {
        "listing__country_latest": country,
        "listing__is_lux_latest": is_lux,
        "__largest_listing": largest_listing,
        "__listings": listings,
    }


@pytest.mark.duckdb_only
@pytest.mark.parametrize("optimization_level", [SqlOptimizationLevel.O0, SqlOptimizationLevel.default_level()])
def test_join_to_base_keeps_groups_with_null_dimension_values(  # noqa: D103
    create_source_tables: None,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
    optimization_level: SqlOptimizationLevel,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY, _IS_LUX)),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", (_COUNTRY, _IS_LUX))],
    )

    rows = _execute(node, dataflow_to_sql_converter, sql_client, optimization_level)

    assert rows == sorted(
        [
            _row("us", True, 5, 2),
            _row("cote d'ivoire", False, 4, 1),
            _row("us", None, None, 1),
            _row(None, None, None, 1),
            _row("ca", False, None, 1),
        ],
        key=repr,
    )


@pytest.mark.duckdb_only
def test_join_to_base_on_partially_shared_group_by_keeps_base_grain(  # noqa: D103
    create_source_tables: None,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY, _IS_LUX)),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", (_COUNTRY,))],
    )

    rows = _execute(node, dataflow_to_sql_converter, sql_client, SqlOptimizationLevel.default_level())

    assert rows == sorted(
        [
            _row("us", True, 5, 3),
            _row("cote d'ivoire", False, 4, 1),
            _row("us", None, None, 3),
            _row(None, None, None, 1),
            _row("ca", False, None, 1),
        ],
        key=repr,
    )


@pytest.mark.duckdb_only
def test_join_to_base_keeps_base_groups_without_a_declared_input_match(  # noqa: D103
    create_source_tables: None,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    us_only_listings = AggregateSimpleMetricInputsNode.create(
        parent_node=WhereFilterNode.create(
            parent_node=SelectorNode.create(
                parent_node=listings_source_node,
                include_specs=InstanceSpecSet(
                    simple_metric_input_specs=(SimpleMetricInputSpec(element_name="listings"),),
                    dimension_specs=(_COUNTRY, _IS_LUX),
                ),
            ),
            filter_specs=(
                WhereFilterSpec(
                    where_sql="listing__country_latest = 'us'",
                    bind_parameters=SqlBindParameterSet(),
                    element_set=GroupByItemSet(
                        annotated_specs=(
                            AnnotatedSpec.create(
                                element_type=LinkableElementType.DIMENSION,
                                element_name="country_latest",
                                entity_links=(EntityReference(element_name="listing"),),
                                time_grain=None,
                                date_part=None,
                                metric_subquery_entity_links=None,
                                properties=(),
                                origin_model_ids=(),
                                derived_from_semantic_models=(),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        null_fill_value_mapping=NullFillValueMapping.create(),
    )
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY, _IS_LUX)),
        declared_input_nodes=[us_only_listings],
    )

    rows = _execute(node, dataflow_to_sql_converter, sql_client, SqlOptimizationLevel.default_level())

    assert rows == sorted(
        [
            _row("us", True, 5, 2),
            _row("cote d'ivoire", False, 4, None),
            _row("us", None, None, 1),
            _row(None, None, None, None),
            _row("ca", False, None, None),
        ],
        key=repr,
    )


@pytest.mark.duckdb_only
def test_join_to_base_without_shared_group_by_repeats_declared_input(  # noqa: D103
    create_source_tables: None,
    dataflow_to_sql_converter: DataflowToSqlPlanConverter,
    sql_client: SqlClient,
    listings_source_node: DataflowPlanNode,
) -> None:
    node = JoinToBaseAggregatedOutputsNode.create(
        base_node=_aggregated_node(listings_source_node, "largest_listing", (_COUNTRY,)),
        declared_input_nodes=[_aggregated_node(listings_source_node, "listings", ())],
    )

    rows = _execute(node, dataflow_to_sql_converter, sql_client, SqlOptimizationLevel.default_level())

    assert rows == sorted(
        [
            {"listing__country_latest": "us", "__largest_listing": 5, "__listings": 6},
            {"listing__country_latest": "cote d'ivoire", "__largest_listing": 4, "__listings": 6},
            {"listing__country_latest": None, "__largest_listing": None, "__listings": 6},
            {"listing__country_latest": "ca", "__largest_listing": None, "__listings": 6},
        ],
        key=repr,
    )
