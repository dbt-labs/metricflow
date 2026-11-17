from __future__ import annotations

import copy
import dataclasses
import logging
from collections.abc import Mapping, Sequence
from typing import Optional

import pytest
from metricflow_semantics.errors.error_classes import (
    ExcludeDimensionsWithTimeOffsetError,
    InvalidExcludedFilterError,
    InvalidQueryException,
    UnmatchedExcludeDimensionsError,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.metric_spec import MetricSpec
from metricflow_semantics.specs.query_spec import MetricFlowQuerySpec
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_spec_factory import WhereFilterSpecFactory

from metricflow.dataflow.nodes.join_to_base_aggregated_outputs import JoinToBaseAggregatedOutputsNode
from metricflow.dataflow.optimizer.dataflow_optimizer_factory import DataflowPlanOptimization
from metricflow.metric_evaluation.dfs_me_planner import DepthFirstSearchMetricEvaluationPlanner
from metricflow.metric_evaluation.metric_query_helper import MetricQueryHelper
from metricflow.metric_evaluation.metric_query_planner import MetricEvaluationPlanner
from metricflow.metric_evaluation.passthrough.passthrough_me_planner import PassThroughMetricEvaluationPlanner
from metricflow.metric_evaluation.plan.query_element import MetricQueryElement
from metricflow.plan_conversion.node_processor import PredicatePushdownState
from metricflow_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from metricflow_semantic_interfaces.implementations.metric import PydanticMetric
from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantic_interfaces.references import EntityReference, MetricReference
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture
from tests_metricflow.fixtures.sql_clients.ddl_sql_client import SqlClient

logger = logging.getLogger(__name__)

COUNTRY = "listing__country_latest"
IS_LUX = "listing__is_lux_latest"
FILTERED_RATIO = "bookings_per_view"
METRIC_LEVEL_FILTER_SQL = "{{ Dimension('listing__is_lux_latest') }}"


def _get_metric(manifest: PydanticSemanticManifest, metric_name: str) -> PydanticMetric:
    return next(metric for metric in manifest.metrics if metric.name == metric_name)


def _declare(manifest: PydanticSemanticManifest, metric_name: str, input_name: str, *names: str) -> None:
    for input_metric in _get_metric(manifest, metric_name).input_metrics:
        if input_metric.name == input_name:
            input_metric.exclude_dimensions = tuple(names)
            return
    raise AssertionError(f"No input {input_name} in {metric_name}")


@pytest.fixture(scope="module")
def parsing_fixture(
    simple_semantic_manifest: PydanticSemanticManifest, sql_client: SqlClient
) -> MetricFlowEngineTestFixture:
    """Fixture without any `exclude_dimensions`, used to parse queries."""
    manifest = copy.deepcopy(simple_semantic_manifest)
    _get_metric(manifest, FILTERED_RATIO).filter = PydanticWhereFilterIntersection.parse_obj(
        {"where_filters": [{"where_sql_template": METRIC_LEVEL_FILTER_SQL}]}
    )
    return MetricFlowEngineTestFixture.from_parameters(sql_client, manifest)


@pytest.fixture(scope="module", params=("qualified", "bare"))
def declared_fixture(
    request: pytest.FixtureRequest, parsing_fixture: MetricFlowEngineTestFixture, sql_client: SqlClient
) -> MetricFlowEngineTestFixture:
    """Fixture where some inputs declare `exclude_dimensions`, with entity-qualified or bare names."""

    def declared_name(qualified_name: str) -> str:
        return qualified_name if request.param == "qualified" else qualified_name.split("__")[-1]

    manifest = copy.deepcopy(parsing_fixture.semantic_manifest)
    _declare(manifest, "bookings_per_listing", "listings", declared_name(COUNTRY))
    _declare(manifest, "lux_booking_value_rate_expr", "booking_value", declared_name(COUNTRY))
    _declare(manifest, "lux_booking_value_rate_expr", "bookings", declared_name(IS_LUX))
    _declare(manifest, "bookings_per_booker", "bookers", declared_name(COUNTRY), declared_name(IS_LUX))
    _declare(manifest, FILTERED_RATIO, "views", declared_name(IS_LUX))
    _declare(manifest, "booking_fees_last_week_per_booker_this_week", "bookers", declared_name(COUNTRY))
    _declare(manifest, "booking_fees_last_week_per_booker_this_week", "booking_value", declared_name(COUNTRY))
    for input_metric in _get_metric(manifest, "bookings_offset_once").input_metrics:
        input_metric.offset_window = None
    _declare(manifest, "bookings_offset_once", "bookings", declared_name(COUNTRY))
    return MetricFlowEngineTestFixture.from_parameters(sql_client, manifest)


PLANNER_CLASSES = (DepthFirstSearchMetricEvaluationPlanner, PassThroughMetricEvaluationPlanner)


def _planner(
    planner_class: type[MetricEvaluationPlanner], fixture: MetricFlowEngineTestFixture
) -> MetricEvaluationPlanner:
    return planner_class(
        manifest_object_lookup=fixture.manifest_object_lookup,
        metric_lookup=fixture.semantic_manifest_lookup.metric_lookup,
        column_association_resolver=fixture.column_association_resolver,
    )


def _parse_query(
    parsing_fixture: MetricFlowEngineTestFixture,
    metric_name: str,
    group_by_names: Sequence[str],
    where_constraints: Optional[Sequence[str]],
) -> tuple[MetricFlowQuerySpec, WhereFilterSpecFactory, tuple[WhereFilterSpec, ...]]:
    query_spec = parsing_fixture.query_parser.parse_and_validate_query(
        metric_names=[metric_name],
        group_by_names=group_by_names,
        where_constraint_strs=where_constraints,
    ).query_spec
    filter_spec_factory = WhereFilterSpecFactory(
        column_association_resolver=parsing_fixture.column_association_resolver,
        spec_resolution_lookup=query_spec.filter_spec_resolution_lookup,
        custom_grain_names=tuple(grain.name for grain in parsing_fixture.manifest_object_lookup.expanded_time_grains),
    )
    query_level_filter_specs = tuple(
        filter_spec_factory.create_from_where_filter_intersection(
            filter_location=WhereFilterLocation.for_query((MetricReference(metric_name),)),
            filter_intersection=query_spec.filter_intersection,
        )
    )
    return query_spec, filter_spec_factory, query_level_filter_specs


def _input_query_elements(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
    metric_name: str,
    group_by_names: Sequence[str],
    where_constraints: Optional[Sequence[str]] = None,
) -> Mapping[str, MetricQueryElement]:
    query_spec, filter_spec_factory, query_level_filter_specs = _parse_query(
        parsing_fixture, metric_name, group_by_names, where_constraints
    )
    me_plan = _planner(planner_class, declared_fixture).build_plan(
        metric_specs=(MetricSpec.create(element_name=metric_name, where_filter_specs=query_level_filter_specs),),
        group_by_item_specs=query_spec.linkable_specs.as_tuple,
        predicate_pushdown_state=PredicatePushdownState.create(where_filter_specs=()),
        filter_spec_factory=filter_spec_factory,
    )
    return {
        element.metric_name: element
        for node in me_plan.nodes_in_dfs_order()
        for element in node.output_query_elements
        if element.metric_name != metric_name
    }


def _group_by_names(element: MetricQueryElement) -> set[str]:
    return {spec.dunder_name for spec in element.group_by_item_specs}


def _filter_sqls(element: MetricQueryElement) -> set[str]:
    return {filter_spec.where_sql for filter_spec in element.metric_spec.where_filter_specs}


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_ratio_excludes_dimension_from_denominator_only(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """Only the input that declares the dimension drops it from the group-by."""
    elements = _input_query_elements(
        planner_class, parsing_fixture, declared_fixture, "bookings_per_listing", ["metric_time", COUNTRY]
    )

    assert _group_by_names(elements["bookings"]) == {"metric_time__day", COUNTRY}
    assert _group_by_names(elements["listings"]) == {"metric_time__day"}
    assert elements["bookings"].metric_spec.exclude_dimensions == ()
    assert elements["listings"].metric_spec.exclude_dimensions in ((COUNTRY,), ("country_latest",))


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_derived_metric_with_three_inputs_declares_on_some(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """Each input derives its own group-by from its own declaration."""
    elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "lux_booking_value_rate_expr",
        ["metric_time", COUNTRY, IS_LUX],
    )

    assert _group_by_names(elements["average_booking_value"]) == {"metric_time__day", COUNTRY, IS_LUX}
    assert _group_by_names(elements["bookings"]) == {"metric_time__day", COUNTRY}
    assert _group_by_names(elements["booking_value"]) == {"metric_time__day", IS_LUX}


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_input_declaring_several_dimensions(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """All declared dimensions are removed from that input."""
    elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "bookings_per_booker",
        ["metric_time", COUNTRY, IS_LUX],
    )

    assert _group_by_names(elements["bookers"]) == {"metric_time__day"}
    assert _group_by_names(elements["bookings"]) == {"metric_time__day", COUNTRY, IS_LUX}


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_filter_on_declared_dimension_is_not_applied_to_declaring_input(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """A query filter that only references the declared dimension applies to the other inputs only."""
    elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "bookings_per_listing",
        ["metric_time"],
        where_constraints=["{{ Dimension('listing__country_latest') }} = 'US'"],
    )

    assert len(_filter_sqls(elements["bookings"])) == 1
    assert _filter_sqls(elements["listings"]) == set()


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_filter_on_other_dimension_is_applied_to_all_inputs(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """A query filter that doesn't reference the declared dimension applies to every input."""
    elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "bookings_per_listing",
        ["metric_time"],
        where_constraints=["{{ Dimension('listing__is_lux_latest') }}"],
    )

    assert len(_filter_sqls(elements["bookings"])) == 1
    assert _filter_sqls(elements["listings"]) == _filter_sqls(elements["bookings"])


def _select(
    parsing_fixture: MetricFlowEngineTestFixture,
    helper: MetricQueryHelper,
    where_constraint: str,
    exclude_dimensions: Sequence[str] = (COUNTRY,),
) -> Sequence[WhereFilterSpec]:
    _, _, filter_specs = _parse_query(parsing_fixture, "bookings_per_listing", ["metric_time"], [where_constraint])
    return helper.select_filter_specs_for_input_metric(
        metric_name="bookings_per_listing",
        input_metric_name="listings",
        filter_specs=filter_specs,
        exclude_dimensions=exclude_dimensions,
    )


COUNTRY_DIM = "{{ Dimension('listing__country_latest') }}"
LUX_DIM = "{{ Dimension('listing__is_lux_latest') }}"
CAPACITY_DIM = "{{ Dimension('listing__capacity_latest') }}"


@pytest.mark.parametrize(
    ("where_constraint", "expected_where_sql"),
    [
        pytest.param(f"{COUNTRY_DIM} = 'US' AND {LUX_DIM}", "listing__is_lux_latest", id="and_drops_excluded_term"),
        pytest.param(
            f"({COUNTRY_DIM} = 'US' AND {LUX_DIM}) OR ({COUNTRY_DIM} = 'CA' AND {CAPACITY_DIM} > 3)",
            "(listing__is_lux_latest) OR (listing__capacity_latest > 3)",
            id="or_of_ands_keeps_other_terms",
        ),
        pytest.param(
            f"{LUX_DIM} AND ({COUNTRY_DIM} = 'US' OR {CAPACITY_DIM} > 3)",
            "listing__is_lux_latest",
            id="or_with_excluded_term_is_dropped_inside_and",
        ),
        pytest.param(
            f"NOT ({COUNTRY_DIM} = 'US' OR {COUNTRY_DIM} = 'CA') AND {LUX_DIM}",
            "listing__is_lux_latest",
            id="not_over_only_excluded_terms_is_dropped",
        ),
        pytest.param(
            f"{LUX_DIM} AND NOT NOT {COUNTRY_DIM} = 'US'",
            "listing__is_lux_latest",
            id="double_not_over_only_excluded_term_is_dropped",
        ),
        pytest.param(
            f"({LUX_DIM} OR {CAPACITY_DIM} > 3) AND {COUNTRY_DIM} IN ('US', 'it''s')",
            "(listing__is_lux_latest OR listing__capacity_latest > 3)",
            id="in_list_with_escaped_quote",
        ),
        pytest.param(
            f"NOT {COUNTRY_DIM} = 'US' AND NOT {LUX_DIM}",
            "NOT listing__is_lux_latest",
            id="negated_single_predicate_is_a_predicate",
        ),
        pytest.param(
            f"{COUNTRY_DIM} IS NOT NULL AND LOWER(CAST({CAPACITY_DIM} AS VARCHAR)) LIKE '%a%'",
            "LOWER(CAST(listing__capacity_latest AS VARCHAR)) LIKE '%a%'",
            id="functions_and_casts",
        ),
        pytest.param(
            f"{CAPACITY_DIM} BETWEEN 1 AND 5 AND {COUNTRY_DIM} = 'US'",
            "listing__capacity_latest BETWEEN 1 AND 5",
            id="between_is_one_predicate",
        ),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND {CAPACITY_DIM} != 3 AND EXTRACT(YEAR FROM {CAPACITY_DIM}) = 2020",
            "listing__capacity_latest <> 3 AND EXTRACT(YEAR FROM listing__capacity_latest) = 2020",
            id="not_equal_spelling_is_normalized",
        ),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND {LUX_DIM} AND REGEXP_LIKE({CAPACITY_DIM}, '^a') AND {CAPACITY_DIM} ILIKE '%a%'",
            "listing__is_lux_latest AND REGEXP_LIKE(listing__capacity_latest, '^a')"
            " AND listing__capacity_latest ILIKE '%a%'",
            id="engine_specific_functions_are_kept",
        ),
        pytest.param(
            f"NOT ({LUX_DIM} AND {CAPACITY_DIM} > 3) AND {COUNTRY_DIM} = 'US'",
            "NOT (listing__is_lux_latest AND listing__capacity_latest > 3)",
            id="not_over_other_predicates_is_kept",
        ),
    ],
)
def test_filter_mixing_declared_and_other_dimensions_is_relaxed(
    parsing_fixture: MetricFlowEngineTestFixture,
    helper: MetricQueryHelper,
    where_constraint: str,
    expected_where_sql: str,
) -> None:
    """Predicates on the declared dimension are replaced with TRUE, which AND drops and OR absorbs."""
    selected = _select(parsing_fixture, helper, where_constraint)

    assert [filter_spec.where_sql for filter_spec in selected] == [expected_where_sql]
    assert COUNTRY not in {spec.dunder_name for spec in selected[0].linkable_specs}


@pytest.mark.parametrize(
    "where_constraint",
    [
        pytest.param(f"{COUNTRY_DIM} = 'US' OR {LUX_DIM}", id="or_with_a_declared_only_branch"),
        pytest.param(f"{COUNTRY_DIM} = 'US' AND {COUNTRY_DIM} <> 'CA'", id="only_declared"),
    ],
)
def test_filter_that_becomes_true_is_dropped(
    parsing_fixture: MetricFlowEngineTestFixture, helper: MetricQueryHelper, where_constraint: str
) -> None:
    """A filter in which every branch is relaxed to TRUE doesn't apply to the input."""
    assert _select(parsing_fixture, helper, where_constraint) == ()


def test_filter_without_declared_dimension_is_unchanged(
    parsing_fixture: MetricFlowEngineTestFixture, helper: MetricQueryHelper
) -> None:
    """The SQL of a filter that references no declared dimension is not rewritten."""
    where_constraint = f"NOT ({LUX_DIM} AND {CAPACITY_DIM} > 3)  OR   {LUX_DIM}"
    _, _, filter_specs = _parse_query(parsing_fixture, "bookings_per_listing", ["metric_time"], [where_constraint])

    selected = helper.select_filter_specs_for_input_metric(
        metric_name="bookings_per_listing",
        input_metric_name="listings",
        filter_specs=filter_specs,
        exclude_dimensions=(COUNTRY,),
    )

    assert selected == filter_specs


@pytest.mark.parametrize(
    ("where_constraint", "expected_message"),
    [
        pytest.param(f"NOT ({COUNTRY_DIM} = 'US' AND {LUX_DIM})", "inside a NOT", id="not_over_mixed"),
        pytest.param(f"NOT ({COUNTRY_DIM} = 'US' OR {LUX_DIM})", "inside a NOT", id="not_over_or"),
        pytest.param(f"{LUX_DIM} XOR {COUNTRY_DIM} = 'US'", "could not be parsed", id="xor_is_not_parsed"),
        pytest.param(
            f"COALESCE({COUNTRY_DIM}, 'x') = CASE WHEN {LUX_DIM} THEN 'US' ELSE 'CA' END",
            "references both",
            id="predicate_with_both",
        ),
        pytest.param(f"{LUX_DIM} AND ({COUNTRY_DIM} = 'US'", "could not be parsed", id="unparsable"),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND {CAPACITY_DIM} >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)",
            "change its meaning",
            id="bigquery_interval_would_be_rewritten",
        ),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND {CAPACITY_DIM} = ANY(ARRAY[1, 2])",
            "change its meaning",
            id="postgres_array_would_be_rewritten",
        ),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND {CAPACITY_DIM}::int > 3",
            "change its meaning",
            id="postgres_cast_would_be_rewritten",
        ),
        pytest.param(
            f"{COUNTRY_DIM} = 'US' AND `{{{{ Dimension('listing__capacity_latest') }}}}` > 3",
            "could not be parsed",
            id="backtick_quoted_is_not_parsed",
        ),
    ],
)
def test_filter_that_cannot_be_relaxed_is_an_error(
    parsing_fixture: MetricFlowEngineTestFixture,
    helper: MetricQueryHelper,
    where_constraint: str,
    expected_message: str,
) -> None:
    """Relaxing that would tighten the filter, or can't be done reliably, is rejected with context."""
    with pytest.raises(InvalidExcludedFilterError) as exception_info:
        _select(parsing_fixture, helper, where_constraint)

    message = str(exception_info.value)
    assert "bookings_per_listing" in message
    assert "listings" in message
    assert expected_message in message


def test_filter_whose_sql_does_not_contain_the_declared_dimension_is_an_error(
    parsing_fixture: MetricFlowEngineTestFixture, helper: MetricQueryHelper
) -> None:
    """If the column of a declared dimension can't be found in the SQL, the filter is rejected instead of kept."""
    _, _, filter_specs = _parse_query(
        parsing_fixture, "bookings_per_listing", ["metric_time"], [f"{COUNTRY_DIM} = 'US' AND {LUX_DIM}"]
    )
    renamed = dataclasses.replace(filter_specs[0], where_sql="country_alias = 'US' AND listing__is_lux_latest")

    with pytest.raises(InvalidExcludedFilterError, match="were not found in the SQL"):
        helper.select_filter_specs_for_input_metric(
            metric_name="bookings_per_listing",
            input_metric_name="listings",
            filter_specs=[renamed],
            exclude_dimensions=(COUNTRY,),
        )


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_relaxed_filter_is_applied_to_declaring_input_only(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """The input that declares the dimension gets the relaxed filter, and the others get the original one."""
    elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "bookings_per_listing",
        ["metric_time"],
        where_constraints=[f"{COUNTRY_DIM} = 'US' AND {LUX_DIM}"],
    )

    assert _filter_sqls(elements["bookings"]) == {"listing__country_latest = 'US' AND listing__is_lux_latest"}
    assert _filter_sqls(elements["listings"]) == {"listing__is_lux_latest"}


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_input_and_metric_filters_are_kept_for_declaring_input(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """Filters defined on the input and on the metric are not dropped by the declaration."""
    lux_elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        "lux_booking_value_rate_expr",
        ["metric_time"],
    )
    assert len(_filter_sqls(lux_elements["bookings"])) == 1
    assert _filter_sqls(lux_elements["bookings"]) == _filter_sqls(lux_elements["average_booking_value"])
    assert _filter_sqls(lux_elements["booking_value"]) == set()

    metric_filter_elements = _input_query_elements(
        planner_class,
        parsing_fixture,
        declared_fixture,
        FILTERED_RATIO,
        ["metric_time"],
    )
    assert len(_filter_sqls(metric_filter_elements["views"])) == 1
    assert _filter_sqls(metric_filter_elements["views"]) == _filter_sqls(metric_filter_elements["bookings"])


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_declaration_on_input_with_offset_is_an_error(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """`exclude_dimensions` on an input that is itself offset is not supported."""
    with pytest.raises(ExcludeDimensionsWithTimeOffsetError) as exception_info:
        _input_query_elements(
            planner_class,
            parsing_fixture,
            declared_fixture,
            "booking_fees_last_week_per_booker_this_week",
            ["metric_time"],
        )

    assert "booking_value" in str(exception_info.value)


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_declaration_under_offset_parent_is_an_error(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """An input with a declaration can't sit under a metric that is offset."""
    with pytest.raises(ExcludeDimensionsWithTimeOffsetError) as exception_info:
        _input_query_elements(
            planner_class,
            parsing_fixture,
            declared_fixture,
            "bookings_offset_twice",
            ["metric_time"],
        )

    message = str(exception_info.value)
    assert "bookings" in message
    assert "bookings_offset_once" in message


def _dimension_spec(name: str) -> DimensionSpec:
    *entity_links, element_name = name.split("__")
    return DimensionSpec(
        element_name=element_name, entity_links=tuple(EntityReference(entity_link) for entity_link in entity_links)
    )


@pytest.fixture(scope="module")
def helper(declared_fixture: MetricFlowEngineTestFixture) -> MetricQueryHelper:  # noqa: D103
    return MetricQueryHelper(metric_lookup=declared_fixture.semantic_manifest_lookup.metric_lookup)


@pytest.mark.parametrize("is_lux_name", [IS_LUX, "is_lux_latest"])
def test_group_by_of_input_that_does_not_have_the_declared_dimension(
    helper: MetricQueryHelper, is_lux_name: str
) -> None:
    """Removing is by name, so the group-by is the same whether or not the input's model has the dimension."""
    queried = (_dimension_spec(COUNTRY), _dimension_spec(IS_LUX))

    assert set(helper.resolve_group_by_specs_for_input_metric(queried, (is_lux_name,))) == {_dimension_spec(COUNTRY)}
    assert set(helper.resolve_group_by_specs_for_input_metric(queried, ("booking__is_instant",))) == set(queried)
    assert set(helper.resolve_group_by_specs_for_input_metric(queried, ("is_instant",))) == set(queried)
    assert set(helper.resolve_group_by_specs_for_input_metric(queried, ())) == set(queried)


def test_filter_on_dimension_reached_through_another_entity_path_is_not_dropped(  # noqa: D103
    parsing_fixture: MetricFlowEngineTestFixture, helper: MetricQueryHelper
) -> None:
    _, _, filter_specs = _parse_query(
        parsing_fixture, "bookings_per_view", ["metric_time"], ["{{ Dimension('listing__is_lux_latest') }}"]
    )

    selected = helper.select_filter_specs_for_input_metric(
        metric_name="bookings_per_view",
        input_metric_name="views",
        filter_specs=filter_specs,
        exclude_dimensions=("view__listing__is_lux_latest",),
    )

    assert selected == filter_specs


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_pushdown_state_of_declaring_input_has_only_applicable_filters(
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    """Predicates for the declared dimension are not pushed down into the declaring input."""
    query_spec, filter_spec_factory, query_filter_specs = _parse_query(
        parsing_fixture,
        "bookings_per_listing",
        ["metric_time"],
        [
            "{{ Dimension('listing__country_latest') }} = 'US'",
            "{{ Dimension('listing__is_lux_latest') }}",
        ],
    )
    assert len(query_filter_specs) == 2
    planner = _planner(planner_class, declared_fixture)
    me_plan = planner.build_plan(
        metric_specs=(MetricSpec.create(element_name="bookings_per_listing"),),
        group_by_item_specs=query_spec.linkable_specs.as_tuple,
        predicate_pushdown_state=PredicatePushdownState.create(where_filter_specs=query_filter_specs),
        filter_spec_factory=filter_spec_factory,
    )
    elements: Mapping[str, Sequence[WhereFilterSpec]] = {
        element.metric_name: element.predicate_pushdown_state.where_filter_specs
        for node in me_plan.nodes_in_dfs_order()
        for element in node.output_query_elements
    }

    assert set(elements["bookings"]) == set(query_filter_specs)
    assert {spec.where_sql for spec in elements["listings"]} == {
        spec.where_sql for spec in query_filter_specs if COUNTRY not in spec.where_sql
    }
    assert len(elements["listings"]) == 1


def _fixture_declaring(
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
    declarations: Sequence[tuple[str, str, Sequence[str]]],
) -> MetricFlowEngineTestFixture:
    manifest = copy.deepcopy(parsing_fixture.semantic_manifest)
    for metric_name, input_name, names in declarations:
        _declare(manifest, metric_name, input_name, *names)
    return MetricFlowEngineTestFixture.from_parameters(sql_client, manifest)


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_declared_name_matching_no_group_by_item_is_an_error(  # noqa: D103
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    declared_fixture = _fixture_declaring(
        parsing_fixture, sql_client, [("bookings_per_listing", "listings", ["booking__listing__country_latest"])]
    )

    with pytest.raises(UnmatchedExcludeDimensionsError) as exception_info:
        _input_query_elements(planner_class, parsing_fixture, declared_fixture, "bookings_per_listing", ["metric_time"])

    message = str(exception_info.value)
    assert "listings" in message
    assert "booking__listing__country_latest" in message
    assert "Examples of dimensions that can be used" in message


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_unmatched_name_on_one_of_several_declaring_inputs_is_an_error(  # noqa: D103
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    declared_fixture = _fixture_declaring(
        parsing_fixture,
        sql_client,
        [
            ("lux_booking_value_rate_expr", "booking_value", [COUNTRY]),
            ("lux_booking_value_rate_expr", "bookings", ["booking__listing__is_lux_latest"]),
        ],
    )

    with pytest.raises(UnmatchedExcludeDimensionsError) as exception_info:
        _input_query_elements(
            planner_class, parsing_fixture, declared_fixture, "lux_booking_value_rate_expr", ["metric_time"]
        )

    message = str(exception_info.value)
    assert "'bookings'" in message
    assert "booking__listing__is_lux_latest" in message
    assert "'booking_value'" not in message


@pytest.mark.parametrize("planner_class", PLANNER_CLASSES)
def test_declared_name_reachable_only_through_another_input_passes(  # noqa: D103
    planner_class: type[MetricEvaluationPlanner],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    declared_fixture = _fixture_declaring(
        parsing_fixture, sql_client, [(FILTERED_RATIO, "views", ["booking__is_instant"])]
    )

    elements = _input_query_elements(planner_class, parsing_fixture, declared_fixture, FILTERED_RATIO, ["metric_time"])

    assert set(elements) == {"bookings", "views"}


@pytest.mark.parametrize(
    "optimizations",
    [frozenset[DataflowPlanOptimization](), frozenset({DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION})],
)
@pytest.mark.parametrize(
    ("metric_name", "group_by_names", "where_constraints", "is_joined_to_base"),
    [
        ("bookings_per_listing", ["metric_time"], None, False),
        ("bookings_per_listing", ["metric_time"], ["{{ Dimension('listing__is_lux_latest') }}"], False),
        ("bookings_per_listing", [COUNTRY], None, True),
        ("bookings_per_listing", ["metric_time"], ["{{ Dimension('listing__country_latest') }} = 'US'"], True),
        ("lux_booking_value_rate_expr", [COUNTRY], None, True),
        ("lux_booking_value_rate_expr", [IS_LUX], None, True),
        ("lux_booking_value_rate_expr", ["metric_time"], None, False),
    ],
)
def test_inputs_are_joined_to_base_only_when_a_declared_dimension_is_used(  # noqa: D103
    optimizations: frozenset[DataflowPlanOptimization],
    metric_name: str,
    group_by_names: Sequence[str],
    where_constraints: Optional[Sequence[str]],
    is_joined_to_base: bool,
    parsing_fixture: MetricFlowEngineTestFixture,
    declared_fixture: MetricFlowEngineTestFixture,
) -> None:
    query_spec = parsing_fixture.query_parser.parse_and_validate_query(
        metric_names=[metric_name],
        group_by_names=group_by_names,
        where_constraint_strs=where_constraints,
    ).query_spec

    plan = declared_fixture.dataflow_plan_builder.build_plan(query_spec, optimizations=optimizations)

    assert (JoinToBaseAggregatedOutputsNode.__name__ in plan.structure_text()) == is_joined_to_base


LUX_RATE = "lux_booking_value_rate_expr"
LUX_ONLY = "{{ Dimension('listing__is_lux_latest') }}"
ALL_OPTIMIZATIONS = [
    frozenset[DataflowPlanOptimization](),
    frozenset({DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION}),
]


def _join_nodes(
    fixture: MetricFlowEngineTestFixture,
    parsing_fixture: MetricFlowEngineTestFixture,
    optimizations: frozenset[DataflowPlanOptimization],
    group_by_names: Sequence[str],
    where_constraints: Optional[Sequence[str]] = None,
) -> list[JoinToBaseAggregatedOutputsNode]:
    query_spec = parsing_fixture.query_parser.parse_and_validate_query(
        metric_names=[LUX_RATE],
        group_by_names=group_by_names,
        where_constraint_strs=where_constraints,
    ).query_spec
    plan = fixture.dataflow_plan_builder.build_plan(query_spec, optimizations=optimizations)
    nodes = [plan.sink_node]
    join_nodes: list[JoinToBaseAggregatedOutputsNode] = []
    while nodes:
        node = nodes.pop()
        if type(node) is JoinToBaseAggregatedOutputsNode:
            join_nodes.append(node)
        nodes.extend(node.parent_nodes)
    return join_nodes


@pytest.mark.parametrize("optimizations", ALL_OPTIMIZATIONS)
def test_inputs_declaring_the_same_dimension_are_joined_once_each(
    optimizations: frozenset[DataflowPlanOptimization],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    """The planner can compute several inputs in one node, which must not be joined to the base once per input."""
    fixture = _fixture_declaring(
        parsing_fixture,
        sql_client,
        [(LUX_RATE, "average_booking_value", [COUNTRY]), (LUX_RATE, "bookings", [COUNTRY])],
    )

    (join_node,) = _join_nodes(fixture, parsing_fixture, optimizations, [COUNTRY])

    declared_input_nodes = join_node.declared_input_nodes
    assert len(declared_input_nodes) == len(set(declared_input_nodes))
    expected_count = 1 if DataflowPlanOptimization.PASSTHROUGH_METRIC_EVALUATION in optimizations else 2
    assert len(declared_input_nodes) == expected_count


@pytest.mark.parametrize("optimizations", ALL_OPTIMIZATIONS)
def test_input_that_only_filters_on_its_declared_dimension_is_the_base_when_every_input_is_declared(
    optimizations: frozenset[DataflowPlanOptimization],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    """The base has all the group-by items, even if the input excludes a dimension that the query filters on."""
    fixture = _fixture_declaring(
        parsing_fixture,
        sql_client,
        [
            (LUX_RATE, "average_booking_value", [COUNTRY]),
            (LUX_RATE, "bookings", [COUNTRY]),
            (LUX_RATE, "booking_value", [IS_LUX]),
        ],
    )

    (join_node,) = _join_nodes(fixture, parsing_fixture, optimizations, [COUNTRY], [LUX_ONLY])

    base_node = join_node.base_node
    assert COUNTRY in {spec.dunder_name for spec in base_node.aggregated_to_elements}
    assert "booking_value" in base_node.structure_text()
    assert "average_booking_value" not in base_node.structure_text()


@pytest.mark.parametrize("optimizations", ALL_OPTIMIZATIONS)
def test_group_by_item_that_every_input_excludes_is_an_error(
    optimizations: frozenset[DataflowPlanOptimization],
    parsing_fixture: MetricFlowEngineTestFixture,
    sql_client: SqlClient,
) -> None:
    """No input has the group-by item, so there is no base to join to."""
    fixture = _fixture_declaring(
        parsing_fixture,
        sql_client,
        [
            (LUX_RATE, "average_booking_value", [COUNTRY]),
            (LUX_RATE, "bookings", [COUNTRY]),
            (LUX_RATE, "booking_value", [COUNTRY]),
        ],
    )

    with pytest.raises(InvalidQueryException):
        _join_nodes(fixture, parsing_fixture, optimizations, [COUNTRY])
