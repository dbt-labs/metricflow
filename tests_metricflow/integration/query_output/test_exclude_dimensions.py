from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.errors.error_classes import InvalidExcludedFilterError, InvalidQueryException
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.dunder_column_association_resolver import DunderColumnAssociationResolver
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.time_helpers import ConfigurableTimeSource
from metricflow_semantics.time.time_spine_source import TimeSpineSource

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from metricflow_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantic_interfaces.test_utils import as_datetime
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from tests_metricflow.snapshot_utils import assert_sql_snapshot_equal

RATIO = "bookings_per_view"
DERIVED = "views_times_booking_value"
THREE_INPUT_DERIVED = "lux_booking_value_rate_expr"


def _engine(
    sql_client: SqlClient,
    manifest: PydanticSemanticManifest,
    column_association_resolver: ColumnAssociationResolver,
) -> MetricFlowEngine:
    return MetricFlowEngine(
        semantic_manifest_lookup=SemanticManifestLookup(manifest),
        sql_client=sql_client,
        column_association_resolver=column_association_resolver,
        time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
        consistent_id_enumeration=True,
    )


def _declare_on_denominator(manifest: PydanticSemanticManifest, *names: str) -> PydanticSemanticManifest:
    declared = copy.deepcopy(manifest)
    ratio = next(metric for metric in declared.metrics if metric.name == RATIO)
    assert ratio.type_params.denominator is not None
    ratio.type_params.denominator.exclude_dimensions = tuple(names)
    return declared


def _rows(
    engine: MetricFlowEngine,
    metric_names: list[str],
    group_by_names: list[str],
    where: Optional[str] = None,
) -> Mapping[tuple[object, ...], tuple[object, ...]]:
    result = engine.query(
        MetricFlowQueryRequest.create(
            metric_names=metric_names,
            group_by_names=group_by_names,
            where_constraints=[where] if where else None,
        )
    ).result_df
    assert result is not None, "Unexpected empty result."
    column_names = [name.lower() for name in result.column_names]
    key_indexes = [column_names.index(name) for name in group_by_names]
    value_indexes = [index for index in range(len(column_names)) if index not in key_indexes]
    return {
        tuple(row[index] for index in key_indexes): tuple(row[index] for index in value_indexes) for row in result.rows
    }


@pytest.fixture
def engines(  # noqa: D103
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest: PydanticSemanticManifest,
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
    mf_test_configuration: MetricFlowTestConfiguration,
) -> tuple[MetricFlowEngine, MetricFlowEngine]:
    resolver = DunderColumnAssociationResolver()
    return (
        _engine(sql_client, simple_semantic_manifest, resolver),
        _engine(sql_client, _declare_on_denominator(simple_semantic_manifest, "is_instant"), resolver),
    )


def test_denominator_keeps_shared_dimension(engines: tuple[MetricFlowEngine, MetricFlowEngine]) -> None:
    """Dimensions the denominator has still split it, while the declared one does not."""
    plain, declared = engines
    bookings = _rows(plain, ["bookings"], ["listing__country_latest", "booking__is_instant"])
    views = _rows(plain, ["views"], ["listing__country_latest"])

    rates = _rows(declared, [RATIO], ["listing__country_latest", "booking__is_instant"])

    assert set(rates) == set(bookings)
    for (country, is_instant), (rate,) in rates.items():
        assert rate == pytest.approx(
            float(bookings[(country, is_instant)][0]) / float(views[(country,)][0])  # type: ignore[arg-type]
        )


def test_where_on_declared_dimension_applies_to_numerator_only(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """A condition on the declared dimension filters the numerator and leaves the denominator whole."""
    plain, declared = engines
    bookings = _rows(plain, ["bookings"], [], where="{{ Dimension('booking__is_instant') }}")
    views = _rows(plain, ["views"], [])

    rates = _rows(declared, [RATIO], [], where="{{ Dimension('booking__is_instant') }}")

    assert rates[()][0] == pytest.approx(float(bookings[()][0]) / float(views[()][0]))  # type: ignore[arg-type]


def test_where_mixing_declared_and_other_dimensions_is_relaxed_for_denominator(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """In an AND, the term on the declared dimension is dropped from the denominator and kept in the numerator."""
    plain, declared = engines
    is_instant = "{{ Dimension('booking__is_instant') }}"
    country = "{{ Dimension('listing__country_latest') }} = 'us'"
    bookings = _rows(plain, ["bookings"], [], where=f"{is_instant} AND {country}")
    views = _rows(plain, ["views"], [], where=country)
    unfiltered_views = _rows(plain, ["views"], [])
    assert views[()] != unfiltered_views[()]

    rates = _rows(declared, [RATIO], [], where=f"{is_instant} AND {country}")

    assert rates[()][0] == pytest.approx(float(bookings[()][0]) / float(views[()][0]))  # type: ignore[arg-type]


def test_where_with_declared_dimension_in_each_or_branch_is_relaxed_for_denominator(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """Replacing the declared dimension's predicates with TRUE turns `(a AND x) OR (b AND y)` into `x OR y`."""
    plain, declared = engines
    is_instant = "{{ Dimension('booking__is_instant') }}"
    country_us = "{{ Dimension('listing__country_latest') }} = 'us'"
    country_ca = "{{ Dimension('listing__country_latest') }} = 'ca'"
    where = f"({is_instant} AND {country_us}) OR (NOT {is_instant} AND {country_ca})"
    bookings = _rows(plain, ["bookings"], [], where=where)
    views = _rows(plain, ["views"], [], where=f"{country_us} OR {country_ca}")

    rates = _rows(declared, [RATIO], [], where=where)

    assert rates[()][0] == pytest.approx(float(bookings[()][0]) / float(views[()][0]))  # type: ignore[arg-type]


def test_where_that_cannot_be_relaxed_is_refused_with_its_own_error_type(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """A NOT over a compound that holds the declared dimension is refused with a type callers can catch."""
    _, declared = engines
    is_instant = "{{ Dimension('booking__is_instant') }}"
    country = "{{ Dimension('listing__country_latest') }} = 'us'"

    with pytest.raises(InvalidExcludedFilterError):
        _rows(declared, [RATIO], [], where=f"NOT ({is_instant} OR {country})")


def test_refusal_for_an_unknown_name_is_not_the_excluded_filter_error(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """A value that spells the declaration keyword does not turn another refusal into the excluded filter error."""
    _, declared = engines

    with pytest.raises(InvalidQueryException) as exception_info:
        _rows(declared, [RATIO], [], where="{{ Dimension('no_such__dimension') }} = '`exclude_dimensions`'")

    assert not isinstance(exception_info.value, InvalidExcludedFilterError)


def _rate_by_day(engine: MetricFlowEngine, where: Optional[str] = None) -> Mapping[object, object]:
    return {group[0]: values[0] for group, values in _rows(engine, [RATIO], ["metric_time__day"], where=where).items()}


def test_group_existing_only_in_denominator_is_kept_when_declared_dimension_is_unused(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """A group that only the denominator has stays in the output when the query doesn't use the excluded dimension."""
    plain, declared = engines
    bookings = _rows(plain, ["bookings"], ["metric_time__day"])
    views = _rows(plain, ["views"], ["metric_time__day"])
    days_only_in_denominator = {day[0] for day in set(views) - set(bookings)}
    assert days_only_in_denominator

    rates = _rate_by_day(declared)

    assert rates == _rate_by_day(plain)
    assert set(rates) == {day[0] for day in set(bookings) | set(views)}
    for day in days_only_in_denominator:
        assert rates[day] is None


def test_group_existing_only_in_denominator_is_dropped_when_declared_dimension_is_filtered(
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """Using the excluded dimension in a filter keeps the numerator as the base of the join."""
    plain, declared = engines
    where = "{{ Dimension('booking__is_instant') }}"
    bookings = _rows(plain, ["bookings"], ["metric_time__day"], where=where)
    views = _rows(plain, ["views"], ["metric_time__day"])
    assert set(views) - set(bookings)

    rates = _rate_by_day(declared, where=where)

    assert set(rates) == {day[0] for day in bookings}
    for day, rate in rates.items():
        if (day,) in views:
            assert rate == pytest.approx(float(bookings[(day,)][0]) / float(views[(day,)][0]))  # type: ignore[arg-type]
        else:
            assert rate is None


@pytest.mark.duckdb_only
@pytest.mark.sql_engine_snapshot
def test_sql_is_unchanged_when_declared_dimension_is_unused(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    engines: tuple[MetricFlowEngine, MetricFlowEngine],
    sql_client: SqlClient,
) -> None:
    """The plan of a query that doesn't use the excluded dimension is the one of the metric without it."""
    plain, declared = engines
    query = MetricFlowQueryRequest.create(metric_names=[RATIO], group_by_names=["metric_time__day"])

    declared_sql = declared.explain(query).sql_statement.sql

    assert declared_sql == plain.explain(query).sql_statement.sql
    assert "FULL OUTER JOIN" in declared_sql
    assert_sql_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        sql=declared_sql,
        sql_engine=sql_client.sql_engine_type,
    )


def test_dimension_list_includes_declared_dimension(engines: tuple[MetricFlowEngine, MetricFlowEngine]) -> None:
    """The dimensions of a metric include the declared one only when the input declares it."""
    plain, declared = engines
    plain_names = {dimension.dunder_name for dimension in plain.list_dimensions(metric_names=[RATIO])}
    declared_names = {dimension.dunder_name for dimension in declared.list_dimensions(metric_names=[RATIO])}

    assert "booking__is_instant" not in plain_names
    assert "booking__is_instant" in declared_names


@pytest.fixture
def derived_engines(  # noqa: D103
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest: PydanticSemanticManifest,
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
    mf_test_configuration: MetricFlowTestConfiguration,
) -> tuple[MetricFlowEngine, MetricFlowEngine]:
    resolver = DunderColumnAssociationResolver()
    declared_manifest = copy.deepcopy(simple_semantic_manifest)
    derived = next(metric for metric in declared_manifest.metrics if metric.name == DERIVED)
    views_input = next(input_metric for input_metric in derived.input_metrics if input_metric.name == "views")
    views_input.exclude_dimensions = ("is_instant",)
    return (
        _engine(sql_client, simple_semantic_manifest, resolver),
        _engine(sql_client, declared_manifest, resolver),
    )


def test_derived_metric_input_ignores_declared_dimension(
    derived_engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """A derived metric multiplies each group's value by the overall value of the input that excludes the dimension."""
    plain, declared = derived_engines
    booking_value = _rows(plain, ["booking_value"], ["booking__is_instant"])
    views = _rows(plain, ["views"], [])

    values = _rows(declared, [DERIVED], ["booking__is_instant"])

    assert set(values) == set(booking_value)
    for group, (value,) in values.items():
        assert value == pytest.approx(float(booking_value[group][0]) * float(views[()][0]))  # type: ignore[arg-type]


@pytest.fixture
def three_input_engines(  # noqa: D103
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest: PydanticSemanticManifest,
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
    mf_test_configuration: MetricFlowTestConfiguration,
) -> tuple[MetricFlowEngine, MetricFlowEngine]:
    resolver = DunderColumnAssociationResolver()
    declared_manifest = copy.deepcopy(simple_semantic_manifest)
    derived = next(metric for metric in declared_manifest.metrics if metric.name == THREE_INPUT_DERIVED)
    total_input = next(input_metric for input_metric in derived.input_metrics if input_metric.name == "booking_value")
    total_input.exclude_dimensions = ("country_latest",)
    return (
        _engine(sql_client, simple_semantic_manifest, resolver),
        _engine(sql_client, declared_manifest, resolver),
    )


def test_two_inputs_without_declaration_are_combined_before_the_join(
    three_input_engines: tuple[MetricFlowEngine, MetricFlowEngine],
) -> None:
    """Inputs that don't exclude the dimension are combined, and the declared input is joined onto them."""
    plain, declared = three_input_engines
    lux_where = "{{ Dimension('listing__is_lux_latest') }}"
    average = _rows(plain, ["average_booking_value"], ["listing__country_latest"], where=lux_where)
    bookings = _rows(plain, ["bookings"], ["listing__country_latest"], where=lux_where)
    total = float(_rows(plain, ["booking_value"], [])[()][0])  # type: ignore[arg-type]

    values = _rows(declared, [THREE_INPUT_DERIVED], ["listing__country_latest"])

    assert set(values) == set(average) | set(bookings)
    for group, (value,) in values.items():
        expected = float(average[group][0]) * float(bookings[group][0]) / total  # type: ignore[arg-type]
        assert value == pytest.approx(expected)


SHARE_OF_TOTAL = "bookings_share_of_total"


def _with_share_of_total(manifest: PydanticSemanticManifest, **denominator: object) -> PydanticSemanticManifest:
    """Adds a ratio that divides `bookings` by `bookings` with the denominator excluding `is_instant`."""
    changed = copy.deepcopy(manifest)
    ratio = copy.deepcopy(next(metric for metric in changed.metrics if metric.name == RATIO))
    ratio.name = SHARE_OF_TOTAL
    assert ratio.type_params.numerator is not None and ratio.type_params.denominator is not None
    ratio.type_params.numerator.name = "bookings"
    ratio.type_params.denominator.name = "bookings"
    ratio.type_params.denominator.exclude_dimensions = ("is_instant",)
    for key, value in denominator.items():
        setattr(ratio.type_params.denominator, key, value)
    changed.metrics = [*changed.metrics, ratio]
    return changed


def test_share_of_total_divides_by_the_total(
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    """The same metric as the numerator and as the excluding denominator, told apart by an alias, gives the share."""
    engine = _engine(
        sql_client,
        _with_share_of_total(simple_semantic_manifest, alias="all_bookings"),
        DunderColumnAssociationResolver(),
    )
    shares = _rows(engine, [SHARE_OF_TOTAL], ["booking__is_instant"])
    bookings = _rows(engine, ["bookings"], ["booking__is_instant"])
    total = sum(float(values[0]) for values in bookings.values())  # type: ignore[arg-type,misc]
    for key, values in shares.items():
        assert float(values[0]) == pytest.approx(float(bookings[key][0]) / total)  # type: ignore[arg-type]


def test_group_with_a_null_time_dimension_is_joined_to_the_denominator(
    sql_client: SqlClient,
    create_source_tables: bool,
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    """A time dimension reached through an entity link is NULL for rows without a match, and that group still joins."""
    engine = _engine(
        sql_client,
        _with_share_of_total(simple_semantic_manifest, alias="all_bookings"),
        DunderColumnAssociationResolver(),
    )
    group_by = ["booking__is_instant", "listing__created_at__day"]

    shares = _rows(engine, [SHARE_OF_TOTAL], group_by)
    bookings = _rows(engine, ["bookings"], group_by)
    totals: dict[object, float] = {}
    for (_, created_at), values in bookings.items():
        totals[created_at] = totals.get(created_at, 0.0) + float(values[0])  # type: ignore[arg-type]

    null_time_groups = [key for key in bookings if key[1] is None]
    assert null_time_groups
    for key in null_time_groups:
        assert shares[key][0] is not None
        assert float(shares[key][0]) == pytest.approx(float(bookings[key][0]) / totals[None])  # type: ignore[arg-type]
