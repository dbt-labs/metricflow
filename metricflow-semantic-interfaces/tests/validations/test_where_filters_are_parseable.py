from __future__ import annotations

import copy
import logging

import pytest
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from metricflow_semantic_interfaces.implementations.saved_query import (
    PydanticSavedQuery,
    PydanticSavedQueryQueryParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import (
    check_no_errors_or_warnings,
    check_only_one_error_with_message,
    check_only_one_warning_with_message,
    find_metric_with,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)
from metricflow_semantic_interfaces.validations.where_filters import WhereFiltersAreParseable

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Metric validations
# ------------------------------------------------------------------------------


def test_metric_where_filter_validations_happy(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    results = validator.validate_semantic_manifest(simple_semantic_manifest__with_primary_transforms)
    assert not results.has_blocking_issues


def test_where_filter_validations_bad_base_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(manifest, lambda metric: metric.filter is not None)
    assert metric.filter is not None
    assert len(metric.filter.where_filters) > 0
    metric.filter.where_filters[0].where_sql_template = "{{ dimension('too', 'many', 'variables', 'to', 'handle') }}"
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(SemanticManifestValidationException, match=f"trying to parse filter of metric `{metric.name}`"):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_measure_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.measure is not None
    )
    assert metric.type_params.measure is not None
    metric.type_params.measure.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException,
        match=f"trying to parse filter of measure input `{metric.type_params.measure.name}` on metric `{metric.name}`",
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_numerator_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.numerator is not None
    )
    assert metric.type_params.numerator is not None
    metric.type_params.numerator.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException, match=f"trying to parse the numerator filter on metric `{metric.name}`"
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_denominator_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.denominator is not None
    )
    assert metric.type_params.denominator is not None
    metric.type_params.denominator.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException, match=f"trying to parse the denominator filter on metric `{metric.name}`"
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_input_metric_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest,
        lambda metric: metric.type_params is not None
        and metric.type_params.metrics is not None
        and len(metric.type_params.metrics) > 0,
    )
    assert metric.type_params.metrics is not None
    input_metric = metric.type_params.metrics[0]
    input_metric.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException,
        match=f"trying to parse filter for input metric `{input_metric.name}` on metric `{metric.name}`",
    ):
        validator.checked_validations(manifest)


def test_metric_where_filter_validations_invalid_granularity(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest,
        lambda metric: metric.type_params is not None
        and metric.type_params.metrics is not None
        and len(metric.type_params.metrics) > 0,
    )
    assert metric.type_params.metrics is not None
    input_metric = metric.type_params.metrics[0]
    input_metric.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'cool') }}"),
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'month') }}"),
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'MONTH') }}"),
            PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'martian_day') }}"),
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    issues = validator.validate_semantic_manifest(manifest)
    assert not issues.has_blocking_issues
    assert len(issues.warnings) == 1
    assert "`cool` is not a valid granularity name" in issues.warnings[0].message


# ------------------------------------------------------------------------------
# Saved Query validations
# ------------------------------------------------------------------------------


def test_saved_query_with_happy_filter(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[
                        PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'hour') }}"),
                        PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'martian_day') }}"),
                    ]
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    check_no_errors_or_warnings(manifest_validator.validate_semantic_manifest(manifest))


def test_saved_query_validates_granularity_name_despite_case(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[
                        PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'DAY') }}"),
                    ]
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    check_no_errors_or_warnings(manifest_validator.validate_semantic_manifest(manifest))


def test_invalid_where_in_saved_query(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ invalid_jinja }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "trying to parse a filter in saved query",
    )


def test_saved_query_where_filter_validations_invalid_granularity(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)

    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[
                        PydanticWhereFilter(where_sql_template="{{ TimeDimension('metric_time', 'cool') }}"),
                    ]
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    check_only_one_warning_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "is not a valid granularity name",
    )


def test_metric_filter_error(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["listings"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Metric('bookings') }} > 2")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while trying to parse a filter in saved query",
    )
