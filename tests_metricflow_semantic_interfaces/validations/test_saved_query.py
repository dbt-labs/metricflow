from __future__ import annotations

import copy
import logging

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
from metricflow_semantic_interfaces.test_utils import check_only_one_error_with_message
from metricflow_semantic_interfaces.validations.saved_query import SavedQueryRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)

logger = logging.getLogger(__name__)


def test_invalid_metric_in_saved_query(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["invalid_metric"],
                group_by=["Dimension('booking__is_instant')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest), "is not a valid metric name."
    )


def test_invalid_group_by_element_in_saved_query(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["Dimension('booking__invalid_dimension')"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "is not a valid group-by name.",
    )


def test_invalid_group_by_format_in_saved_query(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    manifest.saved_queries = [
        PydanticSavedQuery(
            name="Example Saved Query",
            description="Example description.",
            query_params=PydanticSavedQueryQueryParams(
                metrics=["bookings"],
                group_by=["invalid_format"],
                where=PydanticWhereFilterIntersection(
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Dimension('booking__is_instant') }}")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    check_only_one_error_with_message(
        manifest_validator.validate_semantic_manifest(manifest),
        "An error occurred while trying to parse a group-by in saved query",
    )


def test_metric_filter_success(  # noqa: D103
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
                    where_filters=[PydanticWhereFilter(where_sql_template="{{ Metric('bookings', ['listing']) }} > 2")],
                ),
            ),
        ),
    ]

    manifest_validator = SemanticManifestValidator[PydanticSemanticManifest]([SavedQueryRule()])
    results = manifest_validator.validate_semantic_manifest(manifest)
    assert len(results.warnings) == 0
    assert len(results.errors) == 0
    assert len(results.future_errors) == 0
