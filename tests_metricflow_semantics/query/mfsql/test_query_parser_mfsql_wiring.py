from __future__ import annotations

from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser


def test_parse_and_validate_mfsql_query_matches_the_equivalent_flag_based_query(
    simple_semantic_manifest_lookup: SemanticManifestLookup,
) -> None:
    """The mfsql entry point should produce the exact same query spec as the equivalent flag-based call."""
    parser = MetricFlowQueryParser(semantic_manifest_lookup=simple_semantic_manifest_lookup)

    mfsql_result = parser.parse_and_validate_mfsql_query(
        "SELECT METRIC(bookings), booking__is_instant FROM metrics WHERE booking__is_instant = true "
        "ORDER BY -METRIC(bookings)"
    )
    flag_based_result = parser.parse_and_validate_query(
        metric_names=["bookings"],
        group_by_names=["booking__is_instant"],
        where_constraint_strs=["{{ Dimension('booking__is_instant') }} = TRUE"],
        order_by_names=["-bookings"],
    )

    assert mfsql_result.query_spec == flag_based_result.query_spec
