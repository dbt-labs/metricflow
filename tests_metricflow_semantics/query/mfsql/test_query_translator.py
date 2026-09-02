from __future__ import annotations

import pytest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.mfsql.query_translator import MfsqlQueryTranslationError, translate_mfsql_query


def test_translate_full_query(simple_semantic_manifest_lookup: SemanticManifestLookup) -> None:
    """Exercises every clause at once: SELECT, FROM, WHERE, both ORDER BY spellings, and LIMIT."""
    result = translate_mfsql_query(
        """
        SELECT bookings, booking__is_instant, metric_time__month
        FROM metrics
        WHERE booking__is_instant = true
        ORDER BY -metric_time__month, bookings DESC
        LIMIT 10
        """,
        simple_semantic_manifest_lookup,
    )

    assert result.metric_names == ("bookings",)
    assert result.group_by_names == ("booking__is_instant", "metric_time__month")
    assert result.where_constraint_strs == ("{{ Dimension('booking__is_instant') }} = TRUE",)
    assert result.order_by_names == ("-metric_time__month", "-bookings")
    assert result.limit == 10


def test_translate_query_without_optional_clauses(simple_semantic_manifest_lookup: SemanticManifestLookup) -> None:
    """WHERE, ORDER BY, and LIMIT are all optional; an entity (`listing`) is just a non-metric group-by name."""
    result = translate_mfsql_query("SELECT bookings, listing FROM metrics", simple_semantic_manifest_lookup)

    assert result.metric_names == ("bookings",)
    assert result.group_by_names == ("listing",)
    assert result.where_constraint_strs == ()
    assert result.order_by_names == ()
    assert result.limit is None


@pytest.mark.parametrize(
    "sql,match",
    [
        ("SELECT bookings FROM not_metrics", "requires `FROM metrics`"),
        ("SELECT bookings FROM metrics m", "requires `FROM metrics`"),
        ("SELECT bookings FROM some_schema.metrics", "requires `FROM metrics`"),
        ("SELECT * FROM metrics", "does not support `SELECT \\*`"),
        ("SELECT bookings AS b FROM metrics", "bare column names"),
        ("SELECT t.bookings FROM metrics", "table-qualified columns"),
        ("SELECT DISTINCT bookings FROM metrics", "does not support DISTINCT"),
        ("SELECT bookings FROM metrics JOIN other ON bookings.a = other.a", "does not support JOINs"),
        ("SELECT bookings FROM metrics GROUP BY bookings", "does not support GROUP BY"),
        ("SELECT bookings FROM metrics HAVING bookings > 1", "does not support HAVING"),
        ("SELECT bookings FROM metrics UNION SELECT bookings FROM metrics", "single `SELECT"),
        ("WITH x AS (SELECT 1) SELECT bookings FROM metrics", "does not support CTEs"),
        ("SELECT bookings FROM metrics ORDER BY bookings NULLS LAST", "explicit NULLS"),
        ("SELECT bookings FROM metrics ORDER BY -bookings DESC", "leading `-` or `DESC`, not both"),
        ("SELECT bookings FROM metrics LIMIT 'ten'", "LIMIT must be a plain integer"),
    ],
)
def test_translate_rejects_unsupported_shapes(
    sql: str, match: str, simple_semantic_manifest_lookup: SemanticManifestLookup
) -> None:
    """Statement shapes outside mfsql's supported grammar should raise with a specific, actionable message."""
    with pytest.raises(MfsqlQueryTranslationError, match=match):
        translate_mfsql_query(sql, simple_semantic_manifest_lookup)
