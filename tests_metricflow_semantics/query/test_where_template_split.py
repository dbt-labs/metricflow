from __future__ import annotations

from metricflow_semantics.query.query_parser import _split_top_level_and


def test_top_level_and_splits_and_between_stays_one_predicate() -> None:
    """Top-level AND splits, and the AND inside BETWEEN stays in one predicate."""
    parts = _split_top_level_and("mf_jinja_0 = '1' AND mf_jinja_1 BETWEEN '2020-01-01' AND '2020-01-02'")
    assert parts == [
        "mf_jinja_0 = '1'",
        "mf_jinja_1 BETWEEN '2020-01-01' AND '2020-01-02'",
    ]


def test_and_inside_or_stays_one_predicate() -> None:
    """AND that belongs to an OR expression is not a split point."""
    parts = _split_top_level_and("(a AND b) OR c")
    assert len(parts) == 1
    assert "AND" in parts[0]
    assert "OR" in parts[0]


def test_jinja_blocks_are_restored_and_not_split() -> None:
    """Jinja calls are restored after parsing, including a time predicate with BETWEEN."""
    parts = _split_top_level_and(
        "{{ Entity('listing') }} = '1' AND "
        "{{ TimeDimension('metric_time', 'day') }} BETWEEN '2020-01-01' AND '2020-01-02'"
    )
    assert parts == [
        "{{ Entity('listing') }} = '1'",
        "{{ TimeDimension('metric_time', 'day') }} BETWEEN '2020-01-01' AND '2020-01-02'",
    ]


def test_unparseable_template_is_kept_whole() -> None:
    """A template sqlglot cannot parse is returned as one predicate."""
    assert _split_top_level_and("{{ Entity('listing') }} =") == ["{{ Entity('listing') }} ="]
