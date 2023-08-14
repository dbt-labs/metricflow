from __future__ import annotations

from metricflow.specs.group_by_order_by_dimension import GroupByOrderByDimensionFactory


def test_to_str() -> None:  # noqa
    dimension = GroupByOrderByDimensionFactory().create("revenue").grain("month")
    assert str(dimension) == "revenue__month"
