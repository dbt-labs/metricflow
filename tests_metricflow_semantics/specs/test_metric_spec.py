from __future__ import annotations

import logging

from metricflow_semantics.specs.metric_spec import MetricSpec

logger = logging.getLogger(__name__)


def test_exclude_dimensions_order_does_not_matter() -> None:  # noqa: D103
    spec0 = MetricSpec.create(element_name="m", exclude_dimensions=("b", "a"))
    spec1 = MetricSpec.create(element_name="m", exclude_dimensions=("a", "b"))

    assert spec0 == spec1
    assert spec0.exclude_dimensions == ("a", "b")
    assert spec0.metric_modifier == spec1.metric_modifier
    assert spec0.metric_modifier.exclude_dimensions == ("a", "b")


def test_exclude_dimensions_duplicates_are_collapsed() -> None:  # noqa: D103
    spec = MetricSpec.create(element_name="m", exclude_dimensions=("a", "b", "a"))

    assert spec == MetricSpec.create(element_name="m", exclude_dimensions=("a", "b"))
    assert spec.exclude_dimensions == ("a", "b")
