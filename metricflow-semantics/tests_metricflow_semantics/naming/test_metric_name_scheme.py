from __future__ import annotations

from typing import Sequence

import pytest
from metricflow_semantics.naming.metric_scheme import MetricNamingScheme
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec


@pytest.fixture(scope="session")
def metric_naming_scheme() -> MetricNamingScheme:  # noqa: D103
    return MetricNamingScheme()


def test_input_str(metric_naming_scheme: MetricNamingScheme) -> None:  # noqa: D103
    assert metric_naming_scheme.input_str(MetricSpec(element_name="example_metric")) == "example_metric"


def test_input_follows_scheme(metric_naming_scheme: MetricNamingScheme) -> None:  # noqa: D103
    assert metric_naming_scheme.input_str_follows_scheme("some_metric_name")


def test_spec_pattern(metric_naming_scheme: MetricNamingScheme) -> None:  # noqa: D103
    spec_pattern = metric_naming_scheme.spec_pattern("metric_0")

    specs: Sequence[InstanceSpec] = (
        MetricSpec(element_name="metric_0"),
        MetricSpec(element_name="metric_1"),
        # Shouldn't happen in practice, but checks to see that only metric specs are matched.
        DimensionSpec(element_name="metric_0", entity_links=()),
    )

    assert (MetricSpec(element_name="metric_0"),) == tuple(spec_pattern.match(specs))
