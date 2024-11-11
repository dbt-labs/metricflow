from __future__ import annotations

from typing import Sequence

import pytest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.naming.metric_scheme import MetricNamingScheme
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.metric_spec import MetricSpec


@pytest.fixture(scope="session")
def metric_naming_scheme() -> MetricNamingScheme:  # noqa: D103
    return MetricNamingScheme()


def test_input_str(metric_naming_scheme: MetricNamingScheme) -> None:  # noqa: D103
    assert metric_naming_scheme.input_str(MetricSpec(element_name="bookings")) == "bookings"


def test_input_follows_scheme(  # noqa: D103
    metric_naming_scheme: MetricNamingScheme, simple_semantic_manifest_lookup: SemanticManifestLookup
) -> None:
    assert metric_naming_scheme.input_str_follows_scheme(
        "listings", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )


def test_spec_pattern(  # noqa: D103
    metric_naming_scheme: MetricNamingScheme, simple_semantic_manifest_lookup: SemanticManifestLookup
) -> None:
    spec_pattern = metric_naming_scheme.spec_pattern(
        "bookings", semantic_manifest_lookup=simple_semantic_manifest_lookup
    )

    specs: Sequence[InstanceSpec] = (
        MetricSpec(element_name="bookings"),
        MetricSpec(element_name="listings"),
        # Shouldn't happen in practice, but checks to see that only metric specs are matched.
        DimensionSpec(element_name="bookings", entity_links=()),
    )

    assert (MetricSpec(element_name="bookings"),) == tuple(spec_pattern.match(specs))
