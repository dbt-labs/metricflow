from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.errors.error_classes import UnknownMetricError
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def test_metadata_methods_with_invalid_metics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Check the exception raised when invalid metric names are provided to metadata methods in `MetricFlowEngine."""
    mf_engine = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].metricflow_engine
    results: dict[str, str] = {}

    # Include 1 valid and 2 invalid metric names.
    metric_names = ["bookings"] + [f"invalid_metric_{i}" for i in range(2)]

    for method_name, method in [
        ("entities_for_metrics", mf_engine.entities_for_metrics),
        ("list_dimensions", mf_engine.list_dimensions),
        ("list_group_bys", mf_engine.list_group_bys),
        ("simple_dimensions_for_metrics", mf_engine.simple_dimensions_for_metrics),
    ]:
        with pytest.raises(UnknownMetricError) as exception_info:
            method(metric_names=metric_names)  # type: ignore[operator]
        results[method_name] = str(exception_info.value)

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj=results,
    )
