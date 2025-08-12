from __future__ import annotations

import logging
from collections.abc import Mapping

import pytest
from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def test_simple_dimensions_for_metrics_with_invalid_metric(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Check the exception raised when invalid metric names are provided to `simple_dimensions_for_metrics`."""
    mf_engine = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].metricflow_engine

    with pytest.raises(Exception) as exception_info_for_one_invalid_metric:
        mf_engine.simple_dimensions_for_metrics(["invalid_metric"])

    with pytest.raises(Exception) as exception_info_for_multiple_invalid_metrics:
        mf_engine.simple_dimensions_for_metrics(["invalid_metric_0", "invalid_metric_1"])

    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj={
            "one_invalid_metric": exception_info_for_one_invalid_metric.value,
            "multiple_invalid_metrics": exception_info_for_multiple_invalid_metrics.value,
        },
    )
