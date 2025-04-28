from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal


def test_expectation_description(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
) -> None:
    """Tests having a description of the expectation in a snapshot."""
    assert_str_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str="1 + 1 = 2",
        expectation_description="The snapshot should show the 2 as the result.",
    )
