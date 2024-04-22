from __future__ import annotations

from _pytest.fixtures import FixtureRequest

from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.integration.conftest import IntegrationTestHelpers
from tests.snapshot_utils import assert_object_snapshot_equal


def test_list_dimensions(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=sorted([dim.qualified_name for dim in it_helpers.mf_engine.list_dimensions()]),
    )
