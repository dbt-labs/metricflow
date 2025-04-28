from __future__ import annotations

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_object_snapshot_equal


@pytest.fixture(scope="module")
def query_parser(simple_semantic_manifest: PydanticSemanticManifest) -> MetricFlowQueryParser:  # noqa: D103
    return MetricFlowQueryParser(SemanticManifestLookup(simple_semantic_manifest))


def test_conversion_rate_with_constant_properties(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    query_parser: MetricFlowQueryParser,
) -> None:
    result = query_parser.parse_and_validate_query(
        metric_names=("visit_buy_conversion_rate_by_session",),
        group_by_names=("visit__referrer_id", "user__home_state_latest", "metric_time"),
    )

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj=result,
    )
