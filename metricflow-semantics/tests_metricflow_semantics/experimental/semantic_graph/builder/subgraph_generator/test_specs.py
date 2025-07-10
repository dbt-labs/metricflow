from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from tests_metricflow_semantics.experimental.semantic_graph.test_helpers import LinkableSpecResolverTester

logger = logging.getLogger(__name__)


def test_linkable_spec_resolvers(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_02_single_join_manifest: PydanticSemanticManifest,
) -> None:
    LinkableSpecResolverTester.compare_resolver_outputs_for_measures(
        semantic_manifest=sg_02_single_join_manifest,
    )
