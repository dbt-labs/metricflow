from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.runtime import log_block_runtime
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


def test_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    sg_00_minimal_manifest: PydanticSemanticManifest,
    sql_client: SqlClient,
) -> None:
    semantic_manifest = sg_00_minimal_manifest
    manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=True)
    with log_block_runtime("Init engine"):
        mf_engine = MetricFlowEngine(
            semantic_manifest_lookup=manifest_lookup,
            sql_client=sql_client,
        )

    with log_block_runtime("Query"):
        metric_names = ["metric_sm_0_measure"]
        group_by_names = [
            "metric_time__day",
        ]
        # where_constraints = ["{{ Metric('metric_1_001', group_by=['common_entity']) }}"]
        where_constraints: list[str] = []
        with log_block_runtime("Run explain #0"):
            result = mf_engine.explain(
                MetricFlowQueryRequest.create_with_random_request_id(
                    metric_names=metric_names, group_by_names=group_by_names, where_constraints=where_constraints
                )
            )
            logger.info(
                LazyFormat(
                    "Generated explain result",
                    result=result,
                )
            )
