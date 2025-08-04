from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.performance.performance_helpers import PerformanceTracker
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def manifest_with_50_models_25_metrics() -> SemanticManifest:
    """A semantic manifest with 200 models (100 with measures) and 100 metrics (50 of them derived)."""
    parameter_set = SyntheticManifestParameterSet(
        measure_semantic_model_count=25,
        measures_per_semantic_model=20,
        dimension_semantic_model_count=25,
        categorical_dimensions_per_semantic_model=20,
        max_metric_depth=1,
        max_metric_width=25,
        saved_query_count=100,
        metrics_per_saved_query=20,
        categorical_dimensions_per_saved_query=20,
    )

    generator = SyntheticManifestGenerator(parameter_set)
    semantic_manifest = generator.generate_manifest()
    return PydanticSemanticManifestTransformer.transform(semantic_manifest)


@pytest.mark.skip
def test_profile_explain(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_50_models_25_metrics: SemanticManifest,
    sql_client: SqlClient,
) -> None:
    """Tests formatting a performance report to a text table."""
    semantic_manifest = manifest_with_50_models_25_metrics
    manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=True)

    mf_engine = MetricFlowEngine(
        manifest_lookup,
        sql_client,
    )
    performance_tracker = PerformanceTracker()
    with performance_tracker.session("Profile Explain"):
        for saved_query in semantic_manifest.saved_queries[:1]:
            try:
                mf_engine.explain(
                    MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name)
                )
            except Exception:
                logger.exception("Ignoring exception for the test")

    text_table = performance_tracker.last_session_report.text_format()

    logger.info(LazyFormat("Profiled explain", text_table=text_table))


# def test_sg_run_explain_many_saved_queries(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
#     sql_client: SqlClient,
# ) -> None:
#     conf_source = simple_semantic_manifest
#     semantic_manifest = _create_manifest(conf_source)
#
#     with log_block_runtime("Engine Init"):
#         manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=True)
#         mf_engine = MetricFlowEngine(
#             semantic_manifest_lookup=manifest_lookup,
#             sql_client=sql_client,
#         )
#
#     # cProfile.runctx(
#     #     statement="mf_engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))",
#     #     filename=str(CPROFILE_OUTPUT_FILE_PATH),
#     #     locals=locals(),
#     #     globals=globals(),
#     # )
#
#     with log_block_runtime("Explain Queries"):
#         for saved_query in semantic_manifest.saved_queries[:40]:
#             try:
#                 mf_engine.explain(
#                     MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name)
#                 )
#             except Exception:
#                 logger.exception("Ignoring exception for the test")
#
#     # with log_block_runtime("Query Explain - Run 2"):
#     #     mf_engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))
