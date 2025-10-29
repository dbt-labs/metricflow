"""This module contains examples of test cases that can be used to profile MF engine performance."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Optional

import pytest
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.manifest_helpers import mf_load_manifest_from_json_file
from metricflow_semantics.test_helpers.performance.profiling import PerformanceTracker
from metricflow_semantics.test_helpers.synthetic_manifest.semantic_manifest_generator import SyntheticManifestGenerator
from metricflow_semantics.test_helpers.synthetic_manifest.synthetic_manifest_parameter_set import (
    SyntheticManifestParameterSet,
)
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.toolkit.performance_helpers import ExecutionTimer
from metricflow_semantics.toolkit.string_helpers import mf_indent

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def manifest_with_50_models_25_metrics() -> SemanticManifest:
    """A semantic manifest with 200 models (100 with measures) and 100 metrics (50 of them derived)."""
    parameter_set = SyntheticManifestParameterSet(
        simple_metric_semantic_model_count=25,
        simple_metrics_per_semantic_model=20,
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


def mf_explain_saved_query(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
    saved_query_names: Sequence[str],
    profile: bool = False,
) -> Optional[MetricFlowExplainResult]:
    """Helper to profile a set of saved queries in the given manifest."""
    with ExecutionTimer("Create `SemanticManifestLookup`"):
        manifest_lookup = SemanticManifestLookup(semantic_manifest)

    with ExecutionTimer("Create `MetricFlowEngine`"):
        mf_engine = MetricFlowEngine(
            manifest_lookup,
            sql_client,
        )

    performance_tracker = PerformanceTracker() if profile else None
    session_id = "Run Explain"
    run_context = (
        performance_tracker.session(session_id) if performance_tracker is not None else ExecutionTimer(session_id)
    )
    explain_result: Optional[MetricFlowExplainResult] = None
    with run_context:
        name_to_saved_query = {saved_query.name: saved_query for saved_query in semantic_manifest.saved_queries}

        for saved_query_name in saved_query_names:
            saved_query = name_to_saved_query[saved_query_name]
            logger.info(
                LazyFormat(
                    "Explaining saved query",
                    saved_query_name=saved_query.name,
                    metric_count=len(saved_query.query_params.metrics),
                    group_by_count=len(saved_query.query_params.group_by),
                )
            )
            try:
                explain_result = mf_engine.explain(
                    MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name)
                )
            except Exception:
                logger.exception("Ignoring exception for the test")

    if performance_tracker is not None:
        logger.info(
            LazyFormat(lambda: "Profiled explain.\n" + mf_indent(performance_tracker.last_session_report.text_format()))
        )

    return explain_result


def mf_simulate_validation(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
    profile: bool = False,
) -> None:
    """Simulate generation of queries for the manifest."""
    with ExecutionTimer("Create `SemanticManifestLookup`"):
        manifest_lookup = SemanticManifestLookup(semantic_manifest)

    with ExecutionTimer("Create `MetricFlowEngine`"):
        mf_engine = MetricFlowEngine(
            manifest_lookup,
            sql_client,
        )

    performance_tracker = PerformanceTracker() if profile else None
    session_id = "Run Explain"
    run_context = (
        performance_tracker.session(session_id) if performance_tracker is not None else ExecutionTimer(session_id)
    )
    with run_context:
        for metric in semantic_manifest.metrics:
            metric_name = metric.name
            logger.info(
                LazyFormat(
                    "Explaining metric query",
                    metric_name=metric_name,
                )
            )
            try:
                mf_engine.explain(
                    MetricFlowQueryRequest.create_with_random_request_id(
                        metric_names=[metric_name], group_by_names=[METRIC_TIME_ELEMENT_NAME]
                    )
                )
            except Exception:
                logger.exception("Ignoring exception for the test")

    if performance_tracker is not None:
        logger.info(
            LazyFormat(
                lambda: "Profiled validation.\n" + mf_indent(performance_tracker.last_session_report.text_format())
            )
        )


@pytest.mark.skip("Example only.")
def test_profile_explain(
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_50_models_25_metrics: SemanticManifest,
    sql_client: SqlClient,
) -> None:
    """Tests formatting a performance report to a text table."""
    saved_query_names = (manifest_with_50_models_25_metrics.saved_queries[0].name,)
    mf_explain_saved_query(manifest_with_50_models_25_metrics, sql_client, saved_query_names=saved_query_names)


@pytest.mark.skip("Example only.")
def test_profile_performance_using_json_manifest(
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    """Example of profiling MF engine performance using a JSON-serialized manifest."""
    # Example file path - replace with actual path.
    manifest_path = Path("git_ignored/semantic_manifest.json")

    with ExecutionTimer(LazyFormat("Load Manifest", manifest_path=manifest_path)):
        semantic_manifest = mf_load_manifest_from_json_file(manifest_path)

    logger.info(
        LazyFormat(
            "Starting session.",
            manifest_path=manifest_path,
            model_count=len(semantic_manifest.semantic_models),
            metric_count=len(semantic_manifest.metrics),
            saved_query_count=len(semantic_manifest.saved_queries),
        )
    )
    saved_query_names = tuple(saved_query.name for saved_query in semantic_manifest.saved_queries)[:100]

    mf_explain_saved_query(semantic_manifest, sql_client, saved_query_names=saved_query_names, profile=True)
    mf_simulate_validation(semantic_manifest, sql_client, profile=True)
