from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.implementations.time_spine import PydanticTimeSpine, PydanticTimeSpinePrimaryColumn
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
from dbt_semantic_interfaces.type_enums import TimeGranularity
from metricflow_semantics.helpers.performance_helpers import ExecutionTimer
from metricflow_semantics.helpers.string_helpers import mf_indent
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


def mf_profile_explain_saved_queries(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
    saved_query_count: int,
    use_semantic_graph: bool = False,
    profile: bool = False,
) -> None:
    """Profile a set of saved queries in the given manifest."""
    with ExecutionTimer("Create `SemanticManifestLookup`"):
        manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=use_semantic_graph)

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
        for saved_query in semantic_manifest.saved_queries[:saved_query_count]:
            logger.info(
                LazyFormat(
                    "Explaining saved query",
                    saved_query_name=saved_query.name,
                    metric_count=len(saved_query.query_params.metrics),
                    group_by_count=len(saved_query.query_params.group_by),
                )
            )
            try:
                mf_engine.explain(
                    MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name)
                )
            except Exception:
                logger.exception("Ignoring exception for the test")

    if performance_tracker is not None:
        logger.info(
            LazyFormat(lambda: "Profiled explain.\n" + mf_indent(performance_tracker.last_session_report.text_format()))
        )


def mf_simulate_validation(
    semantic_manifest: SemanticManifest,
    sql_client: SqlClient,
    use_semantic_graph: bool = False,
    profile: bool = False,
) -> None:
    """Simulate generation of queries for the manifest."""
    with ExecutionTimer("Create `SemanticManifestLookup`"):
        manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=use_semantic_graph)

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


@pytest.mark.skip
def test_profile_explain(
    mf_test_configuration: MetricFlowTestConfiguration,
    manifest_with_50_models_25_metrics: SemanticManifest,
    sql_client: SqlClient,
) -> None:
    """Tests formatting a performance report to a text table."""
    mf_profile_explain_saved_queries(
        manifest_with_50_models_25_metrics, sql_client, saved_query_count=1, use_semantic_graph=True
    )


def test_profile_slow_validation(  # noqa: D103
    mf_test_configuration: MetricFlowTestConfiguration,
    sql_client: SqlClient,
) -> None:
    manifest_path = Path("git_ignored/semantic_manifest_4286131.json")

    with ExecutionTimer(LazyFormat("Load Manifest", manifest_path=manifest_path)):
        semantic_manifest = mf_load_manifest_from_json_file(manifest_path)

    logger.info(
        LazyFormat(
            "Logging manifest stats.",
            saved_query_count=len(semantic_manifest.saved_queries),
            metric_count=len(semantic_manifest.metrics),
            model_count=len(semantic_manifest.semantic_models),
        )
    )
    # mf_profile_explain_saved_queries(semantic_manifest, sql_client, saved_query_count=100, use_semantic_graph=False)
    mf_simulate_validation(semantic_manifest, sql_client, use_semantic_graph=False, profile=False)


def mf_load_manifest_from_json_file(json_file_path: Path) -> SemanticManifest:
    """Load a manifest from a file containing the JSON-serialized form of a `PydanticSemanticManifest`.

    This uses a dummy project configuration as it's missing in the file.
    """
    with open(json_file_path) as fp:
        manifest_json = json.load(fp)

    semantic_models = [
        PydanticSemanticModel.parse_obj(semantic_model_json) for semantic_model_json in manifest_json["semantic_models"]
    ]
    metrics = [PydanticMetric.parse_obj(metric_json) for metric_json in manifest_json["metrics"]]
    saved_queries = [
        PydanticSavedQuery.parse_obj(saved_query_json) for saved_query_json in manifest_json["saved_queries"]
    ]
    node_relation = PydanticNodeRelation(
        schema_name="dummy_schema",
        relation_name="dummy_relation",
        database="dummy_database",
        alias="dummy_alias",
    )
    project_configuration = PydanticProjectConfiguration(
        time_spines=[
            PydanticTimeSpine(
                node_relation=node_relation,
                primary_column=PydanticTimeSpinePrimaryColumn(
                    name="date_day",
                    time_granularity=TimeGranularity.DAY,
                ),
            )
        ],
    )
    manifest = PydanticSemanticManifest(
        semantic_models=semantic_models,
        metrics=metrics,
        saved_queries=saved_queries,
        project_configuration=project_configuration,
    )

    # noinspection PyTypeChecker
    return PydanticSemanticManifestTransformer.transform(manifest, ordered_rule_sequences=[[ConvertCountToSumRule()]])
