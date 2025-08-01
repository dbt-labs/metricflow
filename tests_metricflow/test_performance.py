# from __future__ import annotations
#
# import cProfile
# import json
# import logging
# import time
# from dataclasses import dataclass
#
# from dbt_semantic_interfaces.implementations.metric import PydanticMetric
# from dbt_semantic_interfaces.implementations.saved_query import PydanticSavedQuery
# from dbt_semantic_interfaces.implementations.semantic_manifest import (
#     PydanticSemanticManifest,
# )
# from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
# from dbt_semantic_interfaces.protocols import SemanticManifest
# from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
# from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import PydanticSemanticManifestTransformer
# from metricflow_semantics.api.v0_1.saved_query_dependency_resolver import SavedQueryDependencyResolver
# from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
# from metricflow_semantics.mf_logging.runtime import log_block_runtime
# from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
# from run_pstats import CPROFILE_OUTPUT_FILE_PATH
#
# from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
# from metricflow.protocols.sql_client import SqlClient
#
# logger = logging.getLogger(__name__)
#
#
# def _create_manifest(project_configuration_source: PydanticSemanticManifest) -> PydanticSemanticManifest:
#     with open("/Users/paul_work/tmp/us_foods_manifest.json") as fp:
#         manifest_json = json.load(fp)
#
#     semantic_models = [
#         PydanticSemanticModel(**semantic_model_json)
#         for semantic_model_json in manifest_json["semantic_models"].values()
#     ]
#     metrics = [PydanticMetric(**metric_json) for metric_json in manifest_json["metrics"].values()]
#     saved_queries = [
#         PydanticSavedQuery(**saved_query_json) for saved_query_json in manifest_json["saved_queries"].values()
#     ]
#     manifest = PydanticSemanticManifest(
#         semantic_models=semantic_models,
#         metrics=metrics,
#         saved_queries=saved_queries,
#         project_configuration=project_configuration_source.project_configuration,
#     )
#
#     return PydanticSemanticManifestTransformer.transform(manifest, ordered_rule_sequences=[[ConvertCountToSumRule()]])
#
#
# def _create_resolver(semantic_manifest: PydanticSemanticManifest) -> SavedQueryDependencyResolver:
#     return SavedQueryDependencyResolver(semantic_manifest)
#
#
# # def _run_resolution(semantic_manifest: PydanticSemanticManifest, resolver: SavedQueryDependencyResolver) -> None:
# #     # for i, saved_query in enumerate(semantic_manifest.saved_queries):
# #     #     if i >= 100:
# #     #         break
# #     #     with log_block_runtime(f"Resolve {saved_query.name=}"):
# #     #         resolver.resolve_dependencies(saved_query.name)
# #
# #     all_saved_query_names = tuple(saved_query.name for saved_query in semantic_manifest.saved_queries)
# #     saved_query_names_subset = all_saved_query_names[:]
# #     resolver.resolve_dependencies(saved_query_names_subset)
#
#
# def _run_resolution(semantic_manifest: PydanticSemanticManifest, resolver: SavedQueryDependencyResolver) -> None:
#     saved_query_names = tuple(saved_query.name for saved_query in semantic_manifest.saved_queries)
#     saved_query_names_subset = saved_query_names[:100]
#     for saved_query_name in saved_query_names_subset:
#         with log_block_runtime(f"Resolve {saved_query_name=}"):
#             resolver.resolve_dependencies(saved_query_name)
#
#
# # def test_saved_query_dependency_resolver(  # noqa: D103
# #     simple_semantic_manifest: PydanticSemanticManifest,
# # ) -> None:
# # with log_block_runtime("test_total"):
# #     with log_block_runtime("_create_manifest"):
# #         semantic_manifest = _create_manifest(simple_semantic_manifest)
# #     with log_block_runtime("_create_resolver"):
# #         resolver = _create_resolver(semantic_manifest)
# #     with log_block_runtime("_run_resolution"):
# #         _run_resolution(semantic_manifest, resolver)
#
# # cProfile.runctx(
# #     statement="_run_resolution(semantic_manifest, resolver)",
# #     filename=str(CPROFILE_OUTPUT_FILE_PATH),
# #     locals=locals(),
# #     globals=globals(),
# # )
#
#
# def _log_semantic_manifest_stats(semantic_manifest: PydanticSemanticManifest) -> None:
#     entities_set = set()
#
#     for semantic_model in semantic_manifest.semantic_models:
#         for entity in semantic_model.entities:
#             entities_set.add(entity.name)
#
#     logger.info(f"{len(semantic_manifest.semantic_models)=}")
#     logger.info(f"{len(entities_set)=}")
#
#     element_count = 0
#     for semantic_model in semantic_manifest.semantic_models:
#         element_count += len(semantic_model.entities) + len(semantic_model.dimensions) + len(semantic_model.measures)
#
#     logger.info(f"{element_count=}")
#
#     for saved_query in semantic_manifest.saved_queries:
#         logger.info(
#             f"{saved_query.name=} {len(saved_query.query_params.metrics)=} {len(saved_query.query_params.group_by)=}"
#         )
#
#
# def test_profile_100_queries(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
# ) -> None:
#     """Profile large manifest.
#
#     Run this test then run `./display_profiler_result.sh`.
#
#     """
#     with log_block_runtime("test_total"):
#         with log_block_runtime("_create_manifest"):
#             semantic_manifest = _create_manifest(simple_semantic_manifest)
#
#         # cProfile.runctx(
#         #     statement="_create_manifest(simple_semantic_manifest)",
#         #     filename=str(CPROFILE_OUTPUT_FILE_PATH),
#         #     locals=locals(),
#         #     globals=globals(),
#         # )
#
#         with log_block_runtime("_create_resolver"):
#             resolver = _create_resolver(semantic_manifest)
#
#         with log_block_runtime("_run_resolution"):
#             _run_resolution(semantic_manifest, resolver)
#
#         cProfile.runctx(
#             statement="_run_resolution(semantic_manifest, resolver)",
#             filename=str(CPROFILE_OUTPUT_FILE_PATH),
#             locals=locals(),
#             globals=globals(),
#         )
#
#
# def _run_resolve_one_saved_query(resolver: SavedQueryDependencyResolver) -> None:
#     # saved_query_name = "sq_system_level_rnps_score_month"
#     # saved_query_name = "sq_system_level_total_customers_all_time"
#     saved_query_name = "sq_act_trigger_dashboard_export_week_market_metrics_wo_sales"
#     resolver.resolve_dependencies(saved_query_name)
#
#
# def test_profile_resolve_one_saved_query(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     resolver = _create_resolver(semantic_manifest)
#     cProfile.runctx(
#         statement="_run_resolve_one_saved_query(resolver)",
#         filename=str(CPROFILE_OUTPUT_FILE_PATH),
#         locals=locals(),
#         globals=globals(),
#     )
#
#
# def _run_resolve_all_saved_queries(semantic_manifest: SemanticManifest, resolver: SavedQueryDependencyResolver) -> None:
#     # saved_query_name = "sq_system_level_rnps_score_month"
#     # saved_query_name = "sq_system_level_total_customers_all_time"
#     for saved_query in semantic_manifest.saved_queries:
#         resolver.resolve_dependencies(saved_query.name)
#
#
# def test_profile_all_saved_queries(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     resolver = _create_resolver(semantic_manifest)
#     cProfile.runctx(
#         statement="_run_resolve_all_saved_queries(semantic_manifest, resolver)",
#         filename=str(CPROFILE_OUTPUT_FILE_PATH),
#         locals=locals(),
#         globals=globals(),
#     )
#
#
# @dataclass(frozen=True)
# class SavedQueryResolutionResult:
#     """Time to resolve a saved query."""
#
#     saved_query_name: str
#     resolution_time: float
#
#
# def test_order_saved_query_resolution_runtimes(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     resolver = _create_resolver(semantic_manifest)
#     runtimes = []
#
#     logger.info(f"There are {len(semantic_manifest.saved_queries)} queries")
#     for saved_query in semantic_manifest.saved_queries:
#         start_time = time.time()
#         resolver.resolve_dependencies(saved_query.name)
#         runtimes.append(
#             SavedQueryResolutionResult(
#                 saved_query_name=saved_query.name,
#                 resolution_time=time.time() - start_time,
#             )
#         )
#
#     logger.info(LazyFormat("Sorted runtimes", sorted_runtimes=sorted(runtimes, key=lambda x: x.resolution_time)))
#
#
# def _create_engine(semantic_manifest: SemanticManifest, sql_client: SqlClient) -> MetricFlowEngine:
#     return MetricFlowEngine(
#         semantic_manifest_lookup=SemanticManifestLookup(semantic_manifest),
#         sql_client=sql_client,
#     )
#
#
# def _run_explain_one_saved_query(engine: MetricFlowEngine) -> None:
#     # saved_query_name = "sq_system_level_rnps_score_month"
#     # saved_query_name = "sq_system_level_total_customers_all_time"
#     saved_query_name = "sq_sc_gap_report_quarter_all_company"
#     engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))
#
#
# def test_run_explain_one_saved_query(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
#     sql_client: SqlClient,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     engine = _create_engine(semantic_manifest, sql_client)
#     saved_query_name = "sq_system_level_total_customers_all_time"
#     engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))
#
#
# def test_sg_run_explain_one_saved_query(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
#     sql_client: SqlClient,
# ) -> None:
#     conf_source = simple_semantic_manifest
#     semantic_manifest = _create_manifest(conf_source)
#     saved_query_name = "sq_act_trigger_dashboard_export_ytd_subregion_metrics_wo_sales"
#
#     with log_block_runtime("Engine Init"):
#         manifest_lookup = SemanticManifestLookup(semantic_manifest, use_semantic_graph=False)
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
#     with log_block_runtime("Query Explain - Run 1"):
#         mf_engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))
#
#     # with log_block_runtime("Query Explain - Run 2"):
#     #     mf_engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query_name))
#
#
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
#
#
# def test_profile_explain_one_saved_query(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
#     sql_client: SqlClient,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     engine = _create_engine(semantic_manifest, sql_client)
#     cProfile.runctx(
#         statement="_run_explain_one_saved_query(engine)",
#         filename=str(CPROFILE_OUTPUT_FILE_PATH),
#         locals=locals(),
#         globals=globals(),
#     )
#
#
# def _run_explain_many_saved_queries(semantic_manifest: SemanticManifest, engine: MetricFlowEngine) -> None:
#     # saved_query_name = "sq_system_level_rnps_score_month"
#     for saved_query in semantic_manifest.saved_queries[:40]:
#         engine.explain(MetricFlowQueryRequest.create_with_random_request_id(saved_query_name=saved_query.name))
#
#
# def test_profile_explain_many_saved_queries(  # noqa: D103
#     simple_semantic_manifest: PydanticSemanticManifest,
#     sql_client: SqlClient,
# ) -> None:
#     semantic_manifest = _create_manifest(simple_semantic_manifest)
#     engine = _create_engine(semantic_manifest, sql_client)
#     cProfile.runctx(
#         statement="_run_explain_many_saved_queries(semantic_manifest, engine)",
#         filename=str(CPROFILE_OUTPUT_FILE_PATH),
#         locals=locals(),
#         globals=globals(),
#     )
