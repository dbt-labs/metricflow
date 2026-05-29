# from __future__ import annotations
#
# import logging
# from collections.abc import Mapping
#
# from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat
#
# from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
# from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
# from metricflow.metric_evaluation.me_plan_table_formatter import MetricEvaluationPlanTableFormatter
# from metricflow.metric_evaluation.passthrough.passthrough_me_planner import PassThroughMetricEvaluationPlanner
# from metricflow.plan_conversion.node_processor import PredicateInputType, PredicatePushdownState
# from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
# from tests_metricflow.metric_evaluation_plan.me_test_helpers import _create_filter_spec_factory
#
# logger = logging.getLogger(__name__)
#
#
# def test_passthrough_planner(  # noqa: D103
#     mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
# ) -> None:
#     engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
#     me_planner = PassThroughMetricEvaluationPlanner(
#         manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
#         column_association_resolver=engine_test_fixture.column_association_resolver,
#     )
#     query_parser = engine_test_fixture.query_parser
#     query_spec = query_parser.parse_and_validate_query(
#         metric_names=[
#             "bookings_per_listing",
#         ],
#         group_by_names=["metric_time"],
#     ).query_spec
#
#     predicate_pushdown_state = PredicatePushdownState(
#         time_range_constraint=query_spec.time_range_constraint,
#         where_filter_specs=(),
#         pushdown_enabled_types=frozenset({PredicateInputType.TIME_RANGE_CONSTRAINT}),
#     )
#     me_plan = me_planner.build_plan(
#         metric_specs=query_spec.metric_specs,
#         group_by_item_specs=query_spec.linkable_specs.as_tuple,
#         predicate_pushdown_state=predicate_pushdown_state,
#         filter_spec_factory=_create_filter_spec_factory(
#             query_spec=query_spec,
#             manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
#             column_association_resolver=engine_test_fixture.column_association_resolver,
#         ),
#     )
#     format_result = MetricEvaluationPlanTableFormatter().format_plan(me_plan)
#     logger.info(
#         LazyFormat(
#             "Generated metric plan",
#             overview_table=format_result.overview_table,
#             node_output_table=format_result.node_output_table,
#         )
#     )
#
#
# def test_passthrough_query_sql(  # noqa: D103
#     mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
# ) -> None:
#     engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
#
#     mf_engine = engine_test_fixture.metricflow_engine
#     explain_result = mf_engine.explain(
#         MetricFlowQueryRequest.create_with_random_request_id(
#             metric_names=[
#                 "booking_fees",
#                 "bookings",
#             ],
#             group_by_names=["metric_time"],
#         )
#     )
#     logger.info(
#         LazyFormat(
#             "Generated SQL",
#             dataflow_plan=explain_result.dataflow_plan.structure_text(),
#             sql=explain_result.sql_statement.sql,
#         )
#     )
#
#
# def test_build_dataflow_plan(  # noqa: D103
#     mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
# ) -> None:
#     engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
#
#     query_parser = engine_test_fixture.query_parser
#     parse_result = query_parser.parse_and_validate_query(
#         metric_names=["bookings_per_listing", "bookings"],
#         group_by_names=["metric_time"],
#     )
#
#     dataflow_plan_builder = DataflowPlanBuilder(
#         source_node_set=engine_test_fixture.source_node_set,
#         semantic_manifest_lookup=engine_test_fixture.semantic_manifest_lookup,
#         node_output_resolver=engine_test_fixture._node_output_resolver,
#         column_association_resolver=engine_test_fixture.column_association_resolver,
#         source_node_builder=engine_test_fixture.source_node_builder,
#     )
#
#     dataflow_plan = dataflow_plan_builder.build_plan(
#         query_spec=parse_result.query_spec,
#     )
#     logger.info(LazyFormat("Generated dataflow plan", dataflow_plan=dataflow_plan.structure_text()))
#
#
# # def test_passthrough_planner(
# #     mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture]
# # ) -> None:
# #     engine_test_fixture = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST]
# #     query_parser = engine_test_fixture.query_parser
# #
# #     me_planner = PassThroughMetricEvaluationPlanner(
# #         manifest_object_lookup=engine_test_fixture.manifest_object_lookup,
# #         column_association_resolver=engine_test_fixture.column_association_resolver,
# #     )
# #     parse_result = query_parser.parse_and_validate_query(
# #         metric_names=["bookings_per_listing", "bookings"],
# #         group_by_names=["metric_time"],
# #     )
# #     metric_plan = me_planner.build_plan(parse_result.query_spec)
# #     logger.info(LazyFormat("Generated metric plan", metric_plan=metric_plan.format(PrettyFormatGraphFormatter())))
