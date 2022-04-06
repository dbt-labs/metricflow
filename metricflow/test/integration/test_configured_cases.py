import datetime
import logging
import os
from typing import List, Optional, Tuple, Sequence

import jinja2
import pytest
from dateutil import parser

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.model.semantic_model import SemanticModel
from metricflow.object_utils import assert_values_exhausted
from metricflow.plan_conversion.column_resolver import (
    DefaultColumnAssociationResolver,
)
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_exprs import (
    SqlTimeDeltaExpression,
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlDateTruncExpression,
    SqlCastToTimestampExpression,
    SqlStringExpression,
)
from metricflow.time.time_granularity import TimeGranularity
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.configured_test_case import (
    ConfiguredIntegrationTestCaseRepository,
    IntegrationTestModel,
    RequiredDwEngineFeatures,
)
from metricflow.test.test_utils import as_datetime
from metricflow.test.time.configurable_time_source import (
    ConfigurableTimeSource,
)

logger = logging.getLogger(__name__)

_integration_test_case_repository = ConfiguredIntegrationTestCaseRepository(
    os.path.join(os.path.dirname(__file__), "test_cases"),
)


class CheckQueryHelpers:
    """Functions that can be used to help render check queries in integration tests."""

    def __init__(self, sql_client: SqlClient) -> None:  # noqa:D
        self._sql_client = sql_client

    def render_time_constraint(
        self,
        expr: str,
        start_time: str,
        stop_time: str,
    ) -> str:
        """Render an expression like "ds >='2020-01-01' AND ds < '2020-01-02'" for start_time = stop_time = '2020-01-01'."""

        start_expr = self.cast_to_ts(f"{start_time}")
        time_format = "%Y-%m-%d"
        stop_time_plus_one_day = (
            datetime.datetime.strptime(stop_time, time_format) + datetime.timedelta(days=1)
        ).strftime(time_format)
        stop_expr = self.cast_to_ts(f"{stop_time_plus_one_day}")
        return f"{expr} >= {start_expr} AND {expr} < {stop_expr}"

    def cast_expr_to_ts(self, expr: str) -> str:
        """Returns the expression as a new expression cast to the timestamp type, if applicable for the DB."""
        if self._sql_client.sql_engine_attributes.timestamp_type_supported:
            return f"CAST({expr} AS {self._sql_client.sql_engine_attributes.timestamp_type_name})"

        return expr

    def cast_to_ts(self, string_literal: str) -> str:
        """Similar to cast_expr_to_ts, but assumes the input string is to be converted into a string literal."""
        return self.cast_expr_to_ts(f"'{string_literal}'")

    def render_date_sub(
        self,
        table_alias: str,
        column_alias: str,
        count: int,
        granularity: TimeGranularity,
    ) -> str:
        """Renders a date subtract expression"""
        expr = SqlTimeDeltaExpression(
            arg=SqlColumnReferenceExpression(SqlColumnReference(table_alias, column_alias)),
            count=count,
            granularity=granularity,
        )
        return self._sql_client.sql_engine_attributes.sql_query_plan_renderer.expr_renderer.render_sql_expr(expr).sql

    def render_date_trunc(self, expr: str, granularity: TimeGranularity) -> str:
        """Return the DATE_TRUNC() call that can be used for converting the given expr to the granularity."""
        renderable_expr = SqlDateTruncExpression(
            time_granularity=granularity,
            arg=SqlCastToTimestampExpression(
                arg=SqlStringExpression(
                    sql_expr=expr,
                    requires_parenthesis=False,
                )
            ),
        )
        return self._sql_client.sql_engine_attributes.sql_query_plan_renderer.expr_renderer.render_sql_expr(
            renderable_expr
        ).sql

    @property
    def double_data_type_name(self) -> str:
        """Return the name of the double data type for the relevant SQL engine"""
        return self._sql_client.sql_engine_attributes.double_data_type_name


def filter_not_supported_features(  # noqa: D
    sql_client: SqlClient, required_features: Tuple[RequiredDwEngineFeatures, ...]
) -> Sequence[RequiredDwEngineFeatures]:
    not_supported_features: List[RequiredDwEngineFeatures] = []
    for required_feature in required_features:
        if required_feature is RequiredDwEngineFeatures.DATE_TRUNC:
            if not sql_client.sql_engine_attributes.date_trunc_supported:
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeatures.FULL_OUTER_JOIN:
            if not sql_client.sql_engine_attributes.full_outer_joins_supported:
                not_supported_features.append(required_feature)
        else:
            assert_values_exhausted(required_feature)
    return not_supported_features


@pytest.mark.parametrize(
    "name",
    _integration_test_case_repository.get_all_test_case_names(),
    ids=lambda name: f"name={name}",
)
def test_case(
    name: str,
    mf_test_session_state: MetricFlowTestSessionState,
    simple_semantic_model: SemanticModel,
    simple_semantic_model_non_ds: SemanticModel,
    composite_identifier_semantic_model: SemanticModel,
    unpartitioned_multi_hop_join_semantic_model: SemanticModel,
    multi_hop_join_semantic_model: SemanticModel,
    extended_date_semantic_model: SemanticModel,
    sql_client: SqlClient,
    create_simple_model_tables: bool,
    create_message_source_tables: bool,
    create_bridge_table: bool,
    create_extended_date_model_tables: bool,
    time_spine_source: TimeSpineSource,
) -> None:
    """Runs all integration tests configured in the test case YAML directory."""
    case = _integration_test_case_repository.get_test_case(name)
    logger.info(f"Running integration test case: '{case.name}' from file '{case.file_path}'")

    missing_required_features = filter_not_supported_features(sql_client, case.required_features)
    if missing_required_features:
        logger.info(f"Skipping test '{name}' since the DW does not support {missing_required_features}")
        return

    semantic_model: Optional[SemanticModel] = None
    if case.model is IntegrationTestModel.SIMPLE_MODEL:
        semantic_model = simple_semantic_model
    elif case.model is IntegrationTestModel.SIMPLE_MODEL_NON_DS:
        semantic_model = simple_semantic_model_non_ds
    elif case.model is IntegrationTestModel.COMPOSITE_IDENTIFIER_MODEL:
        semantic_model = composite_identifier_semantic_model
    elif case.model is IntegrationTestModel.UNPARTITIONED_MULTI_HOP_JOIN_MODEL:
        semantic_model = unpartitioned_multi_hop_join_semantic_model
    elif case.model is IntegrationTestModel.PARTITIONED_MULTI_HOP_JOIN_MODEL:
        semantic_model = multi_hop_join_semantic_model
    elif case.model is IntegrationTestModel.EXTENDED_DATE_MODEL:
        semantic_model = extended_date_semantic_model
    else:
        assert_values_exhausted(case.model)

    assert semantic_model

    engine = MetricFlowEngine(
        semantic_model=semantic_model,
        sql_client=sql_client,
        column_association_resolver=DefaultColumnAssociationResolver(semantic_model),
        time_source=ConfigurableTimeSource(as_datetime("2021-01-04")),
        time_spine_source=time_spine_source,
        system_schema=mf_test_session_state.mf_system_schema,
    )

    query_result = engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=case.metrics,
            group_by_names=case.group_bys,
            limit=case.limit,
            time_constraint_start=parser.parse(case.time_constraint[0]) if case.time_constraint else None,
            time_constraint_end=parser.parse(case.time_constraint[1]) if case.time_constraint else None,
            where_constraint=case.where_constraint,
            order_by_names=case.order_bys,
        )
    )

    actual = query_result.result_df

    check_query_helpers = CheckQueryHelpers(sql_client)
    expected = sql_client.query(
        jinja2.Template(case.check_query, undefined=jinja2.StrictUndefined,).render(
            source_schema=mf_test_session_state.mf_source_schema,
            render_time_constraint=check_query_helpers.render_time_constraint,
            TimeGranularity=TimeGranularity,
            render_date_sub=check_query_helpers.render_date_sub,
            render_date_trunc=check_query_helpers.render_date_trunc,
            mf_time_spine_source=time_spine_source.spine_table.sql,
            double_data_type_name=check_query_helpers.double_data_type_name,
        )
    )
    # If we sort, it's effectively not checking the order whatever order that the output was would be overwritten.
    assert_dataframes_equal(actual, expected, sort_columns=not case.check_order, allow_empty=case.allow_empty)
