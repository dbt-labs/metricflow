from __future__ import annotations

import datetime
import logging
from copy import copy
from typing import List, Optional, Sequence, Tuple

import jinja2
import pytest
from dateutil import parser
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasureAggregationParameters
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import (
    DunderColumnAssociationResolver,
)
from metricflow.protocols.query_parameter import DimensionOrEntityQueryParameter
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs.query_param_implementations import DimensionOrEntityParameter, TimeDimensionParameter
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringExpression,
    SqlSubtractTimeIntervalExpression,
)
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.integration.configured_test_case import (
    CONFIGURED_INTEGRATION_TESTS_REPOSITORY,
    IntegrationTestModel,
    RequiredDwEngineFeatures,
)
from metricflow.test.time.configurable_time_source import (
    ConfigurableTimeSource,
)
from metricflow.time.date_part import DatePart

logger = logging.getLogger(__name__)


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
        return f"CAST({expr} AS {self._sql_client.sql_query_plan_renderer.expr_renderer.timestamp_data_type})"

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
        """Renders a date subtract expression."""
        expr = SqlSubtractTimeIntervalExpression(
            arg=SqlColumnReferenceExpression(SqlColumnReference(table_alias, column_alias)),
            count=count,
            granularity=granularity,
        )
        return self._sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(expr).sql

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
        return self._sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

    def render_extract(self, expr: str, date_part: DatePart) -> str:
        """Return the EXTRACT call that can be used for converting the given expr to the date_part."""
        renderable_expr = SqlExtractExpression(
            date_part=date_part,
            arg=SqlCastToTimestampExpression(
                arg=SqlStringExpression(
                    sql_expr=expr,
                    requires_parenthesis=False,
                )
            ),
        )
        return self._sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

    def render_percentile_expr(
        self, expr: str, percentile: float, use_discrete_percentile: bool, use_approximate_percentile: bool
    ) -> str:
        """Return the percentile call that can be used for computing a percentile aggregation."""
        percentile_args = SqlPercentileExpressionArgument.from_aggregation_parameters(
            PydanticMeasureAggregationParameters(
                percentile=percentile,
                use_discrete_percentile=use_discrete_percentile,
                use_approximate_percentile=use_approximate_percentile,
            )
        )

        renderable_expr = SqlPercentileExpression(
            order_by_arg=SqlStringExpression(
                sql_expr=expr,
                requires_parenthesis=False,
            ),
            percentile_args=percentile_args,
        )
        return self._sql_client.sql_query_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

    @property
    def double_data_type_name(self) -> str:
        """Return the name of the double data type for the relevant SQL engine."""
        return self._sql_client.sql_query_plan_renderer.expr_renderer.double_data_type

    def render_dimension_template(self, dimension_name: str, entity_path: Sequence[str] = ()) -> str:
        """Renders a template that can be used to retrieve a dimension.

        For example:

         "{{ render_dimension_template('country_latest', entity_path=['listing']) }}"

         ->

         "{{ dimension('country_latest', entity_path=['listing'] }}"

         This is needed as the where_filter field in the definition files are rendered twice through Jinja - once
         by the test framework, and again by MF.
        """
        return f"{{{{ Dimension('{dimension_name}', entity_path={repr(entity_path)}) }}}}"

    def render_entity_template(self, entity_name: str, entity_path: Sequence[str] = ()) -> str:
        """Similar to render_dimension_template() but for entities."""
        return f"{{{{ Entity('{entity_name}', entity_path={repr(entity_path)}) }}}}"

    def render_time_dimension_template(
        self, time_dimension_name: str, time_granularity: str, entity_path: Sequence[str] = ()
    ) -> str:
        """Similar to render_dimension_template() but for time dimensions."""
        return (
            f"{{{{ TimeDimension('{time_dimension_name}', '{time_granularity}', entity_path={repr(entity_path)}) }}}}"
        )


def filter_not_supported_features(
    sql_client: SqlClient, required_features: Tuple[RequiredDwEngineFeatures, ...]
) -> Sequence[RequiredDwEngineFeatures]:
    """Given a list of required features, return a list of features not supported by the given SQLClient."""
    not_supported_features: List[RequiredDwEngineFeatures] = []
    for required_feature in required_features:
        if required_feature is RequiredDwEngineFeatures.CONTINUOUS_PERCENTILE_AGGREGATION:
            if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.CONTINUOUS
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeatures.DISCRETE_PERCENTILE_AGGREGATION:
            if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.DISCRETE
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeatures.APPROXIMATE_CONTINUOUS_PERCENTILE_AGGREGATION:
            if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeatures.APPROXIMATE_DISCRETE_PERCENTILE_AGGREGATION:
            if not sql_client.sql_query_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.APPROXIMATE_DISCRETE
            ):
                not_supported_features.append(required_feature)
        else:
            assert_values_exhausted(required_feature)
    return not_supported_features


@pytest.mark.parametrize(
    "name",
    CONFIGURED_INTEGRATION_TESTS_REPOSITORY.all_test_case_names,
    ids=lambda name: f"name={name}",
)
def test_case(
    name: str,
    mf_test_session_state: MetricFlowTestSessionState,
    simple_semantic_manifest_lookup: SemanticManifestLookup,
    simple_semantic_manifest_lookup_non_ds: SemanticManifestLookup,
    unpartitioned_multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    multi_hop_join_semantic_manifest_lookup: SemanticManifestLookup,
    extended_date_semantic_manifest_lookup: SemanticManifestLookup,
    scd_semantic_manifest_lookup: SemanticManifestLookup,
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    """Runs all integration tests configured in the test case YAML directory."""
    case = CONFIGURED_INTEGRATION_TESTS_REPOSITORY.get_test_case(name)
    logger.info(f"Running integration test case: '{case.name}' from file '{case.file_path}'")

    missing_required_features = filter_not_supported_features(sql_client, case.required_features)
    if missing_required_features:
        pytest.skip(f"DW does not support {missing_required_features}")

    semantic_manifest_lookup: Optional[SemanticManifestLookup] = None
    if case.model is IntegrationTestModel.SIMPLE_MODEL:
        semantic_manifest_lookup = simple_semantic_manifest_lookup
    elif case.model is IntegrationTestModel.SIMPLE_MODEL_NON_DS:
        semantic_manifest_lookup = simple_semantic_manifest_lookup_non_ds
    elif case.model is IntegrationTestModel.UNPARTITIONED_MULTI_HOP_JOIN_MODEL:
        semantic_manifest_lookup = unpartitioned_multi_hop_join_semantic_manifest_lookup
    elif case.model is IntegrationTestModel.PARTITIONED_MULTI_HOP_JOIN_MODEL:
        semantic_manifest_lookup = multi_hop_join_semantic_manifest_lookup
    elif case.model is IntegrationTestModel.EXTENDED_DATE_MODEL:
        semantic_manifest_lookup = extended_date_semantic_manifest_lookup
    elif case.model is IntegrationTestModel.SCD_MODEL:
        semantic_manifest_lookup = scd_semantic_manifest_lookup
    else:
        assert_values_exhausted(case.model)

    assert semantic_manifest_lookup

    engine = MetricFlowEngine(
        semantic_manifest_lookup=semantic_manifest_lookup,
        sql_client=sql_client,
        column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup),
        time_source=ConfigurableTimeSource(as_datetime("2021-01-04")),
    )

    check_query_helpers = CheckQueryHelpers(sql_client)

    group_by: List[DimensionOrEntityQueryParameter] = []
    for group_by_kwargs in case.group_by_objs:
        kwargs = copy(group_by_kwargs)
        date_part = kwargs.get("date_part")
        grain = kwargs.get("grain")
        if date_part or grain:
            if date_part:
                kwargs["date_part"] = DatePart(date_part)
            if grain:
                kwargs["grain"] = TimeGranularity(grain)
            group_by.append(TimeDimensionParameter(**kwargs))
        else:
            group_by.append(DimensionOrEntityParameter(**kwargs))
    query_result = engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=case.metrics,
            group_by_names=case.group_bys,
            group_by=tuple(group_by),
            limit=case.limit,
            time_constraint_start=parser.parse(case.time_constraint[0]) if case.time_constraint else None,
            time_constraint_end=parser.parse(case.time_constraint[1]) if case.time_constraint else None,
            where_constraint=jinja2.Template(
                case.where_filter,
                undefined=jinja2.StrictUndefined,
            ).render(
                source_schema=mf_test_session_state.mf_source_schema,
                render_time_constraint=check_query_helpers.render_time_constraint,
                TimeGranularity=TimeGranularity,
                DatePart=DatePart,
                render_date_sub=check_query_helpers.render_date_sub,
                render_date_trunc=check_query_helpers.render_date_trunc,
                render_extract=check_query_helpers.render_extract,
                render_percentile_expr=check_query_helpers.render_percentile_expr,
                mf_time_spine_source=semantic_manifest_lookup.time_spine_source.spine_table.sql,
                double_data_type_name=check_query_helpers.double_data_type_name,
                render_dimension_template=check_query_helpers.render_dimension_template,
                render_entity_template=check_query_helpers.render_entity_template,
                render_time_dimension_template=check_query_helpers.render_time_dimension_template,
            )
            if case.where_filter
            else None,
            order_by_names=case.order_bys,
        )
    )

    actual = query_result.result_df

    expected = sql_client.query(
        jinja2.Template(
            case.check_query,
            undefined=jinja2.StrictUndefined,
        ).render(
            source_schema=mf_test_session_state.mf_source_schema,
            render_time_constraint=check_query_helpers.render_time_constraint,
            TimeGranularity=TimeGranularity,
            DatePart=DatePart,
            render_date_sub=check_query_helpers.render_date_sub,
            render_date_trunc=check_query_helpers.render_date_trunc,
            render_extract=check_query_helpers.render_extract,
            render_percentile_expr=check_query_helpers.render_percentile_expr,
            mf_time_spine_source=semantic_manifest_lookup.time_spine_source.spine_table.sql,
            double_data_type_name=check_query_helpers.double_data_type_name,
        )
    )
    # If we sort, it's effectively not checking the order whatever order that the output was would be overwritten.
    assert_dataframes_equal(actual, expected, sort_columns=not case.check_order, allow_empty=case.allow_empty)
