from __future__ import annotations

import datetime
import logging
from copy import copy
from typing import List, Mapping, Optional, Sequence, Tuple

import jinja2
import pytest
from dateutil import parser
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasureAggregationParameters
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.protocols.query_parameter import DimensionOrEntityQueryParameter
from metricflow_semantics.specs.query_param_implementations import DimensionOrEntityParameter, TimeDimensionParameter
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringExpression,
    SqlSubtractTimeIntervalExpression,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.time.time_constants import ISO8601_PYTHON_FORMAT, ISO8601_PYTHON_TS_FORMAT
from metricflow_semantics.time.time_spine_source import TimeSpineSource
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlClient
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup
from tests_metricflow.integration.configured_test_case import (
    CONFIGURED_INTEGRATION_TESTS_REPOSITORY,
    IntegrationTestModel,
    RequiredDwEngineFeature,
)
from tests_metricflow.sql.compare_data_table import assert_data_tables_equal

logger = logging.getLogger(__name__)


class CheckQueryHelpers:
    """Functions that can be used to help render check queries in integration tests."""

    def __init__(self, sql_client: SqlClient) -> None:  # noqa: D107
        self._sql_client = sql_client

    def render_time_constraint(
        self,
        expr: str,
        start_time: Optional[str] = None,
        stop_time: Optional[str] = None,
    ) -> str:
        """Render an expression like "ds >='2020-01-01' AND ds < '2020-01-02'" for start_time = stop_time = '2020-01-01'."""
        time_param = start_time or stop_time  # needed for type checking
        assert time_param, "At least one of start_time or stop_time must be provided."
        time_format = ISO8601_PYTHON_FORMAT if len(time_param) == 10 else ISO8601_PYTHON_TS_FORMAT

        if start_time:
            start_expr = f"{self.cast_expr_to_ts(expr)} >= {self.cast_to_ts(f'{start_time}')}"

        if stop_time:
            stop_time_dt = datetime.datetime.strptime(stop_time, time_format)
            if time_format == ISO8601_PYTHON_FORMAT:
                stop_time = (stop_time_dt + datetime.timedelta(days=1)).strftime(time_format)
            else:
                stop_time = (stop_time_dt + datetime.timedelta(seconds=1)).strftime(time_format)
            stop_expr = f"{self.cast_expr_to_ts(expr)} < {self.cast_to_ts(f'{stop_time}')}"

        return f"{start_expr if start_time else ''}{' AND ' if start_time and stop_time else ''}{stop_expr if stop_time else ''}"

    def cast_expr_to_ts(self, expr: str) -> str:
        """Returns the expression as a new expression cast to the timestamp type, if applicable for the DB."""
        return f"CAST({expr} AS {self._sql_client.sql_plan_renderer.expr_renderer.timestamp_data_type})"

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
        expr = SqlSubtractTimeIntervalExpression.create(
            arg=SqlColumnReferenceExpression.create(SqlColumnReference(table_alias, column_alias)),
            count=count,
            granularity=granularity,
        )
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(expr).sql

    def render_date_add(
        self,
        date_column: str,
        count_column: str,
        granularity: TimeGranularity,
    ) -> str:
        """Renders a date add expression."""
        expr = SqlAddTimeExpression.create(
            arg=SqlStringExpression.create(sql_expr=date_column, requires_parenthesis=False),
            count_expr=SqlStringExpression.create(sql_expr=count_column, requires_parenthesis=False),
            granularity=granularity,
        )
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(expr).sql

    def render_date_trunc(self, expr: str, granularity: TimeGranularity) -> str:
        """Return the DATE_TRUNC() call that can be used for converting the given expr to the granularity."""
        renderable_expr = SqlDateTruncExpression.create(
            time_granularity=granularity,
            arg=SqlCastToTimestampExpression.create(
                arg=SqlStringExpression.create(
                    sql_expr=expr,
                    requires_parenthesis=False,
                )
            ),
        )
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

    def render_extract(self, expr: str, date_part: DatePart) -> str:
        """Return the EXTRACT call that can be used for converting the given expr to the date_part."""
        renderable_expr = SqlExtractExpression.create(
            date_part=date_part,
            arg=SqlCastToTimestampExpression.create(
                arg=SqlStringExpression.create(
                    sql_expr=expr,
                    requires_parenthesis=False,
                )
            ),
        )
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

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

        renderable_expr = SqlPercentileExpression.create(
            order_by_arg=SqlStringExpression.create(
                sql_expr=expr,
                requires_parenthesis=False,
            ),
            percentile_args=percentile_args,
        )
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(renderable_expr).sql

    @property
    def double_data_type_name(self) -> str:
        """Return the name of the double data type for the relevant SQL engine."""
        return self._sql_client.sql_plan_renderer.expr_renderer.double_data_type

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

    def render_metric_template(self, metric_name: str, group_by: Sequence[str] = ()) -> str:
        """Similar to render_dimension_template() but for metrics."""
        return f"{{{{ Metric('{metric_name}', group_by={group_by}) }}}}"

    def render_time_dimension_template(
        self, time_dimension_name: str, time_granularity: Optional[str] = None, entity_path: Sequence[str] = ()
    ) -> str:
        """Similar to render_dimension_template() but for time dimensions."""
        if time_granularity is not None:
            return f"{{{{ TimeDimension('{time_dimension_name}', '{time_granularity}', entity_path={repr(entity_path)}) }}}}"
        else:
            return f"{{{{ TimeDimension('{time_dimension_name}', entity_path={repr(entity_path)}) }}}}"

    def generate_random_uuid(self) -> str:
        """Returns the generate random UUID SQL function."""
        expr = SqlGenerateUuidExpression.create()
        return self._sql_client.sql_plan_renderer.expr_renderer.render_sql_expr(expr).sql


def filter_not_supported_features(
    sql_client: SqlClient, required_features: Tuple[RequiredDwEngineFeature, ...]
) -> Sequence[RequiredDwEngineFeature]:
    """Given a list of required features, return a list of features not supported by the given SQLClient."""
    not_supported_features: List[RequiredDwEngineFeature] = []
    for required_feature in required_features:
        if required_feature is RequiredDwEngineFeature.CONTINUOUS_PERCENTILE_AGGREGATION:
            if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.CONTINUOUS
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeature.DISCRETE_PERCENTILE_AGGREGATION:
            if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.DISCRETE
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeature.APPROXIMATE_CONTINUOUS_PERCENTILE_AGGREGATION:
            if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
                SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            ):
                not_supported_features.append(required_feature)
        elif required_feature is RequiredDwEngineFeature.APPROXIMATE_DISCRETE_PERCENTILE_AGGREGATION:
            if not sql_client.sql_plan_renderer.expr_renderer.can_render_percentile_function(
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
    mf_test_configuration: MetricFlowTestConfiguration,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
    sql_client: SqlClient,
    create_source_tables: bool,
) -> None:
    """Runs all integration tests configured in the test case YAML directory."""
    _test_case(
        name=name,
        mf_test_configuration=mf_test_configuration,
        engine_test_fixture_mapping=mf_engine_test_fixture_mapping,
        time_spine_sources=time_spine_sources,
        sql_client=sql_client,
    )


def _test_case(
    name: str,
    mf_test_configuration: MetricFlowTestConfiguration,
    engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
    time_spine_sources: Mapping[TimeGranularity, TimeSpineSource],
    sql_client: SqlClient,
) -> None:
    case = CONFIGURED_INTEGRATION_TESTS_REPOSITORY.get_test_case(name)
    logger.debug(LazyFormat(lambda: f"Running integration test case: '{case.name}' from file '{case.file_path}'"))
    time_spine_source = time_spine_sources[TimeGranularity.DAY]

    missing_required_features = filter_not_supported_features(sql_client, case.required_features)
    if missing_required_features:
        pytest.skip(f"DW does not support {missing_required_features}")

    if case.model is IntegrationTestModel.SIMPLE_MODEL:
        engine = engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].metricflow_engine
    elif case.model is IntegrationTestModel.SIMPLE_MODEL_NON_DS:
        engine = engine_test_fixture_mapping[SemanticManifestSetup.NON_SM_MANIFEST].metricflow_engine
    elif case.model is IntegrationTestModel.UNPARTITIONED_MULTI_HOP_JOIN_MODEL:
        engine = engine_test_fixture_mapping[SemanticManifestSetup.MULTI_HOP_JOIN_MANIFEST].metricflow_engine
    elif case.model is IntegrationTestModel.PARTITIONED_MULTI_HOP_JOIN_MODEL:
        engine = engine_test_fixture_mapping[
            SemanticManifestSetup.PARTITIONED_MULTI_HOP_JOIN_MANIFEST
        ].metricflow_engine
    elif case.model is IntegrationTestModel.EXTENDED_DATE_MODEL:
        engine = engine_test_fixture_mapping[SemanticManifestSetup.EXTENDED_DATE_MANIFEST].metricflow_engine
    elif case.model is IntegrationTestModel.SCD_MODEL:
        engine = engine_test_fixture_mapping[SemanticManifestSetup.SCD_MANIFEST].metricflow_engine
    else:
        assert_values_exhausted(case.model)

    check_query_helpers = CheckQueryHelpers(sql_client)

    group_by: List[DimensionOrEntityQueryParameter] = []
    for group_by_kwargs in case.group_by_objs:
        kwargs = copy(group_by_kwargs)
        date_part = kwargs.get("date_part")
        grain = kwargs.get("grain")
        if date_part or grain:
            if date_part:
                kwargs["date_part"] = DatePart(date_part)
            group_by.append(TimeDimensionParameter(**kwargs))
        else:
            group_by.append(DimensionOrEntityParameter(**kwargs))
    query_result = engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=case.metrics,
            group_by_names=case.group_bys if len(case.group_bys) > 0 else None,
            group_by=tuple(group_by) if len(group_by) > 0 else None,
            saved_query_name=case.saved_query_name,
            limit=case.limit,
            time_constraint_start=parser.parse(case.time_constraint[0]) if case.time_constraint else None,
            time_constraint_end=parser.parse(case.time_constraint[1]) if case.time_constraint else None,
            where_constraints=(
                [
                    jinja2.Template(
                        case.where_filter,
                        undefined=jinja2.StrictUndefined,
                    ).render(
                        source_schema=mf_test_configuration.mf_source_schema,
                        render_time_constraint=check_query_helpers.render_time_constraint,
                        TimeGranularity=TimeGranularity,
                        DatePart=DatePart,
                        render_date_sub=check_query_helpers.render_date_sub,
                        render_date_add=check_query_helpers.render_date_add,
                        render_date_trunc=check_query_helpers.render_date_trunc,
                        render_extract=check_query_helpers.render_extract,
                        render_percentile_expr=check_query_helpers.render_percentile_expr,
                        mf_time_spine_source=time_spine_source.sql_table.sql,
                        double_data_type_name=check_query_helpers.double_data_type_name,
                        render_dimension_template=check_query_helpers.render_dimension_template,
                        render_entity_template=check_query_helpers.render_entity_template,
                        render_metric_template=check_query_helpers.render_metric_template,
                        render_time_dimension_template=check_query_helpers.render_time_dimension_template,
                        generate_random_uuid=check_query_helpers.generate_random_uuid,
                        cast_to_ts=check_query_helpers.cast_to_ts,
                    )
                ]
                if case.where_filter
                else None
            ),
            order_by_names=case.order_bys,
            min_max_only=case.min_max_only,
            apply_group_by=case.apply_group_by,
            order_output_columns_by_input_order=True,
        )
    )

    actual = query_result.result_df

    expected = sql_client.query(
        jinja2.Template(
            case.check_query,
            undefined=jinja2.StrictUndefined,
        ).render(
            source_schema=mf_test_configuration.mf_source_schema,
            render_time_constraint=check_query_helpers.render_time_constraint,
            TimeGranularity=TimeGranularity,
            DatePart=DatePart,
            render_date_sub=check_query_helpers.render_date_sub,
            render_date_add=check_query_helpers.render_date_add,
            render_date_trunc=check_query_helpers.render_date_trunc,
            render_extract=check_query_helpers.render_extract,
            render_percentile_expr=check_query_helpers.render_percentile_expr,
            mf_time_spine_source=time_spine_source.sql_table.sql,
            double_data_type_name=check_query_helpers.double_data_type_name,
            generate_random_uuid=check_query_helpers.generate_random_uuid,
            cast_to_ts=check_query_helpers.cast_to_ts,
        )
    )
    # If we sort, it's effectively not checking the order whatever order that the output was would be overwritten.
    assert actual is not None, "Did not get a result table from MetricFlow"
    assert_data_tables_equal(actual, expected, sort_columns=not case.check_order, allow_empty=case.allow_empty)
