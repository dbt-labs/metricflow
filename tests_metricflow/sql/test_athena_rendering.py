from __future__ import annotations

from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlBetweenExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlGenerateUuidExpression,
    SqlIntegerExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringExpression,
    SqlStringLiteralExpression,
    SqlSubtractTimeIntervalExpression,
)

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlExpressionRenderer, AthenaSqlPlanRenderer
from metricflow_semantic_interfaces.type_enums.date_part import DatePart
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from scripts.generate_snapshots import ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet


def test_create_from_url_supports_athena() -> None:
    """Athena URLs should parse as a supported SQL dialect."""
    connection_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena://access_key_id@/awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/"
    )

    assert connection_parameters.dialect == "athena"
    assert connection_parameters.username == "access_key_id"
    assert connection_parameters.database == "awsdatacatalog"
    assert connection_parameters.get_query_field_values("region_name") == ("eu-central-1",)
    assert connection_parameters.get_query_field_values("s3_staging_dir") == ("s3://bucket/dbt/",)


def test_athena_expression_renderer_supports_expected_operations() -> None:
    """Athena-specific SQL syntax should render consistently."""
    renderer = AthenaSqlExpressionRenderer()

    assert renderer.sql_engine is SqlEngine.ATHENA
    assert renderer.supported_percentile_function_types == {SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS}
    assert renderer.render_date_part(DatePart.DOW) == "DAY_OF_WEEK"
    assert renderer.render_date_part(DatePart.YEAR) == "year"

    uuid_expr = SqlGenerateUuidExpression.create()
    assert renderer.visit_generate_uuid_expr(uuid_expr).sql == "uuid()"

    add_expr = SqlAddTimeExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "ts")),
        count_expr=SqlIntegerExpression.create(1),
        granularity=TimeGranularity.DAY,
    )
    assert renderer.visit_add_time_expr(add_expr).sql == "DATE_ADD('day', 1, alias.ts)"

    subtract_expr = SqlSubtractTimeIntervalExpression.create(
        arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "ts")),
        count=7,
        granularity=TimeGranularity.DAY,
    )
    assert renderer.visit_subtract_time_interval_expr(subtract_expr).sql == "DATE_ADD('day', -7, alias.ts)"

    percentile_expr = SqlPercentileExpression.create(
        order_by_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "value")),
        percentile_args=SqlPercentileExpressionArgument(
            percentile=0.5,
            function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
        ),
    )
    assert renderer.visit_percentile_expr(percentile_expr).sql == "approx_percentile(alias.value, 0.5)"

    between_expr = SqlBetweenExpression.create(
        column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time")),
        start_expr=SqlStringLiteralExpression.create("2023-01-01"),
        end_expr=SqlStringLiteralExpression.create("2023-12-31"),
    )
    assert renderer.visit_between_expr(between_expr).sql == (
        "alias.event_time BETWEEN timestamp '2023-01-01' AND timestamp '2023-12-31'"
    )


def test_athena_between_expr_does_not_wrap_arbitrary_sql_fragments() -> None:
    """Athena should not treat rendered SQL fragments as timestamp literals."""
    renderer = AthenaSqlExpressionRenderer()
    between_expr = SqlBetweenExpression.create(
        column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time")),
        start_expr=SqlStringExpression.create("DATE '2023-01-01'", requires_parenthesis=False),
        end_expr=SqlStringExpression.create("DATE '2023-01-10'", requires_parenthesis=False),
    )

    assert renderer.visit_between_expr(between_expr).sql == (
        "alias.event_time BETWEEN DATE '2023-01-01' AND DATE '2023-01-10'"
    )


def test_athena_between_expr_does_not_wrap_timezone_aware_literals() -> None:
    """Athena should not wrap timezone-aware ISO datetime literals with TIMESTAMP syntax."""
    renderer = AthenaSqlExpressionRenderer()
    between_expr = SqlBetweenExpression.create(
        column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time")),
        start_expr=SqlStringLiteralExpression.create("2023-01-01T00:00:00+00:00"),
        end_expr=SqlStringLiteralExpression.create("2023-01-10T00:00:00Z"),
    )

    assert renderer.visit_between_expr(between_expr).sql == (
        "alias.event_time BETWEEN '2023-01-01T00:00:00+00:00' AND '2023-01-10T00:00:00Z'"
    )


def test_athena_between_expr_wraps_timezone_naive_datetime_literals() -> None:
    """Athena should wrap timezone-naive ISO datetime literals with TIMESTAMP syntax."""
    renderer = AthenaSqlExpressionRenderer()
    between_expr = SqlBetweenExpression.create(
        column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time")),
        start_expr=SqlStringLiteralExpression.create("2023-01-01T12:34:56"),
        end_expr=SqlStringLiteralExpression.create("2023-01-10T23:45:01"),
    )

    assert renderer.visit_between_expr(between_expr).sql == (
        "alias.event_time BETWEEN timestamp '2023-01-01T12:34:56' AND timestamp '2023-01-10T23:45:01'"
    )


def test_athena_between_expr_does_not_wrap_non_iso_string_literals() -> None:
    """Athena should not wrap arbitrary non-ISO string literals with TIMESTAMP syntax."""
    renderer = AthenaSqlExpressionRenderer()
    between_expr = SqlBetweenExpression.create(
        column_arg=SqlColumnReferenceExpression.create(SqlColumnReference("alias", "event_time")),
        start_expr=SqlStringLiteralExpression.create("not-a-date"),
        end_expr=SqlStringLiteralExpression.create("also-not-a-date"),
    )

    assert renderer.visit_between_expr(between_expr).sql == (
        "alias.event_time BETWEEN 'not-a-date' AND 'also-not-a-date'"
    )


def test_athena_plan_renderer_uses_athena_expression_renderer() -> None:
    """The plan renderer should be backed by the Athena expression renderer."""
    plan_renderer = AthenaSqlPlanRenderer()

    assert isinstance(plan_renderer.expr_renderer, AthenaSqlExpressionRenderer)


def test_snapshot_engine_configurations_include_athena() -> None:
    """Snapshot generation should include Athena in the engine registry."""
    assert "athena" in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
    assert ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME["athena"] == "athena-env"
