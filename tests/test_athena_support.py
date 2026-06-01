from __future__ import annotations

import os
from unittest.mock import Mock

import pytest
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
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

from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient, SupportedAdapterTypes
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.athena import AthenaSqlExpressionRenderer, AthenaSqlPlanRenderer
from scripts.generate_snapshots import ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
from tests_metricflow.fixtures import sql_client_fixtures
from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet
from tests_metricflow.fixtures.sql_client_fixtures import cleanup_athena_env, make_test_sql_client


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


def test_supported_adapter_types_include_athena() -> None:
    """Athena should be registered as a supported adapter."""
    adapter_values = [adapter_type.value for adapter_type in SupportedAdapterTypes]

    assert "athena" in adapter_values
    assert SupportedAdapterTypes.ATHENA.sql_engine_type is SqlEngine.ATHENA
    assert isinstance(SupportedAdapterTypes.ATHENA.sql_plan_renderer, AthenaSqlPlanRenderer)


def test_adapter_backed_sql_client_supports_athena_adapter() -> None:
    """The dbt adapter wrapper should map athena to the Athena renderer."""
    mock_adapter = Mock()
    mock_adapter.type.return_value = "athena"

    sql_client = AdapterBackedSqlClient(mock_adapter)

    assert sql_client.sql_engine_type is SqlEngine.ATHENA
    assert isinstance(sql_client.sql_plan_renderer, AthenaSqlPlanRenderer)


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


def test_athena_plan_renderer_uses_athena_expression_renderer() -> None:
    """The plan renderer should be backed by the Athena expression renderer."""
    plan_renderer = AthenaSqlPlanRenderer()

    assert isinstance(plan_renderer.expr_renderer, AthenaSqlExpressionRenderer)


def test_make_test_sql_client_supports_athena(
    monkeypatch: pytest.MonkeyPatch, cleanup_athena_env: None
) -> None:
    """Athena clients should be routable through the dbt test harness."""
    monkeypatch.setattr(sql_client_fixtures, "_initialize_dbt", lambda project_dir, profiles_dir: None)

    mock_adapter = Mock()
    mock_adapter.type.return_value = "athena"
    monkeypatch.setattr(sql_client_fixtures, "get_adapter_by_type", lambda adapter_type: mock_adapter)

    for env_var in (
        "AWS_ACCESS_KEY_ID",
        "AWS_DEFAULT_REGION",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "DBT_ENV_SECRET_AWS_PROFILE_NAME",
        "DBT_ENV_SECRET_DATABASE",
        "DBT_ENV_SECRET_REGION_NAME",
        "DBT_ENV_SECRET_S3_STAGING_DIR",
        "DBT_ENV_SECRET_SCHEMA",
    ):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv("DBT_ENV_SECRET_AWS_PROFILE_NAME", "stale-profile")

    sql_client = make_test_sql_client(
        url="athena://access_key_id@/awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/",
        password="secret",
        schema="analytics",
    )

    assert sql_client.sql_engine_type is SqlEngine.ATHENA
    assert "DBT_ENV_SECRET_AWS_PROFILE_NAME" not in os.environ
    assert os.environ["AWS_ACCESS_KEY_ID"] == "access_key_id"
    assert os.environ["AWS_DEFAULT_REGION"] == "eu-central-1"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "secret"
    assert os.environ["DBT_ENV_SECRET_DATABASE"] == "awsdatacatalog"
    assert os.environ["DBT_ENV_SECRET_REGION_NAME"] == "eu-central-1"
    assert os.environ["DBT_ENV_SECRET_S3_STAGING_DIR"] == "s3://bucket/dbt/"
    assert os.environ["DBT_ENV_SECRET_SCHEMA"] == "analytics"


def test_make_test_sql_client_supports_athena_profile_auth(
    monkeypatch: pytest.MonkeyPatch, cleanup_athena_env: None
) -> None:
    """Athena clients should support AWS profile-based auth without explicit key pairs."""
    monkeypatch.setattr(sql_client_fixtures, "_initialize_dbt", lambda project_dir, profiles_dir: None)

    mock_adapter = Mock()
    mock_adapter.type.return_value = "athena"
    monkeypatch.setattr(sql_client_fixtures, "get_adapter_by_type", lambda adapter_type: mock_adapter)

    for env_var in (
        "AWS_ACCESS_KEY_ID",
        "AWS_DEFAULT_REGION",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
        "DBT_ENV_SECRET_AWS_PROFILE_NAME",
        "DBT_ENV_SECRET_DATABASE",
        "DBT_ENV_SECRET_REGION_NAME",
        "DBT_ENV_SECRET_S3_STAGING_DIR",
        "DBT_ENV_SECRET_SCHEMA",
    ):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "stale-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "stale-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "stale-session")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "stale-security")
    monkeypatch.setenv("DBT_ENV_SECRET_AWS_PROFILE_NAME", "stale-profile")

    sql_client = make_test_sql_client(
        url="athena:///awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/&aws_profile_name=my-profile",
        password="",
        schema="analytics",
    )

    assert sql_client.sql_engine_type is SqlEngine.ATHENA
    assert "AWS_ACCESS_KEY_ID" not in os.environ
    assert "AWS_SECRET_ACCESS_KEY" not in os.environ
    assert "AWS_SESSION_TOKEN" not in os.environ
    assert "AWS_SECURITY_TOKEN" not in os.environ
    assert os.environ["DBT_ENV_SECRET_AWS_PROFILE_NAME"] == "my-profile"
    assert os.environ["DBT_ENV_SECRET_REGION_NAME"] == "eu-central-1"
    assert os.environ["DBT_ENV_SECRET_S3_STAGING_DIR"] == "s3://bucket/dbt/"


def test_make_test_sql_client_preserves_ambient_aws_env_credentials(
    monkeypatch: pytest.MonkeyPatch, cleanup_athena_env: None
) -> None:
    """Athena clients should preserve ambient AWS env credentials when the URL does not replace them."""
    monkeypatch.setattr(sql_client_fixtures, "_initialize_dbt", lambda project_dir, profiles_dir: None)

    mock_adapter = Mock()
    mock_adapter.type.return_value = "athena"
    monkeypatch.setattr(sql_client_fixtures, "get_adapter_by_type", lambda adapter_type: mock_adapter)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ambient-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "ambient-session")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "ambient-security")

    sql_client = make_test_sql_client(
        url="athena:///awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/",
        password="",
        schema="analytics",
    )

    assert sql_client.sql_engine_type is SqlEngine.ATHENA
    assert os.environ["AWS_ACCESS_KEY_ID"] == "ambient-access"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "ambient-secret"
    assert os.environ["AWS_SESSION_TOKEN"] == "ambient-session"
    assert os.environ["AWS_SECURITY_TOKEN"] == "ambient-security"
    assert "DBT_ENV_SECRET_AWS_PROFILE_NAME" not in os.environ


def test_snapshot_engine_configurations_include_athena() -> None:
    """Snapshot generation should include Athena in the engine registry."""
    assert "athena" in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
    assert ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME["athena"] == "athena-env"
