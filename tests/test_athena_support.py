from __future__ import annotations

import os
from types import SimpleNamespace
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


def test_initialize_dbt_raises_clear_error_on_failed_debug(monkeypatch: pytest.MonkeyPatch) -> None:
    """dbt initialization should fail fast when dbt debug does not succeed."""
    sql_client_fixtures._initialize_dbt.cache_clear()

    class FailingRunner:
        """Stub runner returning a failed debug result."""

        def invoke(self, args: list[str], project_dir: str, profiles_dir: str) -> SimpleNamespace:
            del args, project_dir, profiles_dir
            return SimpleNamespace(success=False, result="debug failed", exception=None)

    monkeypatch.setattr(sql_client_fixtures, "dbtRunner", lambda: FailingRunner())

    with pytest.raises(RuntimeError, match="Failed to initialize dbt for Athena test setup"):
        sql_client_fixtures._initialize_dbt(project_dir="/tmp/project", profiles_dir="/tmp/profiles")

    sql_client_fixtures._initialize_dbt.cache_clear()


def test_configure_athena_env_rejects_multiple_profile_names(cleanup_athena_env: None) -> None:
    """Athena env setup should reject URLs with multiple aws_profile_name values."""
    connection_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena:///awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/"
        "&aws_profile_name=first&aws_profile_name=second"
    )

    with pytest.raises(ValueError, match="multiple Athena aws_profile_name values"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            connection_parameters, password="", schema="analytics"
        )


def test_configure_athena_env_requires_exactly_one_region_name(cleanup_athena_env: None) -> None:
    """Athena env setup should require exactly one region_name."""
    missing_region_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena:///awsdatacatalog?s3_staging_dir=s3://bucket/dbt/"
    )
    multiple_region_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena:///awsdatacatalog?region_name=eu-central-1&region_name=us-east-1&s3_staging_dir=s3://bucket/dbt/"
    )

    with pytest.raises(ValueError, match="exactly 1 Athena region_name"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            missing_region_parameters, password="", schema="analytics"
        )

    with pytest.raises(ValueError, match="exactly 1 Athena region_name"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            multiple_region_parameters, password="", schema="analytics"
        )


def test_configure_athena_env_requires_exactly_one_s3_staging_dir(cleanup_athena_env: None) -> None:
    """Athena env setup should require exactly one s3_staging_dir."""
    missing_staging_dir_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena:///awsdatacatalog?region_name=eu-central-1"
    )
    multiple_staging_dir_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena:///awsdatacatalog?region_name=eu-central-1&s3_staging_dir=s3://bucket/one/"
        "&s3_staging_dir=s3://bucket/two/"
    )

    with pytest.raises(ValueError, match="exactly 1 Athena s3_staging_dir"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            missing_staging_dir_parameters, password="", schema="analytics"
        )

    with pytest.raises(ValueError, match="exactly 1 Athena s3_staging_dir"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            multiple_staging_dir_parameters, password="", schema="analytics"
        )


def test_configure_athena_env_requires_database_path(cleanup_athena_env: None) -> None:
    """Athena env setup should require a database/catalog in the URL path."""
    connection_parameters = SqlEngineConnectionParameterSet.create_from_url(
        "athena://?region_name=eu-central-1&s3_staging_dir=s3://bucket/dbt/"
    )

    with pytest.raises(ValueError, match="did not specify an Athena database/catalog"):
        sql_client_fixtures._configure_athena_env_from_connection_parameters(
            connection_parameters, password="", schema="analytics"
        )


def test_snapshot_engine_configurations_include_athena() -> None:
    """Snapshot generation should include Athena in the engine registry."""
    assert "athena" in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
    assert ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME["athena"] == "athena-env"
