"""Test URL conversion from MetricFlow format to SqlAlchemy format."""

from __future__ import annotations

import pytest

from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet
from tests_metricflow.fixtures.sql_clients.sqlalchemy_url_builder import SqlAlchemyUrlBuilder


def test_duckdb_in_memory_url() -> None:
    """Test DuckDB in-memory URL conversion."""
    params = SqlEngineConnectionParameterSet.create_from_url("duckdb://")
    url = SqlAlchemyUrlBuilder.build_url(params, password="", schema=None)

    assert url.drivername == "duckdb"
    assert url.database is None  # In-memory


def test_duckdb_file_based_url() -> None:
    """Test DuckDB file-based URL conversion."""
    params = SqlEngineConnectionParameterSet.create_from_url("duckdb:///tmp/test.db")
    url = SqlAlchemyUrlBuilder.build_url(params, password="", schema=None)

    assert url.drivername == "duckdb"
    assert url.database == "tmp/test.db"


def test_postgresql_url() -> None:
    """Test PostgreSQL URL conversion."""
    params = SqlEngineConnectionParameterSet.create_from_url("postgresql://user@localhost:5432/testdb")
    url = SqlAlchemyUrlBuilder.build_url(params, password="secret", schema="myschema")

    assert url.drivername == "postgresql+psycopg2"
    assert url.username == "user"
    assert url.password == "secret"
    assert url.host == "localhost"
    assert url.port == 5432
    assert url.database == "testdb"
    assert "search_path=myschema" in url.query.get("options", "")


@pytest.mark.skip(reason="Databricks URL with semicolon is not yet supported")
def test_databricks_url_with_semicolon() -> None:
    """Test Databricks URL with semicolon-separated http_path."""
    params = SqlEngineConnectionParameterSet.create_from_url(
        "databricks://workspace.cloud.databricks.com:443/main;httppath=/sql/1.0/warehouses/abc"
    )
    url = SqlAlchemyUrlBuilder.build_url(params, password="dapi_token", schema="default")

    assert url.drivername == "databricks"
    assert url.username == "token"
    assert url.password == "dapi_token"
    assert url.host == "workspace.cloud.databricks.com"
    assert url.port == 443
    assert url.database == "main"
    assert (
        url.query["http_path"]
        == "databricks://workspace.cloud.databricks.com:443/main;httppath=/sql/1.0/warehouses/abc"
    )
    assert url.query["schema"] == "default"


def test_snowflake_url_with_warehouse() -> None:
    """Test Snowflake URL with warehouse parameter."""
    params = SqlEngineConnectionParameterSet.create_from_url(
        "snowflake://user@account.us-east-1:443/database?warehouse=compute_wh"
    )
    url = SqlAlchemyUrlBuilder.build_url(params, password="secret", schema="schema1")

    assert url.drivername == "snowflake"
    assert url.username == "user"
    assert url.password == "secret"
    assert url.host == "account.us-east-1"
    assert url.database == "database"
    assert url.query["warehouse"] == "compute_wh"
    assert url.query["schema"] == "schema1"


def test_trino_url_with_catalog() -> None:
    """Test Trino URL with catalog parameter."""
    params = SqlEngineConnectionParameterSet.create_from_url("trino://user@localhost:8080/memory?catalog=memory")
    url = SqlAlchemyUrlBuilder.build_url(params, password="", schema="default")

    assert url.drivername == "trino"
    assert url.username == "user"
    assert url.host == "localhost"
    assert url.port == 8080
    assert url.database == "memory"
    assert url.query["catalog"] == "memory"
    assert url.query["schema"] == "default"


def test_redshift_url() -> None:
    """Test Redshift URL conversion."""
    params = SqlEngineConnectionParameterSet.create_from_url("redshift://user@host.redshift.amazonaws.com:5439/dev")
    url = SqlAlchemyUrlBuilder.build_url(params, password="secret", schema="analytics")

    assert url.drivername == "redshift+psycopg2"
    assert url.username == "user"
    assert url.password == "secret"
    assert url.host == "host.redshift.amazonaws.com"
    assert url.port == 5439
    assert url.database == "dev"
    assert "search_path=analytics" in url.query.get("options", "")


def test_bigquery_url() -> None:
    """Test BigQuery URL with JSON credentials."""
    credentials_json = '{"type": "service_account", "project_id": "my-project", "client_email": "test@test.com"}'
    params = SqlEngineConnectionParameterSet.create_from_url("bigquery://my-project/my-dataset")
    url = SqlAlchemyUrlBuilder.build_url(params, password=credentials_json, schema="my_dataset")

    assert url.drivername == "bigquery"
    assert url.host == "my-project"
    assert url.query["dataset_id"] == "my_dataset"
    assert url.query["credentials_info"] == credentials_json
