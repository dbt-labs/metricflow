from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import URL as SqlAlchemyURL

from tests_metricflow.fixtures.connection_url import SqlEngineConnectionParameterSet
from tests_metricflow.fixtures.sql_clients.common_client import SqlDialect


class SqlAlchemyUrlBuilder:
    """Converts MetricFlow URL format to SqlAlchemy URL objects."""

    @staticmethod
    def build_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build a SqlAlchemy URL from MetricFlow connection parameters.

        Args:
            connection_params: Parsed MetricFlow connection parameters
            password: Database password (from separate env var)
            schema: Default schema to use

        Returns:
            SqlAlchemy URL object
        """
        dialect = SqlDialect(connection_params.dialect)

        if dialect is SqlDialect.DUCKDB:
            return SqlAlchemyUrlBuilder._build_duckdb_url(connection_params)
        elif dialect is SqlDialect.DATABRICKS:
            return SqlAlchemyUrlBuilder._build_databricks_url(connection_params, password, schema)
        elif dialect is SqlDialect.POSTGRESQL:
            return SqlAlchemyUrlBuilder._build_postgresql_url(connection_params, password, schema)
        elif dialect is SqlDialect.SNOWFLAKE:
            return SqlAlchemyUrlBuilder._build_snowflake_url(connection_params, password, schema)
        elif dialect is SqlDialect.REDSHIFT:
            return SqlAlchemyUrlBuilder._build_redshift_url(connection_params, password, schema)
        elif dialect is SqlDialect.BIGQUERY:
            return SqlAlchemyUrlBuilder._build_bigquery_url(connection_params, password, schema)
        elif dialect is SqlDialect.TRINO:
            return SqlAlchemyUrlBuilder._build_trino_url(connection_params, password, schema)
        else:
            raise ValueError(f"Unsupported dialect: {dialect}")

    @staticmethod
    def _build_duckdb_url(
        connection_params: SqlEngineConnectionParameterSet,
    ) -> SqlAlchemyURL:
        """Build DuckDB URL.

        DuckDB URLs can be:
        - duckdb:// (in-memory)
        - duckdb:///path/to/file.db (file-based)

        Using duckdb_engine package: https://github.com/Mause/duckdb_engine
        """
        database = connection_params.database or ""

        # duckdb_engine uses 'duckdb' as the dialect name
        return SqlAlchemyURL.create(
            drivername="duckdb",
            database=database if database else None,
        )

    @staticmethod
    def _build_databricks_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build Databricks URL.

        Databricks SqlAlchemy: https://docs.databricks.com/aws/en/dev-tools/sqlalchemy

        Format: databricks://token:{token}@{hostname}:{port}/{database}

        The http_path is passed as a query parameter.
        """
        # Databricks uses token authentication
        # The password field contains the token

        query_params = {}
        if connection_params.http_path:
            query_params["http_path"] = connection_params.http_path

        # Add catalog if specified in query fields
        catalog_values = connection_params.get_query_field_values("catalog")
        if catalog_values:
            query_params["catalog"] = catalog_values[0]

        if schema:
            query_params["schema"] = schema

        return SqlAlchemyURL.create(
            drivername="databricks",
            username="token",
            password=password,  # This is the access token
            host=connection_params.hostname,
            port=connection_params.port,
            database=connection_params.database,
            query=query_params,
        )

    @staticmethod
    def _build_postgresql_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build PostgreSQL URL."""
        query_params = {}
        if schema:
            query_params["options"] = f"-c search_path={schema}"

        # Preserve any additional query parameters from original URL
        for field in connection_params.query_fields:
            if field.field_name not in query_params:
                query_params[field.field_name] = field.values[0]

        return SqlAlchemyURL.create(
            drivername="postgresql+psycopg2",
            username=connection_params.username,
            password=password,
            host=connection_params.hostname,
            port=connection_params.port,
            database=connection_params.database,
            query=query_params,
        )

    @staticmethod
    def _build_snowflake_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build Snowflake URL."""
        query_params = {}

        # Snowflake requires warehouse parameter
        warehouse_values = connection_params.get_query_field_values("warehouse")
        if warehouse_values:
            query_params["warehouse"] = warehouse_values[0]

        if schema:
            query_params["schema"] = schema

        # Preserve other query parameters
        for field in connection_params.query_fields:
            if field.field_name not in query_params:
                query_params[field.field_name] = field.values[0]

        return SqlAlchemyURL.create(
            drivername="snowflake",
            username=connection_params.username,
            password=password,
            host=connection_params.hostname,
            port=connection_params.port,
            database=connection_params.database,
            query=query_params,
        )

    @staticmethod
    def _build_redshift_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build Redshift URL."""
        query_params = {}
        if schema:
            query_params["options"] = f"-c search_path={schema}"

        # Redshift can use redshift+psycopg2 or redshift_connector
        return SqlAlchemyURL.create(
            drivername="redshift+psycopg2",
            username=connection_params.username,
            password=password,
            host=connection_params.hostname,
            port=connection_params.port or 5439,
            database=connection_params.database,
            query=query_params,
        )

    @staticmethod
    def _build_bigquery_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,  # JSON credentials string
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build BigQuery URL.

        BigQuery uses service account credentials passed as JSON string.
        The password parameter contains the full credentials JSON.
        """
        # Parse credentials to get project_id
        credentials = json.loads(password)
        project_id = credentials.get("project_id")

        # BigQuery URL format: bigquery://project_id/dataset_id
        query_params = {}
        if schema:
            # In BigQuery, schema is the dataset
            query_params["dataset_id"] = schema

        # Credentials are typically passed via application default credentials
        # or via credentials_info query parameter
        query_params["credentials_info"] = password

        return SqlAlchemyURL.create(
            drivername="bigquery",
            host=project_id,
            query=query_params,
        )

    @staticmethod
    def _build_trino_url(
        connection_params: SqlEngineConnectionParameterSet,
        password: str,
        schema: Optional[str] = None,
    ) -> SqlAlchemyURL:
        """Build Trino URL.

        Note - Trino has a "catalog" property in its URL that requires custom handling.
        However, it is currently encoded in their URL format in the same path location as the
        standard database value, so we simply use that the same way we would with a database that
        conforms to the standard SqlAlchemy URL format.
        """
        query_params = {}

        if schema:
            query_params["schema"] = schema

        return SqlAlchemyURL.create(
            drivername="trino",
            username=connection_params.username,
            password=password,
            host=connection_params.hostname,
            port=connection_params.port or 8080,
            database=connection_params.database,
            query=query_params,
        )
