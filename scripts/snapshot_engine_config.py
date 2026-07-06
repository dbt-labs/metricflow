"""Shared engine configuration for snapshot-generation scripts and tests."""

from __future__ import annotations

from typing import Final

DUCKDB_ENGINE_NAME: Final[str] = "duck_db"

# Maps the engine name in the credentials JSON to the `hatch` environment name.
ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME: Final[dict[str, str]] = {
    DUCKDB_ENGINE_NAME: "dev-env",
    "athena": "athena-env",
    "redshift": "redshift-env",
    "snowflake": "snowflake-env",
    "big_query": "bigquery-env",
    "databricks": "databricks-env",
    "postgres": "postgres-env",
    "trino": "trino-env",
}

ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS: Final[frozenset[str]] = frozenset(
    ("athena", "redshift", "snowflake", "big_query", "databricks")
)
