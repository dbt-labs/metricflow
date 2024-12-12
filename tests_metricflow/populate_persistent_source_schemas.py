"""Script to help generate persistent source schemas with test data for all relevant engines."""

from __future__ import annotations

import logging
import os

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.protocols.sql_client import SqlEngine
from tests_metricflow.generate_snapshots import (
    MetricFlowEngineConfiguration,
    run_cli,
    run_command,
    set_engine_env_variables,
)

logger = logging.getLogger(__name__)


def populate_schemas(test_configuration: MetricFlowEngineConfiguration) -> None:  # noqa: D103
    set_engine_env_variables(test_configuration)

    if test_configuration.engine is SqlEngine.DUCKDB or test_configuration.engine is SqlEngine.POSTGRES:
        # DuckDB & Postgres don't use persistent source schema
        return None
    elif (
        test_configuration.engine is SqlEngine.SNOWFLAKE
        or test_configuration.engine is SqlEngine.BIGQUERY
        or test_configuration.engine is SqlEngine.DATABRICKS
        or test_configuration.engine is SqlEngine.REDSHIFT
        or test_configuration.engine is SqlEngine.TRINO
    ):
        engine_name = test_configuration.engine.value.lower()
        os.environ["MF_TEST_ADAPTER_TYPE"] = engine_name
        hatch_env = f"{engine_name}-env"
        run_command(
            f"hatch -v run {hatch_env}:pytest -vv --log-cli-level info --use-persistent-source-schema "
            "tests_metricflow/source_schema_tools.py::populate_source_schema"
        )
    else:
        assert_values_exhausted(test_configuration.engine)


if __name__ == "__main__":
    run_cli(populate_schemas)
