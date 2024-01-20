"""Script to help generate test snapshots for all supported engines.

Credentials are stored as a JSON string in an environment variable set via a shell command like:

export MF_TEST_ENGINE_CREDENTIALS=$(cat <<EOF
{
    "duck_db": {
        "engine_url": null,
        "engine_password": null
    },
    "redshift": {
        "engine_url": "redshift://...",
        "engine_password": "..."
    },
    "snowflake": {
        "engine_url": "snowflake://...",
        "engine_password": "..."
    },
    "big_query": {
        "engine_url": "bigquery://",
        "engine_password": "..."
    },
    "databricks": {
        "engine_url": "databricks://...",
        "engine_password": "..."
    },
    "postgres": {
        "engine_url": postgres://...",
        "engine_password": "..."
    },
    "trino": {
        "engine_url": trino://...",
        "engine_password": "..."
    },
}
EOF
)
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.base import FrozenBaseModel

from metricflow.protocols.sql_client import SqlEngine
from metricflow.test.fixtures.setup_fixtures import SQL_ENGINE_SNAPSHOT_MARKER_NAME

logger = logging.getLogger(__name__)


TEST_DIRECTORY = "metricflow/test"


class MetricFlowTestCredentialSet(FrozenBaseModel):  # noqa: D
    engine_url: Optional[str]
    engine_password: Optional[str]


@dataclass(frozen=True)
class MetricFlowTestConfiguration:  # noqa: D
    engine: SqlEngine
    credential_set: MetricFlowTestCredentialSet


class MetricFlowTestCredentialSetForAllEngines(FrozenBaseModel):  # noqa: D
    duck_db: MetricFlowTestCredentialSet
    redshift: MetricFlowTestCredentialSet
    snowflake: MetricFlowTestCredentialSet
    big_query: MetricFlowTestCredentialSet
    databricks: MetricFlowTestCredentialSet
    postgres: MetricFlowTestCredentialSet
    trino: MetricFlowTestCredentialSet

    @property
    def as_configurations(self) -> Sequence[MetricFlowTestConfiguration]:  # noqa: D
        return (
            MetricFlowTestConfiguration(
                engine=SqlEngine.DUCKDB,
                credential_set=self.duck_db,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.REDSHIFT,
                credential_set=self.redshift,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.SNOWFLAKE,
                credential_set=self.snowflake,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.BIGQUERY,
                credential_set=self.big_query,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.DATABRICKS,
                credential_set=self.databricks,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.POSTGRES,
                credential_set=self.postgres,
            ),
            MetricFlowTestConfiguration(
                engine=SqlEngine.TRINO,
                credential_set=self.trino,
            ),
        )


def run_command(command: str) -> None:  # noqa: D
    logger.info(f"Running command {command}")
    return_code = os.system(command)
    if return_code != 0:
        raise RuntimeError(f"Error running command: {command}")


def set_engine_env_variables(test_configuration: MetricFlowTestConfiguration) -> None:
    """Set connection env variables dynamically for the engine being used.

    Requires MF_TEST_ENGINE_CREDENTIALS env variable to be set with creds for all engines.
    """
    if test_configuration.credential_set.engine_url is None:
        if "MF_SQL_ENGINE_URL" in os.environ:
            del os.environ["MF_SQL_ENGINE_URL"]
    else:
        os.environ["MF_SQL_ENGINE_URL"] = test_configuration.credential_set.engine_url

    if test_configuration.credential_set.engine_password is None:
        if "MF_SQL_ENGINE_PASSWORD" in os.environ:
            del os.environ["MF_SQL_ENGINE_PASSWORD"]
    else:
        os.environ["MF_SQL_ENGINE_PASSWORD"] = test_configuration.credential_set.engine_password


def run_tests(test_configuration: MetricFlowTestConfiguration) -> None:  # noqa: D
    set_engine_env_variables(test_configuration)

    if test_configuration.engine is SqlEngine.DUCKDB:
        # DuckDB is fast, so generate all snapshots, including the engine-agnostic ones
        run_command(f"pytest -x -vv -n 4 --overwrite-snapshots -k 'not itest' {TEST_DIRECTORY}")
    elif (
        test_configuration.engine is SqlEngine.REDSHIFT
        or test_configuration.engine is SqlEngine.SNOWFLAKE
        or test_configuration.engine is SqlEngine.BIGQUERY
        or test_configuration.engine is SqlEngine.DATABRICKS
        or test_configuration.engine is SqlEngine.POSTGRES
        or test_configuration.engine is SqlEngine.TRINO
    ):
        engine_name = test_configuration.engine.value.lower()
        os.environ["MF_TEST_ADAPTER_TYPE"] = engine_name
        hatch_env = f"{engine_name}-env"
        use_persistent_source_schema = (
            test_configuration.engine != SqlEngine.POSTGRES and test_configuration.engine != SqlEngine.TRINO
        )
        run_command(
            f"hatch -v run {hatch_env}:pytest -x -vv -n 4 "
            f"--overwrite-snapshots"
            f"{' --use-persistent-source-schema' if use_persistent_source_schema else ''}"
            f" -m '{SQL_ENGINE_SNAPSHOT_MARKER_NAME}' "
            f"{TEST_DIRECTORY}"
        )
    else:
        assert_values_exhausted(test_configuration.engine)


def run_cli(function_to_run: Callable) -> None:  # noqa: D
    # Setup logging.
    dev_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=dev_format)

    credential_sets_json_str = os.environ.get("MF_TEST_ENGINE_CREDENTIAL_SETS")
    if credential_sets_json_str is None:
        raise ValueError(
            f"Environment variable: MF_TEST_ENGINE_CREDENTIAL_SETS has not been set. Please see the comment in "
            f"{__file__} for details on how to set it."
        )

    credential_sets = MetricFlowTestCredentialSetForAllEngines.parse_raw(credential_sets_json_str)

    logger.info(f"Running tests in '{TEST_DIRECTORY}' with the marker '{SQL_ENGINE_SNAPSHOT_MARKER_NAME}'")

    for test_configuration in credential_sets.as_configurations:
        logger.info(
            f"Running tests for {test_configuration.engine} with URL: {test_configuration.credential_set.engine_url}"
        )
        function_to_run(test_configuration)


if __name__ == "__main__":
    run_cli(run_tests)
