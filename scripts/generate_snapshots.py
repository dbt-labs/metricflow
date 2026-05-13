"""Script to help generate test snapshots for all supported engines.

Credentials are stored as a JSON string in an environment variable set via a shell command like:

export MF_TEST_ENGINE_CREDENTIAL_SETS=$(cat <<EOF
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
    "doris": {
        "engine_url": "doris://...",
        "engine_password": "..."
    },
}
EOF
)
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
from dataclasses import dataclass
from typing import Final, Optional, Sequence, cast

logger = logging.getLogger(__name__)


MF_TEST_DIRECTORY = "tests_metricflow"
MF_SEMANTICS_TEST_DIRECTORY = "tests_metricflow_semantics"

# Tests that generate SQL engine snapshots have this `pytest` marker set.
SQL_ENGINE_SNAPSHOT_MARKER_NAME = "sql_engine_snapshot"

DUCKDB_ENGINE_NAME = "duck_db"

# Maps the engine name in the credentials JSON to the `hatch` environment name.
ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME: Final[dict[str, str]] = {
    DUCKDB_ENGINE_NAME: "dev-env",
    "redshift": "redshift-env",
    "snowflake": "snowflake-env",
    "big_query": "bigquery-env",
    "databricks": "databricks-env",
    "postgres": "postgres-env",
    "trino": "trino-env",
    "doris": "doris-env",
}

ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS: Final[frozenset[str]] = frozenset(
    ("redshift", "snowflake", "big_query", "databricks")
)


@dataclass(frozen=True)
class MetricFlowTestCredentialSet:  # noqa: D101
    engine_url: Optional[str]
    engine_password: Optional[str]


@dataclass(frozen=True)
class MetricFlowEngineConfiguration:  # noqa: D101
    engine: str
    hatch_environment: str
    credential_set: MetricFlowTestCredentialSet


def _credential_set_from_json(credential_set_json: dict[str, object]) -> MetricFlowTestCredentialSet:
    return MetricFlowTestCredentialSet(
        engine_url=cast(Optional[str], credential_set_json.get("engine_url")),
        engine_password=cast(Optional[str], credential_set_json.get("engine_password")),
    )


def parse_credential_sets(credential_sets_json_str: str) -> Sequence[MetricFlowEngineConfiguration]:
    """Parse the JSON credential config for all engines."""
    credential_sets_json = cast(dict[str, dict[str, object]], json.loads(credential_sets_json_str))

    return tuple(
        MetricFlowEngineConfiguration(
            engine=engine,
            hatch_environment=hatch_environment,
            credential_set=_credential_set_from_json(credential_sets_json[engine]),
        )
        for engine, hatch_environment in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME.items()
    )


def run_hatch_command(hatch_environment: str, command: Sequence[str]) -> None:
    """Run a command inside the given `hatch` environment."""
    if len(command) == 0:
        raise ValueError(f"Command cannot be empty: {command!r}")

    complete_command = ["hatch", "-v", "run", f"{hatch_environment}:{command[0]}"]
    complete_command.extend(command[1:])
    logger.info(f"Running command: {shlex.join(complete_command)}")
    subprocess.run(complete_command, check=True)


def set_engine_env_variables(test_configuration: MetricFlowEngineConfiguration) -> None:
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


def run_tests(test_configuration: MetricFlowEngineConfiguration) -> None:  # noqa: D103
    set_engine_env_variables(test_configuration)

    if test_configuration.engine == DUCKDB_ENGINE_NAME:
        # DuckDB is fast, so generate all snapshots, including the engine-agnostic ones
        run_hatch_command(
            hatch_environment=test_configuration.hatch_environment,
            command=(
                "pytest",
                "-x",
                "-vv",
                "-n",
                "4",
                "--overwrite-snapshots",
                "-k",
                "not itest",
                MF_TEST_DIRECTORY,
            ),
        )

        # Run snapshots changes for metricflow-semantics
        # these are not dialect specific, so only need to run once
        run_hatch_command(
            hatch_environment=test_configuration.hatch_environment,
            command=(
                "pytest",
                "-x",
                "-vv",
                "-n",
                "4",
                "--overwrite-snapshots",
                MF_SEMANTICS_TEST_DIRECTORY,
            ),
        )
    elif test_configuration.engine in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME:
        command = [
            "pytest",
            "-x",
            "-vv",
            "-n",
            "4",
            "--overwrite-snapshots",
        ]
        if test_configuration.engine in ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS:
            command.append("--use-persistent-source-schema")
        command.extend(
            [
                "-m",
                SQL_ENGINE_SNAPSHOT_MARKER_NAME,
                MF_TEST_DIRECTORY,
            ]
        )
        run_hatch_command(hatch_environment=test_configuration.hatch_environment, command=command)
    else:
        raise ValueError(f"Unsupported engine: {test_configuration.engine}")


def setup_logging() -> None:
    """Set up logging for the script."""
    dev_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=dev_format)


def load_credential_sets() -> Sequence[MetricFlowEngineConfiguration]:
    """Load test credential sets from the environment."""
    credential_sets_json_str = os.environ.get("MF_TEST_ENGINE_CREDENTIAL_SETS")
    if credential_sets_json_str is None:
        raise ValueError(
            f"Environment variable: MF_TEST_ENGINE_CREDENTIAL_SETS has not been set. Please see the comment in "
            f"{__file__} for details on how to set it."
        )

    return parse_credential_sets(credential_sets_json_str)


if __name__ == "__main__":
    setup_logging()
    credential_sets = load_credential_sets()
    logger.info(f"Running tests in {MF_TEST_DIRECTORY} with the marker {SQL_ENGINE_SNAPSHOT_MARKER_NAME}")
    for test_configuration in credential_sets:
        logger.info(
            f"Running tests for {test_configuration.engine} with URL: {test_configuration.credential_set.engine_url}"
        )
        run_tests(test_configuration)
