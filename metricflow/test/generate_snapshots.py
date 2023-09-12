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
}
EOF
)
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.implementations.base import FrozenBaseModel
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.protocols.sql_client import SqlEngine

logger = logging.getLogger(__name__)


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
        )


SNAPSHOT_GENERATING_TEST_FILES = (
    "metricflow/test/dataflow/optimizer/source_scan/test_cm_branch_combiner.py",
    "metricflow/test/sql/optimizer/test_column_pruner.py",
    "metricflow/test/dataset/test_convert_semantic_model.py",
    "metricflow/test/model/test_data_warehouse_tasks.py",
    "metricflow/test/dataflow/builder/test_dataflow_plan_builder.py",
    "metricflow/test/plan_conversion/test_dataflow_to_execution.py",
    "metricflow/test/plan_conversion/test_dataflow_to_sql_plan.py",
    "metricflow/test/sql/test_engine_specific_rendering.py",
    "metricflow/test/plan_conversion/dataflow_to_sql/test_metric_time_dimension_to_sql.py",
    "metricflow/test/integration/test_rendered_query.py",
    "metricflow/test/sql/optimizer/test_rewriting_sub_query_reducer.py",
    "metricflow/test/sql/test_sql_plan_render.py",
    "metricflow/test/integration/test_rendered_query.py",
    "metricflow/test/sql/optimizer/test_sub_query_reducer.py",
    "metricflow/test/dataflow/optimizer/source_scan/test_source_scan_optimizer.py",
    "metricflow/test/sql/test_sql_plan_render.py",
    "metricflow/test/sql/optimizer/test_sub_query_reducer.py",
    "metricflow/test/sql/optimizer/test_table_alias_simplifier.py",
)


def run_command(command: str) -> None:  # noqa: D
    logger.info(f"Running command {command}")
    return_code = os.system(command)
    if return_code != 0:
        raise RuntimeError(f"Error running command: {command}")


def run_tests(test_configuration: MetricFlowTestConfiguration, test_file_paths: Sequence[str]) -> None:  # noqa: D
    combined_paths = " ".join(test_file_paths)
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

    if test_configuration.engine is SqlEngine.DUCKDB:
        # Can't use --use-persistent-source-schema with duckdb since it's in memory.
        run_command(f"pytest -x -vv -n 4 --overwrite-snapshots {combined_paths}")
    elif (
        test_configuration.engine is SqlEngine.REDSHIFT
        or test_configuration.engine is SqlEngine.SNOWFLAKE
        or test_configuration.engine is SqlEngine.BIGQUERY
        or test_configuration.engine is SqlEngine.DATABRICKS
        or test_configuration.engine is SqlEngine.POSTGRES
    ):
        engine_name = test_configuration.engine.value.lower()
        os.environ["MF_TEST_ADAPTER_TYPE"] = engine_name
        hatch_env = f"{engine_name}-env"
        use_persistent_source_schema = test_configuration.engine != SqlEngine.POSTGRES
        run_command(
            f"hatch -v run {hatch_env}:pytest -x -vv -n 4 "
            f"--overwrite-snapshots"
            f"{' --use-persistent-source-schema' if use_persistent_source_schema else ''}"
            f" {combined_paths}"
        )
    else:
        assert_values_exhausted(test_configuration.engine)


def run_cli() -> None:  # noqa: D
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

    logger.info(
        f"Running the following tests to generate snapshots:\n{pformat_big_objects(SNAPSHOT_GENERATING_TEST_FILES)}"
    )

    for test_configuration in credential_sets.as_configurations:
        logger.info(
            f"Running tests for {test_configuration.engine} with URL: {test_configuration.credential_set.engine_url}"
        )
        run_tests(test_configuration, SNAPSHOT_GENERATING_TEST_FILES)


if __name__ == "__main__":
    run_cli()
