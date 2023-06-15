"""Script to help generate test snapshots for all supported engines.

Credentials are stored as a JSON string in an environment variable set via a shell command like:

export MF_TEST_ENGINE_CREDENTIALS=$(cat <<EOF
{
    "duck_db": {
        "engine_url": null,
        "engine_password": null
    },
    "postgres": {
        "engine_url": "postgresql://...",
        "engine_password": "..."
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
    }
}
EOF
)
"""
from __future__ import annotations

import logging
import os
from typing import Optional, Sequence

from dbt_semantic_interfaces.implementations.base import FrozenBaseModel
from dbt_semantic_interfaces.pretty_print import pformat_big_objects

from metricflow.configuration.env_var import EnvironmentVariable

logger = logging.getLogger(__name__)


class MetricFlowTestCredentialSet(FrozenBaseModel):  # noqa: D
    engine_url: Optional[str]
    engine_password: Optional[str]


class MetricFlowTestCredentialSetForAllEngines(FrozenBaseModel):  # noqa: D
    duck_db: MetricFlowTestCredentialSet
    postgres: MetricFlowTestCredentialSet
    redshift: MetricFlowTestCredentialSet
    snowflake: MetricFlowTestCredentialSet
    big_query: MetricFlowTestCredentialSet
    databricks: MetricFlowTestCredentialSet

    @property
    def as_sequence(self) -> Sequence[MetricFlowTestCredentialSet]:  # noqa: D
        return (self.duck_db, self.postgres, self.redshift, self.snowflake, self.big_query, self.databricks)


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


def run_tests(engine_credential_set: MetricFlowTestCredentialSet, test_file_paths: Sequence[str]) -> None:  # noqa: D
    combined_paths = " ".join(test_file_paths)
    if engine_credential_set.engine_url is None:
        if "MF_SQL_ENGINE_URL" in os.environ:
            del os.environ["MF_SQL_ENGINE_URL"]
    else:
        os.environ["MF_SQL_ENGINE_URL"] = engine_credential_set.engine_url

    if engine_credential_set.engine_password is None:
        if "MF_SQL_ENGINE_PASSWORD" in os.environ:
            del os.environ["MF_SQL_ENGINE_PASSWORD"]
    else:
        os.environ["MF_SQL_ENGINE_PASSWORD"] = engine_credential_set.engine_password
    run_command(f"pytest -x -vv -n 4 --overwrite-snapshots {combined_paths}")


def run_cli() -> None:  # noqa: D
    # Setup logging.
    dev_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
    logging.basicConfig(level=logging.INFO, format=dev_format)

    credential_sets_json_env_var = EnvironmentVariable("MF_TEST_ENGINE_CREDENTIAL_SETS")
    if credential_sets_json_env_var.get_optional() is None:
        raise ValueError(
            f"Environment variable: {credential_sets_json_env_var.name} has not been set. Please see the comment in "
            f"{__file__} for details on how to set it."
        )

    credentials_sets_json_str = credential_sets_json_env_var.get()
    credential_sets = MetricFlowTestCredentialSetForAllEngines.parse_raw(credentials_sets_json_str)

    logger.info(
        f"Running the following tests to generate snapshots:\n{pformat_big_objects(SNAPSHOT_GENERATING_TEST_FILES)}"
    )

    for credential_set in credential_sets.as_sequence:
        logger.info(f"Running test for {credential_set.engine_url}")
        run_tests(credential_set, SNAPSHOT_GENERATING_TEST_FILES)


if __name__ == "__main__":
    run_cli()
