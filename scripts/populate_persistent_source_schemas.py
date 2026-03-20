"""Script to help generate persistent source schemas with test data for all relevant engines."""

from __future__ import annotations

import logging

from tests_metricflow.generate_snapshots import (
    ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME,
    ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS,
    MetricFlowEngineConfiguration,
    load_credential_sets,
    run_hatch_command,
    set_engine_env_variables,
    setup_logging,
)

logger = logging.getLogger(__name__)


def populate_schemas(test_configuration: MetricFlowEngineConfiguration) -> None:  # noqa: D103
    set_engine_env_variables(test_configuration)

    if test_configuration.engine not in ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS:
        pass
    elif test_configuration.engine in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME:
        run_hatch_command(
            hatch_environment=test_configuration.hatch_environment,
            command=(
                "pytest",
                "-vv",
                "--log-cli-level",
                "info",
                "--use-persistent-source-schema",
                "tests_metricflow/source_schema_tools.py::populate_source_schema",
            ),
        )
    else:
        raise ValueError(f"Unsupported engine: {test_configuration.engine}")


if __name__ == "__main__":
    setup_logging()
    for test_configuration in load_credential_sets():
        logger.info(
            f"Populating persistent source schema for {test_configuration.engine} with URL: "
            f"{test_configuration.credential_set.engine_url}"
        )
        populate_schemas(test_configuration)
