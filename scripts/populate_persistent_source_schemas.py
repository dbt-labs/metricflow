"""Script to help generate persistent source schemas with test data for all relevant engines."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from typing import Optional, Sequence

from scripts.generate_snapshots import (
    ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME,
    ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS,
    MetricFlowEngineConfiguration,
    load_credential_sets,
    run_hatch_command,
    set_engine_env_variables,
    setup_logging,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PopulatePersistentSourceSchemaConfig:  # noqa: D101
    # Engine to populate, or None to populate every relevant engine.
    engine: Optional[str]


def _parse_args(argv: Optional[Sequence[str]] = None) -> PopulatePersistentSourceSchemaConfig:
    parser = argparse.ArgumentParser(description="Populate persistent source schemas for supported SQL engines.")
    parser.add_argument(
        "--engine",
        choices=tuple(
            engine
            for engine in ENGINE_NAME_TO_HATCH_ENVIRONMENT_NAME
            if engine in ENGINES_WITH_PERSISTENT_SOURCE_SCHEMAS
        ),
        help="Populate the persistent source schema for only the specified engine.",
    )
    args = parser.parse_args(argv)
    return PopulatePersistentSourceSchemaConfig(engine=args.engine)


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


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Populate persistent source schemas for all relevant engines, or for the requested engine."""
    config = _parse_args(argv)
    setup_logging()
    engine_configs = load_credential_sets()
    if config.engine is not None:
        engine_configs = tuple(
            engine_config for engine_config in engine_configs if engine_config.engine == config.engine
        )
    for engine_config in engine_configs:
        logger.info(
            f"Populating persistent source schema for {engine_config.engine} with URL: "
            f"{engine_config.credential_set.engine_url}"
        )
        populate_schemas(engine_config)


if __name__ == "__main__":
    main()
