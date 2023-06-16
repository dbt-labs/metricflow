from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, Optional

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest

from metricflow.configuration.config_handler import ConfigHandler
from metricflow.configuration.constants import (
    CONFIG_DWH_SCHEMA,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.engine.utils import build_semantic_manifest_from_config
from metricflow.errors.errors import MetricFlowInitException, SqlClientCreationException
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config

logger = logging.getLogger(__name__)


class CLIContext:
    """Context for MetricFlow CLI."""

    def __init__(self) -> None:  # noqa: D
        self.verbose = False
        self._mf: Optional[MetricFlowEngine] = None
        self._sql_client: Optional[SqlClient] = None
        self._semantic_manifest: Optional[SemanticManifest] = None
        self._semantic_manifest_lookup: Optional[SemanticManifestLookup] = None
        self._mf_system_schema: Optional[str] = None
        self.config = ConfigHandler()
        self._configure_logging()

    def _configure_logging(self) -> None:  # noqa: D
        log_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_format)

        log_file_handler = TimedRotatingFileHandler(
            filename=self.config.log_file_path,
            # Rotate every day to a new file, keep 7 days worth.
            when="D",
            interval=1,
            backupCount=7,
        )

        formatter = logging.Formatter(fmt=log_format)
        log_file_handler.setFormatter(formatter)
        log_file_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()
        # StreamHandler to the console would have been setup by logging.basicConfig
        for handler in root_logger.handlers:
            handler.setLevel(logging.CRITICAL)
        root_logger.addHandler(log_file_handler)

    @property
    def mf_system_schema(self) -> str:  # noqa: D
        if self._mf_system_schema is None:
            self._mf_system_schema = self.config.get_value(CONFIG_DWH_SCHEMA)
        assert self._mf_system_schema
        return self._mf_system_schema

    def __initialize_sql_client(self) -> None:
        """Initializes the SqlClient given the credentials."""
        try:
            self._sql_client = make_sql_client_from_config(self.config)
        except Exception as e:
            raise SqlClientCreationException from e

    @property
    def sql_client(self) -> SqlClient:  # noqa: D
        if self._sql_client is None:
            # Initialize the SqlClient if not set
            self.__initialize_sql_client()
        assert self._sql_client is not None
        return self._sql_client

    def run_health_checks(self) -> Dict[str, Dict[str, str]]:
        """Execute the DB health checks."""
        try:
            schema_name = self.mf_system_schema
            table_name = "health_report"
            checks_to_run = [
                ("SELECT 1", lambda: self.sql_client.execute("SELECT 1")),
                (f"Create schema '{schema_name}'", lambda: self.sql_client.create_schema(schema_name)),
                (
                    f"Create table '{schema_name}.{table_name}' with a SELECT",
                    lambda: self.sql_client.execute(
                        f"CREATE TABLE {schema_name}.{table_name} AS SELECT 'test' AS test_col"
                    ),
                ),
                (
                    f"Drop table '{schema_name}.{table_name}'",
                    lambda: self.sql_client.drop_table(SqlTable(schema_name=schema_name, table_name="health_report")),
                ),
            ]

            results: Dict[str, Dict[str, str]] = {}

            for step, check in checks_to_run:
                status = "SUCCESS"
                err_string = ""
                try:
                    resp = check()
                    logger.info(
                        f"Health Check Item {step}: succeeded" + f" with response {str(resp)}" if resp else None
                    )
                except Exception as e:
                    status = "FAIL"
                    err_string = str(e)
                    logger.error(f"Health Check Item {step}: failed with error {err_string}")

                results[f"{self.sql_client.sql_engine_type} - {step}"] = {
                    "status": status,
                    "error_message": err_string,
                }

            return results
        except Exception as e:
            raise SqlClientCreationException from e

    def _initialize_metricflow_engine(self) -> None:
        """Initialize the MetricFlowEngine."""
        try:
            self._mf = MetricFlowEngine.from_dbt_project_root(self.config)
        except Exception as e:
            raise MetricFlowInitException from e

    @property
    def mf(self) -> MetricFlowEngine:  # noqa: D
        if self._mf is None:
            self._initialize_metricflow_engine()
        assert self._mf is not None
        return self._mf

    def _build_semantic_manifest_lookup(self) -> None:
        """Get the path to the models and create a corresponding SemanticManifestLookup."""
        self._semantic_manifest_lookup = SemanticManifestLookup(self.semantic_manifest)

    @property
    def semantic_manifest_lookup(self) -> SemanticManifestLookup:  # noqa: D
        if self._semantic_manifest_lookup is None:
            self._build_semantic_manifest_lookup()
        assert self._semantic_manifest_lookup is not None
        return self._semantic_manifest_lookup

    @property
    def semantic_manifest(self) -> SemanticManifest:  # noqa: D
        if self._semantic_manifest is None:
            self._semantic_manifest = build_semantic_manifest_from_config(self.config)

        assert self._semantic_manifest is not None
        return self._semantic_manifest
