from __future__ import annotations

import logging
import pathlib
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, Optional

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

from dbt_metricflow.cli import PACKAGE_NAME
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts, dbtProjectMetadata
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


class CLIConfiguration:
    """Configuration object used for the MetricFlow CLI."""

    def __init__(self) -> None:  # noqa: D107
        self.verbose = False
        self._dbt_project_metadata: Optional[dbtProjectMetadata] = None
        self._dbt_artifacts: Optional[dbtArtifacts] = None
        self._mf: Optional[MetricFlowEngine] = None
        self._sql_client: Optional[SqlClient] = None
        self._semantic_manifest: Optional[SemanticManifest] = None
        self._semantic_manifest_lookup: Optional[SemanticManifestLookup] = None

    def setup(self) -> None:
        """Setup this configuration for executing commands.

        The dbt_artifacts construct must be loaded in order for logging configuration to work correctly.
        """
        dbt_project_path = pathlib.Path.cwd()
        try:
            self._dbt_project_metadata = dbtProjectMetadata.load_from_project_path(dbt_project_path)

            # self.log_file_path invokes the dbtRunner. If this is done after the configure_logging call all of the
            # dbt CLI logging configuration could be overridden, resulting in lots of things printing to console
            self._configure_logging(log_file_path=self.log_file_path)
        except Exception as e:
            exception_message = str(e)
            if exception_message.find("Could not find adapter type") != -1:
                raise RuntimeError(
                    f"Got an error during setup, potentially due to a missing adapter package. Has the appropriate "
                    f"adapter package (`{PACKAGE_NAME}[dbt-*]`) for the dbt project {str(dbt_project_path)!r} been "
                    f"installed? If not please install it (e.g. `pip install '{PACKAGE_NAME}[dbt-duckdb]')."
                ) from e
            else:
                raise e

    def _get_dbt_project_metadata(self) -> dbtProjectMetadata:
        if self._dbt_project_metadata is None:
            raise RuntimeError(
                f"{self.__class__.__name__}.setup() should have been called before accessing the configuration."
            )
        return self._dbt_project_metadata

    def _configure_logging(self, log_file_path: pathlib.Path) -> None:
        """Initialize the logging spec for the CLI.

        This requires a fully loaded dbt project, including what amounts to a call to dbt debug.
        As a practical matter, this should not have much end user impact except in cases where they are
        using commands that do not require a working adapter AND the call to dbt debug runs slowly.
        In future we may have better API access to the log file location for the project, at which time
        we can swap this out and return to full lazy loading for any context attributes that are slow
        to initialize.
        """
        log_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
        logging.basicConfig(level=logging.INFO, format=log_format)

        log_file_handler = TimedRotatingFileHandler(
            filename=log_file_path,
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
    def dbt_project_metadata(self) -> dbtProjectMetadata:
        """Property accessor for dbt project metadata, useful in cases where the full manifest load is not needed."""
        return self._get_dbt_project_metadata()

    @property
    def dbt_artifacts(self) -> dbtArtifacts:
        """Property accessor for all dbt artifacts, used for powering the sql client (among other things)."""
        if self._dbt_artifacts is None:
            self._dbt_artifacts = dbtArtifacts.load_from_project_metadata(self.dbt_project_metadata)
        return self._dbt_artifacts

    @property
    def log_file_path(self) -> pathlib.Path:
        """Returns the location of the log file path for this CLI invocation."""
        # The dbt Project.log_path attribute is currently sourced from the final runtime config value accessible
        # through the CLI state flags. As such, it will deviate from the default based on the DBT_LOG_PATH environment
        # variable. Should this behavior change, we will need to update this call.
        return pathlib.Path(self.dbt_project_metadata.project.log_path, "metricflow.log")

    @property
    def sql_client(self) -> SqlClient:
        """Property accessor for the sql_client class used in the CLI."""
        if self._sql_client is None:
            self._sql_client = AdapterBackedSqlClient(self.dbt_artifacts.adapter)

        return self._sql_client

    def run_health_checks(self) -> Dict[str, Dict[str, str]]:
        """Execute the DB health checks."""
        checks_to_run = [
            ("SELECT 1", lambda: self.sql_client.execute("SELECT 1")),
        ]

        results: Dict[str, Dict[str, str]] = {}

        for step, check in checks_to_run:
            status = "SUCCESS"
            err_string = ""
            try:
                resp = check()
                logger.info(f"Health Check Item {step}: succeeded" + f" with response {str(resp)}" if resp else None)
            except Exception as e:
                status = "FAIL"
                err_string = str(e)
                logger.error(LazyFormat(lambda: f"Health Check Item {step}: failed with error {err_string}"))

            results[f"{self.sql_client.sql_engine_type} - {step}"] = {
                "status": status,
                "error_message": err_string,
            }

        return results

    @property
    def mf(self) -> MetricFlowEngine:  # noqa: D102
        if self._mf is None:
            self._mf = MetricFlowEngine(
                semantic_manifest_lookup=self.semantic_manifest_lookup,
                sql_client=self.sql_client,
            )
        assert self._mf is not None
        return self._mf

    def _build_semantic_manifest_lookup(self) -> None:
        """Get the path to the models and create a corresponding SemanticManifestLookup."""
        self._semantic_manifest_lookup = SemanticManifestLookup(self.semantic_manifest)

    @property
    def semantic_manifest_lookup(self) -> SemanticManifestLookup:  # noqa: D102
        if self._semantic_manifest_lookup is None:
            self._build_semantic_manifest_lookup()
        assert self._semantic_manifest_lookup is not None
        return self._semantic_manifest_lookup

    @property
    def semantic_manifest(self) -> SemanticManifest:
        """Retrieve the semantic manifest from the dbt project root."""
        return self.dbt_artifacts.semantic_manifest
