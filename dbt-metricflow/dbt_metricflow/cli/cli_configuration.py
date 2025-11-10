from __future__ import annotations

import logging
import os
import pathlib
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, Optional

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

from dbt_metricflow.cli import PACKAGE_NAME
from dbt_metricflow.cli.cli_errors import LoadSemanticManifestException
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts, dbtProjectMetadata
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.protocols.sql_client import SqlClient

logger = logging.getLogger(__name__)


class CLIConfiguration:
    """Configuration object used for the MetricFlow CLI."""

    LOG_FILE_NAME = "metricflow.log"
    DBT_PROFILES_DIR_ENV_VAR_NAME = "DBT_PROFILES_DIR"
    DBT_PROJECT_DIR_ENV_VAR_NAME = "DBT_PROJECT_DIR"

    # Message to log when logging has been set up. Useful for checking the logging in tests.
    LOG_SETUP_MESSAGE = "Set up MF CLI logging"

    def __init__(self) -> None:  # noqa: D107
        self.verbose = False
        self._dbt_project_metadata: Optional[dbtProjectMetadata] = None
        self._dbt_artifacts: Optional[dbtArtifacts] = None
        self._mf: Optional[MetricFlowEngine] = None
        self._sql_client: Optional[SqlClient] = None
        self._semantic_manifest: Optional[SemanticManifest] = None
        self._semantic_manifest_lookup: Optional[SemanticManifestLookup] = None
        self._is_setup = False

    @property
    def is_setup(self) -> bool:
        """Returns true if this configuration object has already been set up.

        This can be used of avoid running `setup()` multiple times when a single configuration object is shared
        between test cases.
        """
        return self._is_setup

    def setup(
        self,
        dbt_profiles_path: Optional[pathlib.Path] = None,
        dbt_project_path: Optional[pathlib.Path] = None,
        configure_file_logging: bool = True,
    ) -> None:
        """Setup this configuration for executing commands.

        The dbt_artifacts construct must be loaded in order for logging configuration to work correctly.

        Args:
            dbt_profiles_path: The directory containing the `profiles.yml` file. If not specified, the CWD is used.
            dbt_project_path: The directory containing the dbt project. If not specified, the CWD is used.
            configure_file_logging: If true, configure the Python logger to log to a rotating log file as specified by
            `self.log_file_path`. This can be set to false in tests to better manage log output.

        Returns: None
        """
        cwd = pathlib.Path.cwd()

        dbt_profiles_dir_env_var = os.environ.get(CLIConfiguration.DBT_PROFILES_DIR_ENV_VAR_NAME)
        dbt_profiles_dir_from_env = (
            pathlib.Path(dbt_profiles_dir_env_var) if dbt_profiles_dir_env_var is not None else None
        )

        dbt_project_dir_env_var = os.environ.get(CLIConfiguration.DBT_PROJECT_DIR_ENV_VAR_NAME)
        dbt_project_dir_from_env = (
            pathlib.Path(dbt_project_dir_env_var) if dbt_project_dir_env_var is not None else None
        )

        dbt_profiles_path = dbt_profiles_path or dbt_profiles_dir_from_env or cwd
        dbt_project_path = dbt_project_path or dbt_project_dir_from_env or cwd
        dbt_project_yaml_path = dbt_project_path / "dbt_project.yml"
        if not dbt_project_yaml_path.exists():
            raise LoadSemanticManifestException(f"Missing: {str(dbt_project_yaml_path)!r}")

        try:
            self._dbt_project_metadata = dbtProjectMetadata.load_from_paths(
                profiles_path=dbt_profiles_path,
                project_path=dbt_project_path,
            )

            # self.log_file_path invokes the dbtRunner. If this is done after the configure_logging call all of the
            # dbt CLI logging configuration could be overridden, resulting in lots of things printing to console
            if configure_file_logging:
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

        self._is_setup = True

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
        In the future we may have better API access to the log file location for the project, at which time
        we can swap this out and return to full lazy loading for any context attributes that are slow
        to initialize.
        """
        root_logger = logging.getLogger()
        previous_root_logger_level = root_logger.level
        previous_root_logger_handlers = tuple(root_logger.handlers)

        # Show >= CRITICAL logs in the console
        stream_handler = StreamHandler()
        stream_handler_log_level = logging.CRITICAL
        stream_handler.setLevel(stream_handler_log_level)

        # Show >= INFO logs in the log file
        log_file_handler_log_level = logging.INFO
        log_file_handler = TimedRotatingFileHandler(
            filename=log_file_path,
            # Rotate every day to a new file, keep 7 days worth.
            when="D",
            interval=1,
            backupCount=7,
        )
        log_file_handler.setLevel(log_file_handler_log_level)

        root_logger_level = logging.INFO
        log_format = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d [%(threadName)s] - %(message)s"
        # `logging.basicConfig` is a no-op without the `force=True` if any handlers have been added to the root logger.
        # Imported modules might add handlers, so set the flag to ensure that the above handlers are used.
        logging.basicConfig(
            level=root_logger_level, format=log_format, handlers=[stream_handler, log_file_handler], force=True
        )

        logger.info(
            LazyFormat(
                CLIConfiguration.LOG_SETUP_MESSAGE,
                root_logger_level=logging.getLevelName(root_logger_level),
                root_logger_handlers=root_logger.handlers,
                previous_root_logger_level=logging.getLevelName(previous_root_logger_level),
                previous_root_logger_handlers=previous_root_logger_handlers,
                log_format=log_format,
            )
        )

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
        return pathlib.Path(self.dbt_project_metadata.project.log_path, CLIConfiguration.LOG_FILE_NAME)

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
