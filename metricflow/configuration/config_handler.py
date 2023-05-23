from __future__ import annotations

import os
import pathlib

from metricflow.configuration.constants import CONFIG_PATH_KEY
from metricflow.configuration.yaml_handler import YamlFileHandler


class ConfigHandler(YamlFileHandler):
    """Class to handle all config file retrieval/insertion actions."""

    def __init__(self) -> None:  # noqa: D
        # Create config directory if not exist
        dir_path = pathlib.Path(self.dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        super().__init__(yaml_file_path=self.file_path)

    @property
    def dir_path(self) -> str:
        """Retrieve MetricFlow config directory from $MF_CONFIG_DIR, default config dir is ~/.metricflow."""
        config_dir_env = os.getenv(CONFIG_PATH_KEY)
        return config_dir_env if config_dir_env else f"{str(pathlib.Path.home())}/.metricflow"

    @property
    def file_path(self) -> str:
        """Config file can be found at <config_dir>/config.yml."""
        return os.path.join(self.dir_path, "config.yml")

    @property
    def log_file_path(self) -> str:
        """Returns the name of the log file where all logging messages are stored."""
        log_dir_elements = [self.dir_path, "logs"]
        log_dir = os.path.join(*log_dir_elements)
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(*(log_dir_elements + ["metricflow.log"]))
