from __future__ import annotations

import os
from typing import Dict, Optional

import yaml


class YamlFileHandler:
    """Class to handle interactions with a non-nested yaml."""

    def __init__(self, yaml_file_path: str) -> None:  # noqa: D
        self.yaml_file_path = yaml_file_path

    def _load_yaml(self) -> Dict[str, str]:
        """Reads the provided yaml file and loads it into a dictionary."""
        content: Dict[str, str] = {}
        if os.path.exists(self.yaml_file_path):
            with open(self.yaml_file_path) as f:
                content = yaml.load(f, Loader=yaml.SafeLoader) or {}
        return content

    def get_value(self, key: str) -> Optional[str]:
        """Attempts to get a corresponding value from the yaml file. Throw an error if not exists or None."""
        content = self._load_yaml()
        return content.get(key)

    def set_value(self, key: str, value: str) -> None:
        """Sets a value to a given key in yaml file."""
        content = self._load_yaml()
        content[key] = value
        with open(self.yaml_file_path, "w") as f:
            yaml.dump(content, f)

    def remove_value(self, key: str) -> None:
        """Removes a key in yaml file."""
        content = self._load_yaml()
        if key not in content:
            return
        del content[key]
        with open(self.yaml_file_path, "w") as f:
            yaml.dump(content, f)

    @property
    def url(self) -> str:
        """Returns the file url of this handler."""
        return f"file:/{self.yaml_file_path}"
