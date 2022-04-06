from dataclasses import dataclass
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from typing import List, Optional, TextIO


@dataclass(frozen=True)
class ConfigKey:
    """Dataclass to represent a yaml key"""

    key: str
    value: str = ""
    comment: Optional[str] = None


class YamlTemplateBuilder:
    """Class to construct and write YAML files."""

    @staticmethod
    def write_yaml(config_keys: List[ConfigKey], file_stream: TextIO) -> None:  # noqa: D
        yaml = YAML()
        file_data = CommentedMap()
        for key in config_keys:
            file_data.insert(0, key.key, key.value, comment=key.comment)
        yaml.dump(file_data, file_stream)
