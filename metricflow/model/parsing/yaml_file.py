from dataclasses import dataclass


@dataclass(frozen=True)
class YamlFile:  # noqa: D
    file_path: str
    contents: str
