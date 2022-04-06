from __future__ import annotations

import re
from typing import Optional

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.utils import HashableBaseModel
from metricflow.object_utils import ExtendedEnum
from metricflow.specs import LinkableElementReference


class Version(HashableBaseModel):  # noqa: D
    major: int
    minor: int

    _VERSION_REGEX = re.compile(r"^v[0-9]+\.[0-9]+$")

    @staticmethod
    def parse(version: str) -> Version:  # noqa: D

        if not Version._VERSION_REGEX.match(version):
            raise ParsingException(
                f"Expected a version of the form 'v<MAJOR_VERSION>.<MINOR_VERSION>' but got '{version}'."
            )
        if version[0] == "v":
            version = version[1:]

        parts = version.split(".")
        assert len(parts) == 2

        return Version(major=int(parts[0]), minor=int(parts[1]))

    def __str__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(major={self.major}, minor={self.minor})"


class Element:  # noqa: D
    name: LinkableElementReference
    expr: Optional[str]
    type: ExtendedEnum

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        raise NotImplementedError(
            f"Subclasses of Element must implement `is_primary_time`. This object is of type {type(self)}"
        )


class SourceFile(HashableBaseModel):  # noqa: D
    path: str
    contents: str


class Commit(HashableBaseModel):  # noqa: D
    commit: str
    timestamp: int


class FileSlice(HashableBaseModel):  # noqa: D
    filename: str
    content: str
    start_line_number: int
    end_line_number: int


class YamlConfigFile(HashableBaseModel):  # noqa: D
    filepath: str
    contents: str
    url: str


class SourceContext(HashableBaseModel):  # noqa: D
    definition_hash: str
