from __future__ import annotations

from typing import Optional

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)


class PydanticSemanticVersion(PydanticCustomInputParser, HashableBaseModel):
    """Pydantic implementation of SemanticVersion."""

    major_version: str
    minor_version: str
    patch_version: Optional[str]

    @classmethod
    @override
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticSemanticVersion:
        if isinstance(input, str):
            return PydanticSemanticVersion.create_from_string(input)
        else:
            raise ValueError(
                f"{cls.__name__} inputs from YAML files are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )

    @staticmethod
    def create_from_string(version_str: str) -> PydanticSemanticVersion:  # noqa: D102
        version_str_split = version_str.split(".")
        if len(version_str_split) < 2:
            raise ValueError(f"Expected version string to be of the form x.y or x.y.z, but got {version_str}")
        return PydanticSemanticVersion(
            major_version=version_str_split[0],
            minor_version=version_str_split[1],
            patch_version=".".join(version_str_split[2:]) if len(version_str_split) >= 3 else None,
        )


UNKNOWN_VERSION_SENTINEL = PydanticSemanticVersion(major_version="0", minor_version="0", patch_version="0")
