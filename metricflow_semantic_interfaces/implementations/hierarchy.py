from __future__ import annotations

import logging
from typing import Optional, Sequence

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.protocols.hierarchy import DimensionHierarchy

logger = logging.getLogger(__name__)


class PydanticDimensionHierarchyLevel(PydanticCustomInputParser, HashableBaseModel):  # noqa: D101
    dimension: str
    parent: Optional[str] = None

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticDimensionHierarchyLevel:  # noqa: D102
        if isinstance(input, str):
            return PydanticDimensionHierarchyLevel(dimension=input)
        else:
            raise ValueError(
                f"DimensionHierarchyLevel inputs from model configs are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )


class PydanticDimensionHierarchy(HashableBaseModel, ProtocolHint[DimensionHierarchy]):  # noqa: D101
    @override
    def _implements_protocol(self) -> DimensionHierarchy:  # noqa: D102
        return self

    name: str
    label: Optional[str] = None
    levels: Sequence[PydanticDimensionHierarchyLevel]
