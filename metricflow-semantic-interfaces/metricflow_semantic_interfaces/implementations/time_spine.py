from __future__ import annotations

from typing import Optional, Sequence

from msi_pydantic_shim import Field
from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import HashableBaseModel
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.protocols.time_spine import (
    TimeSpine,
    TimeSpineCustomGranularityColumn,
    TimeSpinePrimaryColumn,
)
from metricflow_semantic_interfaces.type_enums import TimeGranularity


class PydanticTimeSpinePrimaryColumn(HashableBaseModel, ProtocolHint[TimeSpinePrimaryColumn]):  # noqa: D101
    @override
    def _implements_protocol(self) -> TimeSpinePrimaryColumn:
        return self

    name: str
    time_granularity: TimeGranularity


class PydanticTimeSpineCustomGranularityColumn(  # noqa: D101
    HashableBaseModel, ProtocolHint[TimeSpineCustomGranularityColumn]
):
    @override
    def _implements_protocol(self) -> TimeSpineCustomGranularityColumn:
        return self

    name: str
    column_name: Optional[str] = None

    @property
    def parsed_column_name(self) -> str:
        """The name of the column in the time spine table that contains this custom granularity.

        For convenience in writing configs, if there is no `column_name` set, we assume the `name`
        is also the column name.
        """
        return self.column_name or self.name


class PydanticTimeSpine(HashableBaseModel, ProtocolHint[TimeSpine]):  # noqa: D101
    @override
    def _implements_protocol(self) -> TimeSpine:
        return self

    node_relation: PydanticNodeRelation
    primary_column: PydanticTimeSpinePrimaryColumn
    custom_granularities: Sequence[PydanticTimeSpineCustomGranularityColumn] = Field(default_factory=list)
