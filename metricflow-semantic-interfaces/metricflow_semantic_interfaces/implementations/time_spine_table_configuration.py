from __future__ import annotations

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.protocols.time_spine_configuration import (
    TimeSpineTableConfiguration,
)
from metricflow_semantic_interfaces.type_enums import TimeGranularity


class PydanticTimeSpineTableConfiguration(
    HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[TimeSpineTableConfiguration]
):
    """Legacy Pydantic implementation of SemanticVersion. In the process of deprecation."""

    @override
    def _implements_protocol(self) -> TimeSpineTableConfiguration:
        return self

    location: str
    column_name: str
    grain: TimeGranularity
