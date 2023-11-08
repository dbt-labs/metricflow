from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from typing_extensions import override

from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.protocols.query_parameter import InputOrderByParameter
from metricflow.protocols.query_parameter import SavedQueryParameter as SavedQueryParameterProtocol


@dataclass(frozen=True)
class TimeDimensionParameter:
    """Time dimension requested in a query."""

    name: str
    grain: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    def __post_init__(self) -> None:  # noqa: D
        parsed_name = StructuredLinkableSpecName.from_name(self.name)
        if parsed_name.time_granularity:
            raise ValueError("Must use object syntax for `grain` parameter if `date_part` is requested.")


@dataclass(frozen=True)
class DimensionOrEntityParameter:
    """Group by parameter requested in a query.

    Might represent an entity or a dimension.
    """

    name: str


@dataclass(frozen=True)
class MetricParameter:
    """Metric requested in a query."""

    name: str


@dataclass(frozen=True)
class OrderByParameter:
    """Order by requested in a query."""

    order_by: InputOrderByParameter
    descending: bool = False


@dataclass(frozen=True)
class SavedQueryParameter(ProtocolHint[SavedQueryParameterProtocol]):
    """Dataclass implementation of SavedQueryParameterProtocol."""

    name: str

    @override
    def _implements_protocol(self) -> SavedQueryParameterProtocol:
        return self
