from __future__ import annotations

from dataclasses import dataclass

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


@dataclass(frozen=True)
class ExpandedTimeGranularity(SerializableDataclass):
    """Dataclass container for custom granularity extensions to the base TimeGranularity enumeration.

    This includes the granularity name, which is either the custom granularity or the TimeGranularity string value,
    and an associated base time granularity value which we use as a pointer to the base grain used to look up the
    time spine. This will allow for some level of comparison between custom granularities.

    Note: this assumes that any base TimeGranularity value will derive the name from the TimeGranularity. It might be
    worth adding validation to ensure that is always the case, meaning that no `name` value can be a value in the
    TimeGranularity enumeration.
    """

    name: str
    base_granularity: TimeGranularity

    @property
    def is_custom_granularity(self) -> bool:  # noqa: D102
        return self.base_granularity.value != self.name

    @classmethod
    def from_time_granularity(cls, granularity: TimeGranularity) -> ExpandedTimeGranularity:
        """Factory method for creating an ExpandedTimeGranularity from a standard TimeGranularity enumeration value."""
        return ExpandedTimeGranularity(name=granularity.value, base_granularity=granularity)
