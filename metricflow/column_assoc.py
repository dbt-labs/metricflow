from abc import ABC
from dataclasses import dataclass
from typing import Any

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass


class ColumnCorrelationKey(ABC):
    """Interface for a key object that is used to correlate columns between instance sets."""

    pass


@dataclass(frozen=True)
class SingleColumnCorrelationKey(ColumnCorrelationKey, SerializableDataclass):
    """Key to use when there's only 1 column association in an instance."""

    # Pydantic throws an error during serialization if a dataclass has no fields.
    __PYDANTIC_BUG_WORKAROUND: bool = True

    def __eq__(self, other: Any) -> bool:  # type: ignore[misc] # noqa: D
        return isinstance(other, SingleColumnCorrelationKey)

    def __hash__(self) -> int:  # noqa: D
        return hash(self.__class__.__name__)


@dataclass(frozen=True)
class ColumnAssociation(SerializableDataclass):
    """Used to describe how an instance is associated with columns in table / SQL query."""

    column_name: str
    # When an instance is passed from one dataflow node to another, we need to know how the columns from the input
    # corresponds to the columns from the output. Equality of this key is used to determine that relationship.
    # This could be made to be in a dictionary instead, but having it here means that it doesn't need to be hashable.
    single_column_correlation_key: SingleColumnCorrelationKey

    @property
    def column_correlation_key(self) -> ColumnCorrelationKey:  # noqa: D
        return self.single_column_correlation_key
