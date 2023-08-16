from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass

from metricflow.specs.specs import InstanceSpec


class ColumnCorrelationKey(ABC):
    """Interface for a key object that is used to correlate columns between instance sets."""

    pass


@dataclass(frozen=True)
class SingleColumnCorrelationKey(ColumnCorrelationKey, SerializableDataclass):
    """Key to use when there's only 1 column association in an instance."""

    # Pydantic throws an error during serialization if a dataclass has no fields.
    PYDANTIC_BUG_WORKAROUND: bool = True

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


class ColumnAssociationResolver(ABC):
    """Get the default column associations for an element instance.

    This is used for naming columns in an SQL query consistently. For example, dimensions with links are
    named like <entity link>__<dimension name> e.g. user_id__country, and time dimensions at a different time
    granularity are named <time dimension>__<time granularity> e.g. ds__month. Having a central place to name them will
    make it easier to change this later on. Names generated need to be unique within a query.

    It's also important to maintain this format because customers write constraints in SQL assuming this. This
    allows us to stick the constraint in as WHERE clauses without having to parse the constraint SQL.

    TODO: Updates are needed for time granularity in time dimensions, ToT for metrics.

    The resolve* methods should return the column associations / column names that it should use in queries for the given
    spec.
    """

    @abstractmethod
    def resolve_spec(self, spec: InstanceSpec) -> ColumnAssociation:  # noqa: D
        raise NotImplementedError
