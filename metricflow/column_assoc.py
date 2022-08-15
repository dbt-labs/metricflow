from abc import ABC
from dataclasses import dataclass
from typing import Optional, Any

from metricflow.dataclass_serialization import SerializableDataclass
from metricflow.object_utils import assert_exactly_one_arg_set


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
class CompositeColumnCorrelationKey(ColumnCorrelationKey, SerializableDataclass):
    """Key to use when there are multiple column associations in an instance"""

    sub_identifier: str


@dataclass(frozen=True)
class ColumnAssociation(SerializableDataclass):
    """Used to describe how an instance is associated with columns in table / SQL query.

    Generally there will be a 1:1 mapping, but for composite identifiers, it can map to multiple columns. For that case,
    this can be subclassed to add more context.
    """

    column_name: str
    # When an instance is passed from one dataflow node to another, we need to know how the columns from the input
    # corresponds to the columns from the output. Equality of this key is used to determine that relationship.
    # This could be made to be in a dictionary instead, but having it here means that it doesn't need to be hashable.
    single_column_correlation_key: Optional[SingleColumnCorrelationKey] = None
    composite_column_correlation_key: Optional[CompositeColumnCorrelationKey] = None

    def __post_init__(self) -> None:  # noqa: D
        assert_exactly_one_arg_set(
            single_column_correlation_key=self.single_column_correlation_key,
            composite_column_correlation_key=self.composite_column_correlation_key,
        )

    @property
    def column_correlation_key(self) -> ColumnCorrelationKey:  # noqa: D
        if self.single_column_correlation_key:
            return self.single_column_correlation_key
        elif self.composite_column_correlation_key:
            return self.composite_column_correlation_key

        raise RuntimeError("single_column_correlation_key or composite_column_correlation_key should be set")
