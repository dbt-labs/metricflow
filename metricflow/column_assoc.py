from dataclasses import dataclass
from typing import Protocol, Optional

from metricflow.object_utils import assert_exactly_one_arg_set


class ColumnCorrelationKey(Protocol):
    """Interface for a key object that is used to correlate columns between instance sets."""

    pass


@dataclass(frozen=True)
class SingleColumnCorrelationKey(ColumnCorrelationKey):
    """Key to use when there's only 1 column association in an instance."""

    pass


@dataclass(frozen=True)
class CompositeColumnCorrelationKey(ColumnCorrelationKey):
    """Key to use when there are multiple column associations in an instance"""

    sub_identifier: str


@dataclass(frozen=True)
class ColumnAssociation:
    """Used to describe how an instance is associated with columns in table / SQL query.

    Generally there will be a 1:1 mapping, but for composite identifiers, it can map to multiple columns. For that case,
    this can be subclassed to add more context.
    """

    column_name: str
    # When an instance is passed from one dataflow node to another, we need to know how the columns from the input
    # corresponds to the columns from the output. Equality of this key is used to determine that relationship.
    # This could be made to be in a dictionary instead, but having it here means that it doesn't need to be hashable.
    single_column_correlation_key: Optional[SingleColumnCorrelationKey] = None
    composite_column_correlation_key: Optional[ColumnCorrelationKey] = None

    def __post_init__(self) -> None:  # noqa: D
        assert_exactly_one_arg_set(
            single_column_correlation_key=self.single_column_correlation_key,
            composite_column_correlation_key=self.composite_column_correlation_key,
        )

    def column_correlation_key(self) -> ColumnCorrelationKey:  # noqa: D
        result: ColumnCorrelationKey = self.single_column_correlation_key or self.composite_column_correlation_key
        assert result
        return result
