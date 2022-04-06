from abc import ABC
from typing import Union

from metricflow.model.objects.utils import FrozenBaseModel


class ColumnCorrelationKey(ABC, FrozenBaseModel):
    """Interface for a key object that is used to correlate columns between instance sets."""

    pass


class SingleColumnCorrelationKey(ColumnCorrelationKey):
    """Key to use when there's only 1 column association in an instance."""

    pass


class CompositeColumnCorrelationKey(ColumnCorrelationKey):
    """Key to use when there are multiple column associations in an instance"""

    sub_identifier: str


class ColumnAssociation(FrozenBaseModel):
    """Used to describe how an instance is associated with columns in table / SQL query.

    Generally there will be a 1:1 mapping, but for composite identifiers, it can map to multiple columns. For that case,
    this can be subclassed to add more context.
    """

    column_name: str
    # When an instance is passed from one dataflow node to another, we need to know how the columns from the input
    # corresponds to the columns from the output. Equality of this key is used to determine that relationship.
    # This could be made to be in a dictionary instead, but having it here means that it doesn't need to be hashable.
    column_correlation_key: Union[CompositeColumnCorrelationKey, SingleColumnCorrelationKey]
