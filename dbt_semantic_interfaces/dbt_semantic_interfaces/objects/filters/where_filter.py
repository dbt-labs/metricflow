from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)

TransformOutputT = TypeVar("TransformOutputT")


class WhereFilterTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming WhereFilters."""

    @abstractmethod
    def transform(self, where_filter: WhereFilter) -> TransformOutputT:  # noqa: D
        raise NotImplementedError


class WhereFilter(PydanticCustomInputParser, HashableBaseModel):
    """A filter applied to the data set containing measures, dimensions, identifiers relevant to the query.

    TODO: Clarify whether the filter applies to aggregated or un-aggregated data sets.

    The data set will contain dimensions as required by the query and the dimensions that a referenced in any of the
    filters that are used in the definition of metrics.
    """

    where_sql_template: str

    @classmethod
    def _from_yaml_value(
        cls,
        input: PydanticParseableValueType,
    ) -> WhereFilter:
        """Parses a WhereFilter from a string found in a user-provided model specification.

        User-provided constraint strings are SQL snippets conforming to the expectations of SQL WHERE clauses,
        and as such we parse them using our standard parse method below.
        """
        if isinstance(input, str):
            return WhereFilter(where_sql_template=input)
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")

    def transform(self, where_filter_transform: WhereFilterTransform[TransformOutputT]) -> TransformOutputT:  # noqa: D
        return where_filter_transform.transform(self)
