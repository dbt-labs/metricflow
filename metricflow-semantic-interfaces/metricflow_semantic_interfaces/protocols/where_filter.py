from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, Sequence, Tuple

from metricflow_semantic_interfaces.call_parameter_sets import JinjaCallParameterSets


class WhereFilter(Protocol):
    """A filter that is applied using a WHERE filter in the generated SQL."""

    @property
    @abstractmethod
    def where_sql_template(self) -> str:
        """A template that describes how to render the SQL for a WHERE clause."""
        pass

    @abstractmethod
    def call_parameter_sets(self, custom_granularity_names: Sequence[str]) -> JinjaCallParameterSets:
        """Describe calls like 'dimension(...)' in the SQL template."""
        pass


class WhereFilterIntersection(Protocol):
    """A collection of filters to be applied to an input dataset.

    This is an intersection, meaning each input row must pass all filters to be included in the output. It is the
    equivalent of using an " AND " expression to join each filter expression in the input set into a single SQL
    statement.

    Although there is no formal contract around this, the expectation is these filters will be applied in a manner
    that will produce output equivalent to running the WHERE clause, after dimensional joins but before measure
    aggregations.

    We use a protocol class here, instead of a simple Sequence, partly to centralize any custom parsing and processing
    logic and partly because it is more descriptive as to the relationship between the filter elements in the set.
    """

    @property
    @abstractmethod
    def where_filters(self) -> Sequence[WhereFilter]:
        """The collection of WhereFilters to be applied to the input data set."""
        pass

    @abstractmethod
    def filter_expression_parameter_sets(
        self, custom_granularity_names: Sequence[str]
    ) -> Sequence[Tuple[str, JinjaCallParameterSets]]:
        """Mapping from distinct filter expressions to the call parameter sets associated with them.

        We use a tuple, rather than a Mapping, in case the call parameter sets may vary between
        filter expression specifications.
        """
        pass
