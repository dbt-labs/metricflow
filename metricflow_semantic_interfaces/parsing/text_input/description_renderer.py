from __future__ import annotations

from abc import ABC, abstractmethod

from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderItemDescription,
)


class QueryItemDescriptionRenderer(ABC):
    """Defines how query items specified via object-builder syntax in a Jinja template should be rendered.

    e.g. For a Jinja template in a where-filter:

        {{ Dimension('listing__country') }} = 'US'
        AND {{ TimeDimension('metric_time') }} > '2020-01-01'

    a particular implementation might be used to render it to:

        listing__count = 'US'
        AND metric_time__day > '2020-01-01'
    """

    @abstractmethod
    def render_description(self, item_description: ObjectBuilderItemDescription) -> str:
        """Return the string that will be substituted for the query item in the Jinja template."""
        raise NotImplementedError
