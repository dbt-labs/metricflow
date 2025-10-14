from __future__ import annotations

from abc import ABC, abstractmethod
from textwrap import indent
from typing import List, Sequence

from jinja2 import StrictUndefined, TemplateSyntaxError, UndefinedError
from jinja2.exceptions import SecurityError
from jinja2.sandbox import SandboxedEnvironment
from typing_extensions import override

from metricflow_semantic_interfaces.errors import InvalidQuerySyntax
from metricflow_semantic_interfaces.parsing.text_input.rendering_helper import (
    ObjectBuilderJinjaRenderHelper,
)
from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderItemDescription,
)
from metricflow_semantic_interfaces.parsing.text_input.ti_exceptions import (
    QueryItemJinjaException,
)
from metricflow_semantic_interfaces.parsing.text_input.valid_method import ValidMethodMapping


class ObjectBuilderTextProcessor:
    """Performs processing actions for text containing query items specified in the object-builder syntax.

    This currently supports:
    * Collecting `ObjectBuilderItemDescription`s from a Jinja template.
    * Rendering a Jinja template using a specified renderer.
    """

    def get_description(
        self, query_item_input: str, valid_method_mapping: ValidMethodMapping
    ) -> ObjectBuilderItemDescription:
        """Get the `ObjectBuilderItemDescription` for a single item.

        e.g. `Dimension('listing__country').descending(True)`.
        """
        descriptions = self.collect_descriptions_from_template(
            jinja_template="{{ " + query_item_input + " }}",
            valid_method_mapping=valid_method_mapping,
        )
        if len(descriptions) != 1:
            raise InvalidQuerySyntax(
                f"Did not get exactly one query item from: {query_item_input!r} Got: {descriptions}"
            )
        return descriptions[0]

    def collect_descriptions_from_template(
        self,
        jinja_template: str,
        valid_method_mapping: ValidMethodMapping,
    ) -> Sequence[ObjectBuilderItemDescription]:
        """Returns the `ObjectBuilderItemDescription`s that are found in a Jinja template.

        Args:
            jinja_template: A Jinja-template string like `{{ Dimension('listing__country') }} = 'US'`.
            valid_method_mapping: Mapping from the builder object to the valid methods. See
            `ConfiguredValidMethodMapping`.

        Returns:
            A sequence of the descriptions found in the template.

        Raises:
            QueryItemJinjaException: See definition.
            InvalidBuilderMethodException: See definition.
        """
        description_collector = _CollectDescriptionProcessor()
        self._process_template(
            jinja_template=jinja_template,
            valid_method_mapping=valid_method_mapping,
            description_processor=description_collector,
        )
        return description_collector.collected_descriptions()

    def _process_template(
        self,
        jinja_template: str,
        valid_method_mapping: ValidMethodMapping,
        description_processor: ObjectBuilderItemDescriptionProcessor,
    ) -> str:
        """Helper to run a `ObjectBuilderItemDescriptionProcessor` on a Jinja template."""
        render_helper = ObjectBuilderJinjaRenderHelper(
            description_processor=description_processor,
            valid_method_mapping=valid_method_mapping,
        )
        try:
            # the string that the sandbox renders is unused
            rendered = (
                SandboxedEnvironment(undefined=StrictUndefined)
                .from_string(jinja_template)
                .render(
                    Dimension=render_helper.get_function_for_dimension(),
                    TimeDimension=render_helper.get_function_for_time_dimension(),
                    Entity=render_helper.get_function_for_entity(),
                    Metric=render_helper.get_function_for_metric(),
                )
            )
        except (UndefinedError, TemplateSyntaxError, SecurityError) as e:
            raise QueryItemJinjaException(
                f"Error while processing Jinja template:" f"\n{indent(jinja_template, prefix='    ')}"
            ) from e

        return rendered


class ObjectBuilderItemDescriptionProcessor(ABC):
    """General processor that does something to a query-item description seen in a Jinja template."""

    @abstractmethod
    def process_description(self, item_description: ObjectBuilderItemDescription) -> str:
        """Process the given description, and return a string that would be substituted into the Jinja template."""
        raise NotImplementedError


class _CollectDescriptionProcessor(ObjectBuilderItemDescriptionProcessor):
    """Processor that collects all descriptions that were processed."""

    def __init__(self) -> None:  # noqa: D107
        self._items: List[ObjectBuilderItemDescription] = []

    def collected_descriptions(self) -> Sequence[ObjectBuilderItemDescription]:
        """Return all descriptions that were processed so far."""
        return self._items

    @override
    def process_description(self, item_description: ObjectBuilderItemDescription) -> str:
        if item_description not in self._items:
            self._items.append(item_description)

        return ""
