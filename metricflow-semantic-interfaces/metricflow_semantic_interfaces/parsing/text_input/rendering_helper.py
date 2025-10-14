from __future__ import annotations

import typing
from typing import Callable, FrozenSet, Optional, Sequence

from typing_extensions import override

from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderItemDescription,
    ObjectBuilderMethod,
    QueryItemType,
)
from metricflow_semantic_interfaces.parsing.text_input.ti_exceptions import (
    InvalidBuilderMethodException,
)

if typing.TYPE_CHECKING:
    from metricflow_semantic_interfaces.parsing.text_input.ti_processor import (
        ObjectBuilderItemDescriptionProcessor,
    )

from metricflow_semantic_interfaces.parsing.text_input.valid_method import ValidMethodMapping


class ObjectBuilderJinjaRenderHelper:
    """Helps to build the methods that go into the Jinja template `.render()` call.

    e.g.

        SandboxedEnvironment(undefined=StrictUndefined)
            .from_string(jinja_template)
            .render(
                Dimension=render_helper.get_function_for_dimension(),
                TimeDimension=render_helper.get_function_for_time_dimension(),
                Entity=render_helper.get_function_for_entity(),
                Metric=render_helper.get_function_for_metric(),
            )
        )
    """

    def __init__(  # noqa: D107
        self,
        description_processor: ObjectBuilderItemDescriptionProcessor,
        valid_method_mapping: ValidMethodMapping,
    ) -> None:
        self._description_processor = description_processor
        self._valid_method_mapping = valid_method_mapping

    def get_function_for_dimension(self) -> Callable:
        """Returns the function that should be passed in to `.render(Dimension=...)`."""
        description_processor = self._description_processor
        item_type = QueryItemType.DIMENSION
        allowed_methods = self._valid_method_mapping[item_type]

        def _create(name: str, entity_path: Sequence[str] = ()) -> _RenderingClassForJinjaTemplate:
            return _RenderingClassForJinjaTemplate(
                description_processor=description_processor,
                allowed_methods=allowed_methods,
                initial_item_description=ObjectBuilderItemDescription(
                    item_type=item_type,
                    item_name=name,
                    entity_path=tuple(entity_path),
                    time_granularity_name=None,
                    date_part_name=None,
                    group_by_for_metric_item=(),
                    descending=None,
                ),
            )

        return _create

    def get_function_for_time_dimension(self) -> Callable:
        """Returns the function that should be passed in to `.render(TimeDimension=...)`."""
        description_processor = self._description_processor
        item_type = QueryItemType.TIME_DIMENSION
        allowed_methods = self._valid_method_mapping[item_type]

        def _create(
            time_dimension_name: str,
            time_granularity_name: Optional[str] = None,
            entity_path: Sequence[str] = (),
            descending: Optional[bool] = None,
            date_part_name: Optional[str] = None,
        ) -> _RenderingClassForJinjaTemplate:
            return _RenderingClassForJinjaTemplate(
                description_processor=description_processor,
                allowed_methods=allowed_methods,
                initial_item_description=ObjectBuilderItemDescription(
                    item_type=item_type,
                    item_name=time_dimension_name,
                    entity_path=tuple(entity_path),
                    time_granularity_name=time_granularity_name,
                    date_part_name=date_part_name,
                    group_by_for_metric_item=(),
                    descending=descending,
                ),
            )

        return _create

    def get_function_for_entity(self) -> Callable:
        """Returns the function that should be passed in to `.render(Entity=...)`."""
        description_processor = self._description_processor
        item_type = QueryItemType.ENTITY
        allowed_methods = self._valid_method_mapping[item_type]

        def _create(entity_name: str, entity_path: Sequence[str] = ()) -> _RenderingClassForJinjaTemplate:
            return _RenderingClassForJinjaTemplate(
                description_processor=description_processor,
                allowed_methods=allowed_methods,
                initial_item_description=ObjectBuilderItemDescription(
                    item_type=item_type,
                    item_name=entity_name,
                    entity_path=tuple(entity_path),
                    time_granularity_name=None,
                    date_part_name=None,
                    group_by_for_metric_item=(),
                    descending=None,
                ),
            )

        return _create

    def get_function_for_metric(self) -> Callable:
        """Returns the function that should be passed in to `.render(Metric=...)`."""
        description_processor = self._description_processor
        item_type = QueryItemType.METRIC
        allowed_methods = self._valid_method_mapping[item_type]

        def _create(metric_name: str, group_by: Sequence[str] = ()) -> _RenderingClassForJinjaTemplate:
            return _RenderingClassForJinjaTemplate(
                description_processor=description_processor,
                allowed_methods=allowed_methods,
                initial_item_description=ObjectBuilderItemDescription(
                    item_type=item_type,
                    item_name=metric_name,
                    entity_path=(),
                    time_granularity_name=None,
                    date_part_name=None,
                    group_by_for_metric_item=tuple(group_by),
                    descending=None,
                ),
            )

        return _create


class _RenderingClassForJinjaTemplate:
    """Helper class that behaves like a builder object as used in a Jinja template.

    e.g. in the Jinja template:

        {{ Dimension('listing__created_at').grain('day').date_part('month') }}

    The `Dimension('listing__created_at')` is an instance of this class and when builder methods like `.grain()` are
    called on it, the state of the instance is updated and returns itself so that additional builder methods can be
    chained.
    """

    def __init__(
        self,
        description_processor: ObjectBuilderItemDescriptionProcessor,
        allowed_methods: FrozenSet[ObjectBuilderMethod],
        initial_item_description: ObjectBuilderItemDescription,
    ) -> None:
        """Initializer.

        Args:
            description_processor: The description processor that will run using the query-item description described
            in the builder call. It will run after all builder methods are called.
            allowed_methods: Builder methods that can be used. Otherwise, an `InvalidBuilderMethodException` is raised.
            initial_item_description: The starting description. Usually it contains the element name and entity path.
        """
        self._description_processor = description_processor
        self._allowed_builder_methods = allowed_methods
        self._current_description = initial_item_description

    def _update_current_description(
        self,
        builder_method: ObjectBuilderMethod,
        new_description: ObjectBuilderItemDescription,
    ) -> None:
        if builder_method not in self._allowed_builder_methods:
            raise InvalidBuilderMethodException(
                f"`{builder_method.value}` can't be used with `{self._current_description.item_type.value}`"
                f" in this context.",
                item_type=self._current_description.item_type,
                invalid_builder_method=builder_method,
            )
        self._current_description = new_description

    def grain(self, time_granularity: str) -> _RenderingClassForJinjaTemplate:
        self._update_current_description(
            builder_method=ObjectBuilderMethod.GRAIN,
            new_description=self._current_description.create_modified(time_granularity_name=time_granularity),
        )
        return self

    def descending(self, _is_descending: bool) -> _RenderingClassForJinjaTemplate:
        self._update_current_description(
            builder_method=ObjectBuilderMethod.DESCENDING,
            new_description=self._current_description.create_modified(descending=_is_descending),
        )
        return self

    def date_part(self, date_part_name: str) -> _RenderingClassForJinjaTemplate:
        self._update_current_description(
            builder_method=ObjectBuilderMethod.DATE_PART,
            new_description=self._current_description.create_modified(date_part_name=date_part_name),
        )
        return self

    @override
    def __str__(self) -> str:
        return self._description_processor.process_description(self._current_description)
