from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Sequence

import jinja2

from dbt_semantic_interfaces.references import DimensionReference, EntityReference
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


@dataclass(frozen=True)
class DimensionCallParameterSet:
    """When 'dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    dimension_reference: DimensionReference


@dataclass(frozen=True)
class TimeDimensionCallParameterSet:
    """When 'dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    time_dimension_reference: DimensionReference
    time_granularity: TimeGranularity


@dataclass(frozen=True)
class EntityCallParameterSet:
    """When 'entity(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    entity_reference: EntityReference


@dataclass(frozen=True)
class FilterCallParameterSets:
    """The calls for metric items made in the Jinja template of the where filter."""

    dimension_call_parameter_sets: Tuple[DimensionCallParameterSet, ...] = ()
    time_dimension_call_parameter_sets: Tuple[TimeDimensionCallParameterSet, ...] = ()
    entity_call_parameter_sets: Tuple[EntityCallParameterSet, ...] = ()


class FilterRenderingException(Exception):  # noqa: D
    pass


class FilterFunctionCallRenderer(ABC):
    """Interface for a closure to control how function calls in a template should be rendered.

    Using this class instead of just passing functions to simplify function signatures.
    """

    @abstractmethod
    def render_dimension_call(self, dimension_call_parameter_set: DimensionCallParameterSet) -> str:  # noqa: D
        raise NotImplementedError

    @abstractmethod
    def render_time_dimension_call(  # noqa: D
        self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def render_entity_call(self, entity_call_parameter_set: EntityCallParameterSet) -> str:  # noqa: D
        raise NotImplementedError


class FilterRenderer:
    """Renders the SQL template in the filter field of object definitions and queries.

    For example in the YAML configuration,

    ---
    metric:
      name: booking_value_for_some_states
      ...
      constraint: |
        "{{ dimension('home_state_latest', entity_path=['user']) }} IN ('CA', 'HI', 'WA')"

    the constraint field will need to be rendered into a form that can be used in SQL queries.

    "{{ dimension('home_state_latest', entity_path=['user']) }} IN ('CA', 'HI', 'WA')" should be rendered into SQL
    that can be used in a SQL query like "user__home_state_latest IN ('CA', 'HI', 'WA')"

    This class does not use the *spec classes as they are only available in the metricflow package.
    """

    # Names of the function calls used in the Jinja template.
    _DIMENSION_FUNCTION_NAME = "dimension"
    _TIME_DIMENSION_FUNCTION_NAME = "time_dimension"
    _IDENTIFIER_FUNCTION_NAME = "identifier"

    # To extract the parameters to the calls, we use a function to record the parameters while rendering the Jinja
    # template. The rendered result is not used, but since Jinja has to render something, using this as a placeholder.
    _DUMMY_PLACEHOLDER = "DUMMY_PLACEHOLDER"

    @staticmethod
    def extract_parameter_sets(templated_filter: str) -> FilterCallParameterSets:
        """Parse the filter and extract the metric object call parameters.

        An abbreviated example:

        "{{ dimension('home_state_latest', entity_path=['user']) }} IN ('CA', 'HI', 'WA')"

        ->

        FilterCallParameterSets(
            dimension_call_parameter_sets=(
                DimensionCallParameterSet(
                    entity_path=("user",),
                    dimension_reference="home_state_latest",
                ),
            )
            ...
        )

        """

        dimension_call_parameter_sets: List[DimensionCallParameterSet] = []
        time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []
        entity_call_parameter_sets: List[EntityCallParameterSet] = []

        class _DummyCallRenderer(FilterFunctionCallRenderer):
            def render_dimension_call(self, dimension_call_parameter_set: DimensionCallParameterSet) -> str:  # noqa: D
                dimension_call_parameter_sets.append(dimension_call_parameter_set)
                return FilterRenderer._DUMMY_PLACEHOLDER

            def render_time_dimension_call(  # noqa: D
                self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
            ) -> str:
                time_dimension_call_parameter_sets.append(time_dimension_call_parameter_set)
                return FilterRenderer._DUMMY_PLACEHOLDER

            def render_entity_call(self, entity_call_parameter_set: EntityCallParameterSet) -> str:  # noqa: D
                entity_call_parameter_sets.append(entity_call_parameter_set)
                return FilterRenderer._DUMMY_PLACEHOLDER

        FilterRenderer.render(
            templated_filter_sql=templated_filter,
            call_renderer=_DummyCallRenderer(),
        )

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
        )

    @staticmethod
    def render(
        templated_filter_sql: str,
        call_renderer: FilterFunctionCallRenderer,
    ) -> str:
        """Render the templated SQL according to the supplied functions. See class docstring for an example.

        Args:
            templated_filter_sql: the templated SQL to render
            call_renderer: Specifies how to render calls like dimension(...) in the template.

        Returns:
            The templated SQL rendered according to the supplied functions.
        """

        def _dimension_helper(dimension_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ dimension(...) }}"""
            return call_renderer.render_dimension_call(
                DimensionCallParameterSet(
                    dimension_reference=DimensionReference(element_name=dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )

        def _time_dimension_helper(
            time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
        ) -> str:
            """Gets called by Jinja when rendering {{ time_dimension(...) }}"""
            return call_renderer.render_time_dimension_call(
                TimeDimensionCallParameterSet(
                    time_dimension_reference=DimensionReference(element_name=time_dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    time_granularity=TimeGranularity(time_granularity_name),
                )
            )

        def _entity_helper(entity_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ entity(...) }}"""
            return call_renderer.render_entity_call(
                EntityCallParameterSet(
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    entity_reference=EntityReference(element_name=entity_name),
                )
            )

        try:
            return jinja2.Template(templated_filter_sql, undefined=jinja2.StrictUndefined).render(
                dimension=_dimension_helper,
                time_dimension=_time_dimension_helper,
                entity=_entity_helper,
            )
        except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
            raise FilterRenderingException(f"Error while parsing Jinja template:\n{templated_filter_sql}") from e
