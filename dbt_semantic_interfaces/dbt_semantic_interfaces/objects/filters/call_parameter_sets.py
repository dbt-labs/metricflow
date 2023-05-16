from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Tuple

import jinja2

from dbt_semantic_interfaces.objects.filters.where_filter import (
    WhereFilter,
    WhereFilterTransform,
)
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)


@dataclass(frozen=True)
class DimensionCallParameterSet:
    """When 'dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    dimension_reference: DimensionReference


@dataclass(frozen=True)
class TimeDimensionCallParameterSet:
    """When 'time_dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    time_dimension_reference: TimeDimensionReference
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


class ParseWhereFilterException(Exception):  # noqa: D
    pass


class ParseToCallParameterSets(WhereFilterTransform[FilterCallParameterSets]):
    """Parses the where filter and returns the calls used in the template.

    An abbreviated example:

    WhereFilter("{{ dimension('home_state_latest', entity_path=['user']) }} IN ('CA', 'HI', 'WA')")

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

    # To extract the parameters to the calls, we use a function to record the parameters while rendering the Jinja
    # template. The rendered result is not used, but since Jinja has to render something, using this as a placeholder.
    _DUMMY_PLACEHOLDER = "DUMMY_PLACEHOLDER"

    def transform(self, where_filter: WhereFilter) -> FilterCallParameterSets:  # noqa: D
        dimension_call_parameter_sets: List[DimensionCallParameterSet] = []
        time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []
        entity_call_parameter_sets: List[EntityCallParameterSet] = []

        def _dimension_call(dimension_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ dimension(...) }}."""
            dimension_call_parameter_sets.append(
                DimensionCallParameterSet(
                    dimension_reference=DimensionReference(element_name=dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )
            return ParseToCallParameterSets._DUMMY_PLACEHOLDER

        def _time_dimension_call(
            time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
        ) -> str:
            """Gets called by Jinja when rendering {{ time_dimension(...) }}."""
            time_dimension_call_parameter_sets.append(
                TimeDimensionCallParameterSet(
                    time_dimension_reference=TimeDimensionReference(element_name=time_dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    time_granularity=TimeGranularity(time_granularity_name),
                )
            )
            return ParseToCallParameterSets._DUMMY_PLACEHOLDER

        def _entity_call(entity_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ entity(...) }}."""
            entity_call_parameter_sets.append(
                EntityCallParameterSet(
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    entity_reference=EntityReference(element_name=entity_name),
                )
            )
            return ParseToCallParameterSets._DUMMY_PLACEHOLDER

        try:
            jinja2.Template(where_filter.where_sql_template, undefined=jinja2.StrictUndefined).render(
                dimension=_dimension_call,
                time_dimension=_time_dimension_call,
                entity=_entity_call,
            )
        except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
            raise ParseWhereFilterException(
                f"Error while parsing Jinja template:\n{where_filter.where_sql_template}"
            ) from e

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
        )
