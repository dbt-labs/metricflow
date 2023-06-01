from __future__ import annotations

import logging
from typing import Sequence

import jinja2
from dbt_semantic_interfaces.implementations.filters.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import (
    DimensionSpec,
    EntitySpec,
    LinkableSpecSet,
    TimeDimensionSpec,
    WhereFilterSpec,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)


class RenderSqlTemplateException(Exception):  # noqa: D
    pass


class WhereSpecFactory:
    """Renders the SQL template in the WhereFilter and converts it to a WhereFilterSpec."""

    def __init__(  # noqa: D
        self,
        column_association_resolver: ColumnAssociationResolver,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._bind_parameters = bind_parameters

    @staticmethod
    def _convert_to_dimension_spec(parameter_set: DimensionCallParameterSet) -> DimensionSpec:  # noqa: D
        return DimensionSpec(
            element_name=parameter_set.dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
        )

    @staticmethod
    def _convert_to_time_dimension_spec(parameter_set: TimeDimensionCallParameterSet) -> TimeDimensionSpec:  # noqa: D
        return TimeDimensionSpec(
            element_name=parameter_set.time_dimension_reference.element_name,
            entity_links=parameter_set.entity_path,
            time_granularity=parameter_set.time_granularity,
        )

    @staticmethod
    def _convert_to_entity_spec(parameter_set: EntityCallParameterSet) -> EntitySpec:  # noqa: D
        return EntitySpec(
            element_name=parameter_set.entity_reference.element_name,
            entity_links=parameter_set.entity_path,
        )

    def create_from_where_filter(self, where_filter: WhereFilter) -> WhereFilterSpec:  # noqa: D
        dimension_specs = []
        time_dimension_specs = []
        entity_specs = []

        def _dimension_call(dimension_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ dimension(...) }}."""
            dimension_spec = WhereSpecFactory._convert_to_dimension_spec(
                DimensionCallParameterSet(
                    dimension_reference=DimensionReference(element_name=dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )
            dimension_specs.append(dimension_spec)
            return self._column_association_resolver.resolve_spec(dimension_spec).column_name

        def _time_dimension_call(
            time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
        ) -> str:
            """Gets called by Jinja when rendering {{ time_dimension(...) }}."""
            time_dimension_spec = WhereSpecFactory._convert_to_time_dimension_spec(
                TimeDimensionCallParameterSet(
                    time_dimension_reference=TimeDimensionReference(element_name=time_dimension_name),
                    time_granularity=TimeGranularity(time_granularity_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )
            time_dimension_specs.append(time_dimension_spec)
            return self._column_association_resolver.resolve_spec(time_dimension_spec).column_name

        def _entity_call(entity_name: str, entity_path: Sequence[str] = ()) -> str:
            entity_spec = WhereSpecFactory._convert_to_entity_spec(
                EntityCallParameterSet(
                    entity_reference=EntityReference(element_name=entity_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )

            entity_specs.append(entity_spec)
            """Gets called by Jinja when rendering {{ entity(...) }}"""
            return self._column_association_resolver.resolve_spec(entity_spec).column_name

        try:
            rendered_sql_template = jinja2.Template(
                where_filter.where_sql_template, undefined=jinja2.StrictUndefined
            ).render(
                dimension=_dimension_call,
                time_dimension=_time_dimension_call,
                entity=_entity_call,
            )
        except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
            raise RenderSqlTemplateException(
                f"Error while rendering Jinja template:\n{where_filter.where_sql_template}"
            ) from e

        return WhereFilterSpec(
            where_sql=rendered_sql_template,
            bind_parameters=self._bind_parameters,
            linkable_spec_set=LinkableSpecSet(
                dimension_specs=tuple(dimension_specs),
                time_dimension_specs=tuple(time_dimension_specs),
                entity_specs=tuple(entity_specs),
            ),
        )
