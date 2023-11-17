from __future__ import annotations

import logging
from typing import Optional

import jinja2
from dbt_semantic_interfaces.call_parameter_sets import ParseWhereFilterException
from dbt_semantic_interfaces.protocols import WhereFilterIntersection
from dbt_semantic_interfaces.protocols.where_filter import WhereFilter

from metricflow.filters.merge_where import merge_to_single_where_filter
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.specs import LinkableSpecSet, WhereFilterSpec
from metricflow.specs.where_filter_dimension import WhereFilterDimensionFactory
from metricflow.specs.where_filter_entity import WhereFilterEntityFactory
from metricflow.specs.where_filter_time_dimension import WhereFilterTimeDimensionFactory
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

    def create_from_where_filter_intersection(  # noqa: D
        self, where_filter_intersection: Optional[WhereFilterIntersection]
    ) -> Optional[WhereFilterSpec]:
        if where_filter_intersection is None:
            return None

        where_filter = merge_to_single_where_filter(where_filter_intersection)
        if where_filter is None:
            return None

        return self.create_from_where_filter(where_filter)

    def _render_sql_template(
        self,
        where_filter: WhereFilter,
        dimension_factory: WhereFilterDimensionFactory,
        time_dimension_factory: WhereFilterTimeDimensionFactory,
        entity_factory: WhereFilterEntityFactory,
    ) -> str:
        try:
            return jinja2.Template(where_filter.where_sql_template, undefined=jinja2.StrictUndefined).render(
                {
                    "Dimension": dimension_factory.create,
                    "TimeDimension": time_dimension_factory.create,
                    "Entity": entity_factory.create,
                }
            )
        except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
            raise RenderSqlTemplateException(
                f"Error while rendering Jinja template:\n{where_filter.where_sql_template}"
            ) from e

    def create_from_where_filter(self, where_filter: WhereFilter) -> WhereFilterSpec:
        """Generates WhereFilterSpec using Jinja."""
        try:
            call_parameter_sets = where_filter.call_parameter_sets

            dimension_factory = WhereFilterDimensionFactory(call_parameter_sets, self._column_association_resolver)
            time_dimension_factory = WhereFilterTimeDimensionFactory(
                call_parameter_sets, self._column_association_resolver
            )
            entity_factory = WhereFilterEntityFactory(call_parameter_sets, self._column_association_resolver)
            rendered_sql_template = self._render_sql_template(
                where_filter, dimension_factory, time_dimension_factory, entity_factory
            )

            """
            Dimensions that are created with a grain or date_part parameter, Dimension(...).grain(...), are
            added to time_dimension_factory.time_dimension_specs otherwise they are add to dimension_specs
            """
            dimension_specs = []
            for dimension in dimension_factory.created:
                if dimension.time_granularity_name or dimension.date_part_name:
                    time_dimension_factory.time_dimension_specs.append(dimension.time_dimension_spec)
                else:
                    dimension_specs.append(dimension.dimension_spec)

            return WhereFilterSpec(
                where_sql=rendered_sql_template,
                bind_parameters=self._bind_parameters,
                linkable_spec_set=LinkableSpecSet(
                    dimension_specs=tuple(dimension_specs),
                    time_dimension_specs=tuple(time_dimension_factory.time_dimension_specs),
                    entity_specs=tuple(entity_factory.entity_specs),
                ),
            )
        except (ParseWhereFilterException, TypeError) as e:
            raise InvalidQueryException(
                f"Error parsing the where filter: {where_filter.where_sql_template}. {e}"
            ) from e
