from __future__ import annotations

import logging
from typing import List, Optional, Sequence

import jinja2
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection

from metricflow.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import FilterSpecResolutionLookUp
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.rendered_spec_tracker import RenderedSpecTracker
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
        spec_resolution_lookup: FilterSpecResolutionLookUp,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._spec_resolution_lookup = spec_resolution_lookup

    def create_from_where_filter(  # noqa: D
        self,
        filter_location: WhereFilterLocation,
        where_filter: WhereFilter,
    ) -> WhereFilterSpec:
        return self.create_from_where_filter_intersection(
            filter_location=filter_location,
            filter_intersection=PydanticWhereFilterIntersection(where_filters=[where_filter]),
        )[0]

    def create_from_where_filter_intersection(  # noqa: D
        self,
        filter_location: WhereFilterLocation,
        filter_intersection: Optional[WhereFilterIntersection],
    ) -> Sequence[WhereFilterSpec]:
        if filter_intersection is None:
            return ()

        filter_specs: List[WhereFilterSpec] = []

        for where_filter in filter_intersection.where_filters:
            rendered_spec_tracker = RenderedSpecTracker()
            dimension_factory = WhereFilterDimensionFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            time_dimension_factory = WhereFilterTimeDimensionFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            entity_factory = WhereFilterEntityFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            try:
                # If there was an error with the template, it should have been caught while resolving the specs for
                # the filters during query resolution.
                where_sql = jinja2.Template(where_filter.where_sql_template, undefined=jinja2.StrictUndefined).render(
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
            filter_specs.append(
                WhereFilterSpec(
                    where_sql=where_sql,
                    bind_parameters=SqlBindParameters(),
                    linkable_spec_set=LinkableSpecSet.from_specs(rendered_spec_tracker.rendered_specs),
                )
            )

        return filter_specs
