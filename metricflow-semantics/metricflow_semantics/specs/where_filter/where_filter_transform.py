from __future__ import annotations

import itertools
import logging
from typing import List, Optional, Sequence

import jinja2
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection

from metricflow_semantics.model.semantics.semantic_model_lookup import SemanticModelLookup
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.where_filter.where_filter_dimension import WhereFilterDimensionFactory
from metricflow_semantics.specs.where_filter.where_filter_entity import WhereFilterEntityFactory
from metricflow_semantics.specs.where_filter.where_filter_metric import WhereFilterMetricFactory
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_time_dimension import WhereFilterTimeDimensionFactory
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

logger = logging.getLogger(__name__)


class RenderSqlTemplateException(Exception):  # noqa: D101
    pass


class WhereSpecFactory:
    """Renders the SQL template in the WhereFilter and converts it to a WhereFilterSpec."""

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
        semantic_model_lookup: SemanticModelLookup,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._spec_resolution_lookup = spec_resolution_lookup
        self._semantic_model_lookup = semantic_model_lookup

    def create_from_where_filter(  # noqa: D102
        self,
        filter_location: WhereFilterLocation,
        where_filter: WhereFilter,
    ) -> WhereFilterSpec:
        return self.create_from_where_filter_intersection(
            filter_location=filter_location,
            filter_intersection=PydanticWhereFilterIntersection(where_filters=[where_filter]),
        )[0]

    def create_from_where_filter_intersection(  # noqa: D102
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
                custom_granularity_names=self._semantic_model_lookup.custom_granularity_names,
            )
            time_dimension_factory = WhereFilterTimeDimensionFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
                custom_granularity_names=self._semantic_model_lookup.custom_granularity_names,
            )
            entity_factory = WhereFilterEntityFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            metric_factory = WhereFilterMetricFactory(
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
                        "Metric": metric_factory.create,
                    }
                )
            except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
                raise RenderSqlTemplateException(
                    f"Error while rendering Jinja template:\n{where_filter.where_sql_template}"
                ) from e
            rendered_specs = tuple(result[0] for result in rendered_spec_tracker.rendered_specs_to_elements)
            linkable_elements = tuple(
                itertools.chain.from_iterable(result[1] for result in rendered_spec_tracker.rendered_specs_to_elements)
            )
            filter_specs.append(
                WhereFilterSpec(
                    where_sql=where_sql,
                    bind_parameters=SqlBindParameterSet(),
                    linkable_spec_set=LinkableSpecSet.create_from_specs(rendered_specs),
                    linkable_element_unions=tuple(linkable_element.as_union for linkable_element in linkable_elements),
                )
            )

        return filter_specs
