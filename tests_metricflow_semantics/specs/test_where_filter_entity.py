from __future__ import annotations

import pytest
from metricflow_semantics.errors.error_classes import InvalidQuerySyntax
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.where_filter.where_filter_entity import WhereFilterEntity

from tests_metricflow_semantics.specs.conftest import EXAMPLE_FILTER_LOCATION


def test_descending_cannot_be_set(  # noqa
    column_association_resolver: ColumnAssociationResolver,
) -> None:
    with pytest.raises(InvalidQuerySyntax):
        WhereFilterEntity(
            column_association_resolver=column_association_resolver,
            resolved_spec_lookup=FilterSpecResolutionLookUp.empty_instance(),
            where_filter_location=EXAMPLE_FILTER_LOCATION,
            rendered_spec_tracker=RenderedSpecTracker(),
            element_name="customer",
            entity_links=(),
        ).descending(True)
