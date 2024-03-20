from __future__ import annotations

import pytest

from metricflow.errors.errors import InvalidQuerySyntax
from metricflow.query.group_by_item.filter_spec_resolution.filter_spec_lookup import FilterSpecResolutionLookUp
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow.specs.where_filter_entity import WhereFilterEntity
from metricflow.test.specs.conftest import EXAMPLE_FILTER_LOCATION


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
