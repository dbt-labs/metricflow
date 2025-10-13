from __future__ import annotations

from typing import FrozenSet, Mapping

from metricflow_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderMethod,
    QueryItemType,
)

ValidMethodMapping = Mapping[QueryItemType, FrozenSet[ObjectBuilderMethod]]


class ConfiguredValidMethodMapping:
    """Default mappings for methods valid for the object-builder syntax."""

    # In an order-by item, `.descending(...)` is allowed.
    DEFAULT_MAPPING_FOR_ORDER_BY: ValidMethodMapping = {
        QueryItemType.METRIC: frozenset({ObjectBuilderMethod.DESCENDING}),
        QueryItemType.ENTITY: frozenset({ObjectBuilderMethod.DESCENDING}),
        QueryItemType.DIMENSION: frozenset(
            {ObjectBuilderMethod.DESCENDING, ObjectBuilderMethod.GRAIN, ObjectBuilderMethod.DATE_PART}
        ),
        QueryItemType.TIME_DIMENSION: frozenset({ObjectBuilderMethod.DESCENDING}),
    }

    DEFAULT_MAPPING: ValidMethodMapping = {
        QueryItemType.METRIC: frozenset(),
        QueryItemType.ENTITY: frozenset(),
        QueryItemType.DIMENSION: frozenset(
            {ObjectBuilderMethod.DESCENDING, ObjectBuilderMethod.GRAIN, ObjectBuilderMethod.DATE_PART}
        ),
        QueryItemType.TIME_DIMENSION: frozenset(),
    }
