from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property
from typing import Iterable

from dbt_semantic_interfaces.protocols import Dimension
from dbt_semantic_interfaces.type_enums import DimensionType
from typing_extensions import override

from metricflow_semantics.mf_logging.attribute_pretty_format import AttributeMapping, AttributePrettyFormattable

logger = logging.getLogger(__name__)


class DimensionLookup(AttributePrettyFormattable):
    """A lookup for entities within a semantic model."""

    def __init__(self, dimensions: Iterable[Dimension]) -> None:  # noqa: D107
        self._dimensions = tuple(dimensions)

    @cached_property
    def dimension_name_to_dimension(self) -> Mapping[str, Dimension]:  # noqa: D102
        return {dimension.name: dimension for dimension in self._dimensions}

    @cached_property
    def dimension_name_to_type(self) -> Mapping[str, DimensionType]:  # noqa: D102
        return {
            dimension_name: dimension.type for dimension_name, dimension in self.dimension_name_to_dimension.items()
        }

    @cached_property
    @override
    def _attribute_mapping(self) -> AttributeMapping:
        return dict(
            **super()._attribute_mapping,
            **{attribute_name: getattr(self, attribute_name) for attribute_name in ("dimension_name_to_type",)},
        )
