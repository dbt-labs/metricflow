from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import cached_property

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, TimeGranularity
from typing_extensions import override

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_non_none_or_raise
from metricflow_semantics.experimental.dsi.entity_lookup import EntityLookup
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.mf_logging.attribute_pretty_format import AttributeMapping, AttributePrettyFormattable
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class ModelObjectLookup(AttributePrettyFormattable):
    """Similar to `ManifestObjectLookup` but for objects in a `SemanticModel`."""

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D107
        self._semantic_model = semantic_model

    @cached_property
    def semantic_model(self) -> SemanticModel:  # noqa: D102
        return self._semantic_model

    @cached_property
    def model_id(self) -> SemanticModelId:  # noqa: D102
        return SemanticModelId.get_instance(model_name=self._semantic_model.name)

    @cached_property
    def time_dimension_name_to_grain(self) -> Mapping[str, TimeGranularity]:  # noqa: D102
        time_dimension_name_to_grain: dict[str, TimeGranularity] = {}
        for dimension in self._semantic_model.dimensions:
            # Skip non-time dimensions.
            if dimension.type is DimensionType.TIME:
                pass
            elif dimension.type is DimensionType.CATEGORICAL:
                continue
            else:
                assert_values_exhausted(dimension.type)

            type_params = mf_first_non_none_or_raise(
                dimension.type_params,
                error_supplier=lambda: InvalidManifestException(
                    LazyFormat(
                        "`type_params` should not be `None` for a time dimension.",
                        dimension=dimension,
                        semantic_model=self._semantic_model,
                    )
                ),
            )
            time_dimension_name_to_grain[dimension.name] = type_params.time_granularity
        return time_dimension_name_to_grain

    @cached_property
    def entity_lookup(self) -> EntityLookup:  # noqa: D102
        return EntityLookup(self._semantic_model.entities)

    @cached_property
    @override
    def _attribute_mapping(self) -> AttributeMapping:
        return dict(
            **super()._attribute_mapping,
            **{
                attribute_name: getattr(self, attribute_name)
                for attribute_name in (
                    "model_id",
                    "time_dimension_name_to_grain",
                    "entity_lookup",
                )
            },
        )
