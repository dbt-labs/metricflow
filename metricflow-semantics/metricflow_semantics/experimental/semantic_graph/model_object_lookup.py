from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Optional

from dbt_semantic_interfaces.protocols import Dimension, Entity, Measure, SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType, TimeGranularity

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_non_none_or_raise
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException, MetricflowAssertionError
from metricflow_semantics.experimental.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class SemanticModelObjectLookup:
    """Similar to `ManifestObjectLookup` but for objects in a `SemanticModel`."""

    def __init__(self, semantic_model: SemanticModel) -> None:
        self._semantic_model = semantic_model

    @property
    def semantic_model(self) -> SemanticModel:
        return self._semantic_model

    @cached_property
    def _entity_name_to_entity(self) -> Mapping[str, Entity]:
        return {entity.name: entity for entity in self._semantic_model.entities}

    @cached_property
    def primary_entity_element(self) -> Optional[Entity]:
        primary_entity_name_to_entity = {
            entity_name: entity
            for entity_name, entity in self._entity_name_to_entity.items()
            if entity.type is EntityType.PRIMARY
        }
        if len(primary_entity_name_to_entity) == 0:
            return None
        elif len(primary_entity_name_to_entity) == 1:
            return tuple(primary_entity_name_to_entity.values())[0]
        else:
            raise InvalidManifestException(
                LazyFormat(
                    "More than 1 primary entity found",
                    primary_entity_name_to_entity=primary_entity_name_to_entity,
                    semantic_model=self._semantic_model,
                ),
            )

    @cached_property
    def model_id(self) -> SemanticModelId:
        return SemanticModelId(model_name=self._semantic_model.name)

    @cached_property
    def primary_entity_name(self) -> Optional[str]:
        primary_entity_field_value = self._semantic_model.primary_entity
        if primary_entity_field_value is not None:
            return primary_entity_field_value

        primary_entity_element = self.primary_entity_element
        if primary_entity_element is not None:
            return primary_entity_element.name

        return None


@singleton_dataclass()
class MeasureAggregationConfiguration:
    time_dimension_name: str
    time_grain: TimeGranularity

    @staticmethod
    def create(dimension: Dimension) -> MeasureAggregationConfiguration:
        if dimension.type_params is None:
            raise MetricflowAssertionError(
                LazyFormat("This should have been created with a valid time dimension", dimension=dimension)
            )
        return MeasureAggregationConfiguration(
            time_dimension_name=dimension.name,
            time_grain=dimension.type_params.time_granularity,
        )


class MeasureContainingModelObjectLookup(SemanticModelObjectLookup):
    def __init__(self, semantic_model: SemanticModel) -> None:
        if len(semantic_model.measures) == 0:
            raise MetricflowAssertionError(
                LazyFormat("This requires a semantic model containing measures", semantic_model=semantic_model)
            )

        super().__init__(semantic_model)

    @cached_property
    def aggregation_time_dimension_name_to_measures(self) -> Mapping[str, Sequence[Measure]]:
        current_aggregation_time_dimension_name_to_measures: dict[str, list[Measure]] = defaultdict(list)

        default = (
            self._semantic_model.defaults.agg_time_dimension if self._semantic_model.defaults is not None else None
        )
        for measure in self._semantic_model.measures:
            aggregation_time_dimension_name = mf_first_non_none_or_raise(
                measure.agg_time_dimension,
                default,
                error_supplier=lambda: InvalidManifestException(
                    LazyFormat(
                        "Missing aggregation time dimension", measure=measure, semantic_model=self._semantic_model
                    ),
                ),
            )
            current_aggregation_time_dimension_name_to_measures[aggregation_time_dimension_name].append(measure)

        return current_aggregation_time_dimension_name_to_measures

    # @cached_property
    # def primary_entity_name(self) -> str:
    #     primary_entity_field_value = self._semantic_model.primary_entity
    #     if primary_entity_field_value is not None:
    #         return primary_entity_field_value
    #
    #     primary_entity_element = self.primary_entity_element
    #     if primary_entity_element is None:
    #         raise InvalidManifestException(
    #             LazyFormat(
    #                 "A semantic model containing measures should have a primary entity but this does not",
    #                 semantic_mode=self._semantic_model,
    #             )
    #         )
    #
    #     return primary_entity_element.name

    @cached_property
    def _time_dimension_name_to_grain(self) -> Mapping[str, TimeGranularity]:
        return {
            dimension.name: dimension.type_params.time_granularity
            for dimension in self._semantic_model.dimensions
            if (dimension.type is DimensionType.TIME and dimension.type_params is not None)
        }

    @cached_property
    def aggregation_configuration_to_measures(self) -> Mapping[MeasureAggregationConfiguration, Sequence[Measure]]:
        default_aggregation_time_dimension = (
            self._semantic_model.defaults.agg_time_dimension if self._semantic_model.defaults is not None else None
        )
        current_aggregation_configuration_to_measures = defaultdict(list)

        for measure in self._semantic_model.measures:
            aggregation_time_dimension_name = mf_first_non_none_or_raise(
                measure.agg_time_dimension,
                default_aggregation_time_dimension,
                error_supplier=lambda: InvalidManifestException(
                    LazyFormat(
                        "Missing aggregation time dimension",
                        measure=measure,
                        default_aggregation_time_dimension=default_aggregation_time_dimension,
                        semantic_model=self._semantic_model,
                    )
                ),
            )

            aggregation_time_dimension_grain = mf_first_non_none_or_raise(
                self._time_dimension_name_to_grain.get(aggregation_time_dimension_name),
                error_supplier=lambda: InvalidManifestException(
                    LazyFormat(
                        "Missing aggregation time dimension grain",
                        aggregation_time_dimension_name=aggregation_time_dimension_name,
                        semantic_model=self._semantic_model,
                    )
                ),
            )

            aggregation_configuration = MeasureAggregationConfiguration(
                time_dimension_name=aggregation_time_dimension_name,
                time_grain=aggregation_time_dimension_grain,
            )
            current_aggregation_configuration_to_measures[aggregation_configuration].append(measure)

        return current_aggregation_configuration_to_measures
