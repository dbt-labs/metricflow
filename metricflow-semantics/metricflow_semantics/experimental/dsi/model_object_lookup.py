from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Iterable, Optional

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import Entity, Measure, SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType, TimeGranularity

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_item, mf_first_non_none_or_raise
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException, MetricflowInternalError
from metricflow_semantics.experimental.ordered_set import FrozenOrderedSet, MutableOrderedSet
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
    def model_id(self) -> SemanticModelId:
        return SemanticModelId(model_name=self._semantic_model.name)

    @cached_property
    def primary_entity_name(self) -> Optional[str]:
        primary_entity_field_value = self._semantic_model.primary_entity
        if primary_entity_field_value is not None:
            return primary_entity_field_value

        primary_entity_names = self.entity_lookup.entity_type_to_names[EntityType.PRIMARY]
        matching_count = len(primary_entity_names)

        if matching_count == 0:
            return None
        elif matching_count == 1:
            primary_entity_element_name = mf_first_item(primary_entity_names)
            if primary_entity_field_value is not None and primary_entity_element_name != primary_entity_field_value:
                raise InvalidManifestException(
                    LazyFormat(
                        "The primary-entity-field value does not match the primary-entity element.",
                        primary_entity_field_value=primary_entity_field_value,
                        primary_entity_name=primary_entity_element_name,
                        model_id=self.model_id,
                    )
                )
            return primary_entity_element_name
        elif matching_count > 1:
            raise InvalidManifestException(
                LazyFormat(
                    "Found multiple primary-entity elements in a semantic model.",
                    primary_entity_names=primary_entity_names,
                    model_id=self.model_id,
                )
            )
        else:
            return None

    @cached_property
    def time_dimension_name_to_grain(self) -> Mapping[str, TimeGranularity]:
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
    def entity_lookup(self) -> EntityLookup:
        return EntityLookup(self._semantic_model.entities)


class EntityLookup:
    def __init__(self, entities: Iterable[Entity]) -> None:
        self._entity_name_to_entity: Mapping[str, Entity] = {entity.name: entity for entity in entities}

    @cached_property
    def entity_name_to_type(self) -> Mapping[str, EntityType]:
        return {entity_name: entity.type for entity_name, entity in self._entity_name_to_entity.items()}

    @cached_property
    def entity_type_to_names(self) -> Mapping[EntityType, FrozenOrderedSet[str]]:
        entity_type_to_names: dict[EntityType, MutableOrderedSet[str]] = defaultdict(MutableOrderedSet)
        for entity_name, entity_type in self.entity_name_to_type.items():
            entity_type_to_names[entity_type].add(entity_name)
        return {entity_type: entity_names.as_frozen() for entity_type, entity_names in entity_type_to_names.items()}


@singleton_dataclass()
class MeasureAggregationConfiguration:
    """Key that is used to group the measures in a semantic model by the associated aggregation time dimension."""

    time_dimension_name: str
    time_grain: TimeGranularity

    @staticmethod
    def get_instance(  # noqa: D102
        time_dimension_name: str, time_grain: TimeGranularity
    ) -> MeasureAggregationConfiguration:
        return MeasureAggregationConfiguration(time_dimension_name=time_dimension_name, time_grain=time_grain)


class MeasureContainingModelObjectLookup(SemanticModelObjectLookup):
    def __init__(self, semantic_model: SemanticModel) -> None:
        if len(semantic_model.measures) == 0:
            raise MetricflowInternalError(
                LazyFormat(
                    "This should have been called with a semantic model containing measures",
                    semantic_model=semantic_model,
                )
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

            aggregation_configuration = MeasureAggregationConfiguration.get_instance(
                time_dimension_name=aggregation_time_dimension_name,
                time_grain=aggregation_time_dimension_grain,
            )
            current_aggregation_configuration_to_measures[aggregation_configuration].append(measure)

        return current_aggregation_configuration_to_measures
