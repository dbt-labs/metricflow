from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Mapping, Sequence

from dbt_semantic_interfaces.protocols import Measure, SemanticModel
from dbt_semantic_interfaces.type_enums import DimensionType, TimeGranularity

from metricflow_semantics.collection_helpers.syntactic_sugar import mf_first_non_none_or_raise
from metricflow_semantics.experimental.dsi.model_object_lookup import ModelObjectLookup
from metricflow_semantics.experimental.metricflow_exception import InvalidManifestException, MetricflowInternalError
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


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


class MeasureContainingModelObjectLookup(ModelObjectLookup):
    def __init__(self, semantic_model: SemanticModel) -> None:
        if len(semantic_model.measures) == 0:
            raise MetricflowInternalError(
                LazyFormat(
                    "This should have been created with a semantic model containing measures",
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

    @cached_property
    def _time_dimension_name_to_grain(self) -> Mapping[str, TimeGranularity]:
        return {
            dimension.name: dimension.type_params.time_granularity
            for dimension in self._semantic_model.dimensions
            if (dimension.type is DimensionType.TIME and dimension.type_params is not None)
        }
