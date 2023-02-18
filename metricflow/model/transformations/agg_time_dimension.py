import logging
from typing import Optional

from metricflow.model.objects.conversions import MetricFlowMetricFlowEntity
from metricflow.model.objects.elements.dimension import DimensionType
from dbt.contracts.graph.manifest import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule
from metricflow.references import TimeDimensionReference

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ModelTransformRule):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @staticmethod
    def _find_primary_time_dimension(entity: MetricFlowEntity) -> Optional[TimeDimensionReference]:
        for dimension in entity.dimensions:
            if dimension.type == DimensionType.TIME and dimension.type_params and dimension.type_params.is_primary:
                return dimension.time_dimension_reference
        return None

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for entity in model.entities:
            primary_time_dimension_reference = SetMeasureAggregationTimeDimensionRule._find_primary_time_dimension(
                entity
            )

            if not primary_time_dimension_reference:
                # Dimension entities won't have a primary time dimension.
                continue

            for measure in entity.measures:
                if not measure.agg_time_dimension:
                    measure.agg_time_dimension = primary_time_dimension_reference.element_name

        return model
