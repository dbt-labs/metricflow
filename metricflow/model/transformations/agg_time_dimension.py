import logging
from typing import Optional

from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule
from metricflow.references import TimeDimensionReference

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ModelTransformRule):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @staticmethod
    def _find_primary_time_dimension(data_source: DataSource) -> Optional[TimeDimensionReference]:
        for dimension in data_source.dimensions:
            if dimension.type == DimensionType.TIME and dimension.type_params and dimension.type_params.is_primary:
                return dimension.time_dimension_reference
        return None

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            primary_time_dimension_reference = SetMeasureAggregationTimeDimensionRule._find_primary_time_dimension(
                data_source
            )

            if not primary_time_dimension_reference:
                # Dimension data sources won't have a primary time dimension.
                continue

            for measure in data_source.measures:
                if not measure.agg_time_dimension:
                    measure.agg_time_dimension = primary_time_dimension_reference.element_name

        return model
