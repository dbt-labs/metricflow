import logging
from typing import Optional

from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import DimensionType
from dbt.semantic.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule
from dbt.semantic.references import TimeDimensionReference

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ModelTransformRule):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @staticmethod
    def _find_primary_time_dimension(entity: Entity) -> Optional[TimeDimensionReference]:
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
                    measure.agg_time_dimension = primary_time_dimension_reference.name

        return model
