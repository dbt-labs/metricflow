import logging
from typing import Optional

from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule
from dbt_semantic_interfaces.references import TimeDimensionReference

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ModelTransformRule):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @staticmethod
    def _find_primary_time_dimension(semantic_model: SemanticModel) -> Optional[TimeDimensionReference]:
        for dimension in semantic_model.dimensions:
            if dimension.type == DimensionType.TIME and dimension.type_params and dimension.type_params.is_primary:
                return dimension.time_dimension_reference
        return None

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            primary_time_dimension_reference = SetMeasureAggregationTimeDimensionRule._find_primary_time_dimension(
                semantic_model
            )

            if not primary_time_dimension_reference:
                # Dimension semantic models won't have a primary time dimension.
                continue

            for measure in semantic_model.measures:
                if not measure.agg_time_dimension:
                    measure.agg_time_dimension = primary_time_dimension_reference.element_name

        return model
