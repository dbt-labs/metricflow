import logging

from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class BooleanMeasureAggregationRule(ModelTransformRule):
    """Converts the expression used in boolean measures so that it can be aggregated."""

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.BOOLEAN:
                    logger.warning(
                        f"In semantic model {semantic_model.name}, measure `{measure.reference.element_name}` "
                        f"is configured as aggregation type `boolean`, which has been deprecated. Please use "
                        f"`sum_boolean` instead."
                    )
                if measure.agg == AggregationType.BOOLEAN or measure.agg == AggregationType.SUM_BOOLEAN:
                    if measure.expr:
                        measure.expr = f"CASE WHEN {measure.expr} THEN 1 ELSE 0 END"
                    else:
                        measure.expr = f"CASE WHEN {measure.name} THEN 1 ELSE 0 END"

                    measure.agg = AggregationType.SUM

        return model
