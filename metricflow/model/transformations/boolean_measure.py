import logging

from metricflow.model.objects.elements.measure import AggregationType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule

logger = logging.getLogger(__name__)


class BooleanMeasureAggregationRule(ModelTransformRule):
    """Converts the expression used in boolean measures so that it can be aggregated."""

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.agg == AggregationType.BOOLEAN:
                    logger.warning(
                        f"In data source {data_source.name}, measure `{measure.name}` is configured as "
                        f"aggregation type `boolean`, which has been deprecated. Please use `sum_boolean` "
                        f"instead."
                    )
                if measure.agg == AggregationType.BOOLEAN or measure.agg == AggregationType.SUM_BOOLEAN:
                    if measure.expr:
                        measure.expr = f"CASE WHEN {measure.expr} THEN 1 ELSE 0 END"
                    else:
                        measure.expr = f"CASE WHEN {measure.name} THEN 1 ELSE 0 END"

                    measure.agg = AggregationType.SUM

        return model
