from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.elements.measure import MeasureAggregationParameters
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.transformations.transform_rule import ModelTransformRule

MEDIAN_PERCENTILE = 0.5


class ConvertMedianToPercentileRule(ModelTransformRule):
    """Converts any MEDIAN measures to percentile equivalent."""

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.agg == AggregationType.MEDIAN:
                    measure.agg = AggregationType.PERCENTILE
                    if not measure.agg_params:
                        measure.agg_params = MeasureAggregationParameters()
                    measure.agg_params.percentile = MEDIAN_PERCENTILE
                    measure.agg_params.disc = False
        return model
