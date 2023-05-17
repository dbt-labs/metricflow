from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.objects.elements.measure import MeasureAggregationParameters
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule

MEDIAN_PERCENTILE = 0.5


class ConvertMedianToPercentileRule(ModelTransformRule):
    """Converts any MEDIAN measures to percentile equivalent."""

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.MEDIAN:
                    measure.agg = AggregationType.PERCENTILE

                    if not measure.agg_params:
                        measure.agg_params = MeasureAggregationParameters()
                    else:
                        if measure.agg_params.percentile is not None and measure.agg_params.percentile != 0.5:
                            raise ModelTransformError(
                                f"Measure '{measure.name}' uses a MEDIAN aggregation, while percentile is set to "
                                f"'{measure.agg_params.percentile}', a conflicting value. Please remove the parameter "
                                "or set to '0.5'."
                            )
                        if measure.agg_params.use_discrete_percentile:
                            raise ModelTransformError(
                                f"Measure '{measure.name}' uses a MEDIAN aggregation, while use_discrete_percentile"
                                f"is set to true. Please remove the parameter or set to False."
                            )
                    measure.agg_params.percentile = MEDIAN_PERCENTILE
                    # let's not set use_approximate_percentile to be false due to valid performance reasons
        return model
