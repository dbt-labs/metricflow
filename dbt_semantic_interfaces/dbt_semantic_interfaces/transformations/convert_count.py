from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule

ONE = "1"


class ConvertCountToSumRule(ModelTransformRule):
    """Converts any COUNT measures to SUM equivalent."""

    @staticmethod
    def transform_model(model: UserConfiguredModel) -> UserConfiguredModel:  # noqa: D
        for data_source in model.data_sources:
            for measure in data_source.measures:
                if measure.agg == AggregationType.COUNT:
                    if measure.expr is None:
                        raise ModelTransformError(
                            f"Measure '{measure.name}' uses a COUNT aggregation, which requires an expr to be provided. "
                            f"Provide 'expr: 1' if a count of all rows is desired."
                        )
                    if measure.expr != ONE:
                        # Just leave it as SUM(1) if we want to count all
                        measure.expr = f"CASE WHEN {measure.expr} IS NOT NULL THEN 1 ELSE 0 END"
                    measure.agg = AggregationType.SUM
        return model
