from typing import Set

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.objects.metric import MetricInputMeasure, MetricType
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.transformations.transform_rule import ModelTransformRule


class AddInputMetricMeasuresRule(ModelTransformRule):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @staticmethod
    def _get_measures_for_metric(model: SemanticManifest, metric_name: str) -> Set[MetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures = set()
        matched_metric = next(iter((metric for metric in model.metrics if metric.name == metric_name)), None)
        if matched_metric:
            if matched_metric.type == MetricType.DERIVED:
                for input_metric in matched_metric.input_metrics:
                    measures.update(AddInputMetricMeasuresRule._get_measures_for_metric(model, input_metric.name))
            else:
                measures.update(set(matched_metric.input_measures))
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def transform_model(model: SemanticManifest) -> SemanticManifest:  # noqa: D
        for metric in model.metrics:
            if metric.type == MetricType.DERIVED:
                measures = AddInputMetricMeasuresRule._get_measures_for_metric(model, metric.name)
                assert (
                    metric.type_params.measures is None
                ), "Derived metric should have no measures predefined in the config"
                metric.type_params.measures = list(measures)
        return model
