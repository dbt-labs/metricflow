from __future__ import annotations

from typing import Set

from typing_extensions import override

from metricflow_semantic_interfaces.enum_extension import assert_values_exhausted
from metricflow_semantic_interfaces.errors import ModelTransformError
from metricflow_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from metricflow_semantic_interfaces.type_enums import MetricType, TimeGranularity


class RemovePluralFromWindowGranularityRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Remove trailing s from granularity in MetricTimeWindow.

    During parsing, MetricTimeWindow.granularity can still contain he trailing 's' (ie., 3 days).
    This is because with the introduction of custom granularities, we don't have access to the valid
    custom grains during parsing. This transformation rule is introduced to remove the trailing 's'
    from `MetricTimeWindow.granularity` if necessary.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D102
        return self

    @staticmethod
    def _update_metric(
        semantic_manifest: PydanticSemanticManifest, metric_name: str, custom_granularity_names: Set[str]
    ) -> None:
        """Mutates all the MetricTimeWindow by reparsing to remove the trailing 's'."""
        valid_time_granularities = {item.value.lower() for item in TimeGranularity} | set(
            c.lower() for c in custom_granularity_names
        )

        def trim_trailing_s(window: PydanticMetricTimeWindow) -> PydanticMetricTimeWindow:
            """Reparse the window to remove the trailing 's'."""
            granularity = window.granularity
            if granularity.endswith("s") and granularity[:-1] in valid_time_granularities:
                # months -> month
                granularity = granularity[:-1]
            window.granularity = granularity
            return window

        matched_metric = next(
            iter((metric for metric in semantic_manifest.metrics if metric.name == metric_name)), None
        )
        if matched_metric:
            if matched_metric.type is MetricType.CUMULATIVE:
                if (
                    matched_metric.type_params.cumulative_type_params
                    and matched_metric.type_params.cumulative_type_params.window
                ):
                    matched_metric.type_params.cumulative_type_params.window = trim_trailing_s(
                        matched_metric.type_params.cumulative_type_params.window
                    )

            elif matched_metric.type is MetricType.CONVERSION:
                if (
                    matched_metric.type_params.conversion_type_params
                    and matched_metric.type_params.conversion_type_params.window
                ):
                    matched_metric.type_params.conversion_type_params.window = trim_trailing_s(
                        matched_metric.type_params.conversion_type_params.window
                    )

            elif matched_metric.type is MetricType.DERIVED or matched_metric.type is MetricType.RATIO:
                for input_metric in matched_metric.input_metrics:
                    if input_metric.offset_window:
                        input_metric.offset_window = trim_trailing_s(input_metric.offset_window)
            elif matched_metric.type is MetricType.SIMPLE:
                pass
            else:
                assert_values_exhausted(matched_metric.type)
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D102
        custom_granularity_names = {
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        }

        for metric in semantic_manifest.metrics:
            RemovePluralFromWindowGranularityRule._update_metric(
                semantic_manifest=semantic_manifest,
                metric_name=metric.name,
                custom_granularity_names=custom_granularity_names,
            )
        return semantic_manifest
