from __future__ import annotations

import logging
from typing import Sequence

from typing_extensions import override

from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.protocols import ProtocolHint
from metricflow_semantic_interfaces.transformations.add_input_metric_measures import (
    AddInputMetricMeasuresRule,
)
from metricflow_semantic_interfaces.transformations.boolean_aggregations import (
    BooleanAggregationRule,
)
from metricflow_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from metricflow_semantic_interfaces.transformations.convert_count import (
    ConvertCountMetricToSumRule,
    ConvertCountToSumRule,
)
from metricflow_semantic_interfaces.transformations.convert_median import (
    ConvertMedianMetricToPercentile,
    ConvertMedianToPercentileRule,
)
from metricflow_semantic_interfaces.transformations.cumulative_type_params import (
    SetCumulativeTypeParamsRule,
)
from metricflow_semantic_interfaces.transformations.flatten_simple_metrics_with_measure_inputs import (
    FlattenSimpleMetricsWithMeasureInputsRule,
)
from metricflow_semantic_interfaces.transformations.names import LowerCaseNamesRule
from metricflow_semantic_interfaces.transformations.proxy_measure import CreateProxyMeasureRule
from metricflow_semantic_interfaces.transformations.remove_plural_from_window_granularity import (
    RemovePluralFromWindowGranularityRule,
)
from metricflow_semantic_interfaces.transformations.replace_input_measures_with_simple_metrics_transformation import (
    ReplaceInputMeasuresWithSimpleMetricsTransformationRule,
)
from metricflow_semantic_interfaces.transformations.rule_set import (
    SemanticManifestTransformRuleSet,
)
from metricflow_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class PydanticSemanticManifestTransformRuleSet(
    ProtocolHint[SemanticManifestTransformRuleSet[PydanticSemanticManifest]]
):
    """Transform rules that should be used for the Pydantic implementation of SemanticManifest."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRuleSet[PydanticSemanticManifest]:  # noqa: D102
        return self

    @property
    def legacy_measure_update_rules(
        self,
    ) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D102
        """Legacy rules - Primarily editing legacy measures."""
        return (
            BooleanMeasureAggregationRule(),
            ConvertCountToSumRule(),
            ConvertMedianToPercentileRule(),
        )

    @property
    def convert_legacy_measures_to_metrics_rules(
        self,
    ) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D103
        """Rules that create or update metrics to replace the use of legacy measures.

        Should run after all measures are processed and fixed, but before polishing
        all metrics (because the metrics need to be created here to be polished later).
        """
        return (
            # CreateProxyMeasureRule should always run FIRST in this sequence.
            CreateProxyMeasureRule(),  # FIRST, I SAY!
            # This populates "input_measures" for metric fields.
            # This does NOT add new metrics or depend on most newly-added metrics, but it must
            # run after CreateProxyMeasureRule() to ensure we have all the metrics we will need.
            AddInputMetricMeasuresRule(),
            FlattenSimpleMetricsWithMeasureInputsRule(),
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule(),
        )

    @property
    def general_metric_update_rules(
        self,
    ) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D103
        """These rules apply once all metrics exist; they apply universally to any metric that meet their criteria.

        These should be run AFTER all metrics exist.
        """
        return (
            SetCumulativeTypeParamsRule(),
            RemovePluralFromWindowGranularityRule(),
            ConvertMedianMetricToPercentile(),
            ConvertCountMetricToSumRule(),
            BooleanAggregationRule(),
        )

    @property
    def primary_rules(self) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D102
        return (LowerCaseNamesRule(),)

    @property
    def secondary_rules(self) -> Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]:  # noqa: D102
        """Secondary rules - Primarily editing, copying, or adapting measures and metrics."""
        # Order matters here!
        return [
            *self.legacy_measure_update_rules,
            *self.convert_legacy_measures_to_metrics_rules,
            *self.general_metric_update_rules,
        ]

    @property
    def all_rules(self) -> Sequence[Sequence[SemanticManifestTransformRule[PydanticSemanticManifest]]]:  # noqa: D102
        return self.primary_rules, self.secondary_rules
