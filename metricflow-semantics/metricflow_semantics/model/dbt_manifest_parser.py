from __future__ import annotations

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.transformations.flatten_simple_metrics_with_measure_inputs import (
    FlattenSimpleMetricsWithMeasureInputsRule,
)
from dbt_semantic_interfaces.transformations.names import LowerCaseNamesRule
from dbt_semantic_interfaces.transformations.proxy_measure import CreateProxyMeasureRule
from dbt_semantic_interfaces.transformations.pydantic_rule_set import PydanticSemanticManifestTransformRuleSet
from dbt_semantic_interfaces.transformations.replace_input_measures_with_simple_metrics_transformation import (
    ReplaceInputMeasuresWithSimpleMetricsTransformationRule,
)
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)

from metricflow_semantics.model.transformations.dedupe_metric_input_measures import DedupeMetricInputMeasuresRule


def parse_manifest_from_dbt_generated_manifest(manifest_json_string: str) -> PydanticSemanticManifest:
    """Parse a PydanticSemanticManifest given the generated semantic_manifest json from dbt."""
    raw_model = PydanticSemanticManifest.parse_raw(manifest_json_string)
    # The serialized object in the dbt project does not have all transformations applied to it at
    # this time, which causes failures with input measure resolution.
    # TODO: remove this transform call once the upstream changes are integrated into our dependency tree
    # TODO: align rules between DSI, here, and MFS (if possible!)
    rule_set = PydanticSemanticManifestTransformRuleSet()
    rules = (
        # Primary
        (LowerCaseNamesRule(),),
        # Secondary - broken out into groups because we run DedupeMetricInputMeasuresRule in the middle.
        (
            *rule_set.legacy_measure_update_rules,
            DedupeMetricInputMeasuresRule(),  # Remove once fix is in core
            # These individual rules come from rule_set.convert_legacy_measures_to_metrics_rules, but
            # dsi requires AddInputMetricMeasuresRule, and metricflow requires that we do NOT run that rule
            # as it is incompatible with a parser like dbt-core that pre-populates input measures.
            CreateProxyMeasureRule(),
            FlattenSimpleMetricsWithMeasureInputsRule(),
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule(),
            *rule_set.general_metric_update_rules,
        ),
    )
    model = PydanticSemanticManifestTransformer.transform(raw_model, rules)
    return model
