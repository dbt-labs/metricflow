from __future__ import annotations

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.transformations.fix_proxy_metrics import (
    FixProxyMetricsRule,
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


def transform_dbt_generated_manifest(manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
    """Transform a PydanticSemanticManifest given the generated semantic_manifest json from dbt."""
    # The serialized object in the dbt project does not have all transformations applied to it at
    # this time, which causes failures with input simple-metric input resolution.
    # TODO: remove this transform call once the upstream changes are integrated into our dependency tree
    # TODO: align rules between DSI, here, and MFS (if possible!)
    rule_set = PydanticSemanticManifestTransformRuleSet()
    rules = (
        # Primary
        (LowerCaseNamesRule(),),
        # Secondary
        (
            *rule_set.legacy_measure_update_rules,
            # These individual rules come from rule_set.convert_legacy_measures_to_metrics_rules, but
            # dsi requires AddInputMetricMeasuresRule, and metricflow requires that we do NOT run that rule
            # as it is incompatible with a parser like dbt-core that pre-populates input measures.
            CreateProxyMeasureRule(),
            FlattenSimpleMetricsWithMeasureInputsRule(),
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule(),
            FixProxyMetricsRule(),
            *rule_set.general_metric_update_rules,
        ),
    )
    return PydanticSemanticManifestTransformer.transform(manifest, rules)


def parse_manifest_from_dbt_generated_manifest(manifest_json_string: str) -> PydanticSemanticManifest:
    """Parse a PydanticSemanticManifest given the generated semantic_manifest json from dbt."""
    raw_model = PydanticSemanticManifest.parse_raw(manifest_json_string)
    return transform_dbt_generated_manifest(raw_model)
