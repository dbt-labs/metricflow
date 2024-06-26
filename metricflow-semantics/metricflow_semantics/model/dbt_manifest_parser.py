from __future__ import annotations

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
from dbt_semantic_interfaces.transformations.convert_median import (
    ConvertMedianToPercentileRule,
)
from dbt_semantic_interfaces.transformations.cumulative_type_params import SetCumulativeTypeParamsRule
from dbt_semantic_interfaces.transformations.names import LowerCaseNamesRule
from dbt_semantic_interfaces.transformations.proxy_measure import CreateProxyMeasureRule
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
    rules = (
        # Primary
        (LowerCaseNamesRule(),),
        # Secondary
        (
            CreateProxyMeasureRule(),
            BooleanMeasureAggregationRule(),
            ConvertCountToSumRule(),
            ConvertMedianToPercentileRule(),
            DedupeMetricInputMeasuresRule(),  # Remove once fix is in core
            SetCumulativeTypeParamsRule(),
        ),
    )
    model = PydanticSemanticManifestTransformer.transform(raw_model, rules)
    return model
