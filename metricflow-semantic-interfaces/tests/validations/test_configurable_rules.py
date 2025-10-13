from __future__ import annotations

import copy

import pytest
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetricInput,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import metric_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import MetricType
from metricflow_semantic_interfaces.validations.metrics import DerivedMetricRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)


def test_can_configure_model_validator_rules(  # noqa: D103
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = copy.deepcopy(simple_semantic_manifest__with_primary_transforms)
    model.metrics.append(
        metric_with_guaranteed_meta(
            name="metric_doesnt_exist_squared",
            type=MetricType.DERIVED,
            type_params=PydanticMetricTypeParams(
                expr="metric_doesnt_exist * metric_doesnt_exist",
                metrics=[PydanticMetricInput(name="metric_doesnt_exist")],
            ),
        )
    )

    # confirm that with the default configuration, an issue is raised
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(model)
    assert (
        len(issues.errors) == 1
    ), f"SemanticManifestValidator with default rules had unexpected number of errors {issues.errors}"

    # confirm that a custom configuration excluding ValidMaterializationRule, no issue is raised
    rules = [rule for rule in validator.DEFAULT_RULES if rule.__class__ is not DerivedMetricRule]
    issues = SemanticManifestValidator[PydanticSemanticManifest](rules=rules).validate_semantic_manifest(model)
    assert (
        len(issues.errors) == 0
    ), f"SemanticManifestValidator without DerivedMetricRule returned issues {issues.errors}"


def test_cant_configure_model_validator_without_rules() -> None:  # noqa: D103
    with pytest.raises(ValueError):
        SemanticManifestValidator[PydanticSemanticManifest](rules=[])

    with pytest.raises(ValueError):
        SemanticManifestValidator[PydanticSemanticManifest](rules=())

    with pytest.raises(ValueError):
        SemanticManifestValidator[PydanticSemanticManifest](rules=None)  # type: ignore
