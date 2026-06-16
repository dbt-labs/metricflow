from __future__ import annotations

from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import (
    check_no_errors_or_warnings,
    check_only_one_future_error_with_message,
    semantic_model_with_guaranteed_meta,
)
from metricflow_semantic_interfaces.type_enums import (
    DimensionType,
    EntityType,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.time_dimension_has_granularity import (
    TimeDimensionHasGranularityRule,
)


def test_time_dimension_missing_granularity() -> None:  # noqa: D103
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="model_with_time_dim_missing_grain",
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
                dimensions=[
                    PydanticDimension(
                        name="ds",
                        type=DimensionType.TIME,
                        type_params=None,
                        description="",
                        metadata=None,
                    )
                ],
            )
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[],
        ),
    )

    validator = SemanticManifestValidator[PydanticSemanticManifest]([TimeDimensionHasGranularityRule()])
    results = validator.validate_semantic_manifest(semantic_manifest)
    check_only_one_future_error_with_message(
        results,
        "time dimension",
    )


def test_time_dimension_with_granularity_ok() -> None:  # noqa: D103
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="model_with_time_dim",
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
                dimensions=[
                    PydanticDimension(
                        name="ds",
                        type=DimensionType.TIME,
                        type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
                        description="",
                        metadata=None,
                    )
                ],
            )
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[],
        ),
    )

    validator = SemanticManifestValidator[PydanticSemanticManifest]([TimeDimensionHasGranularityRule()])
    results = validator.validate_semantic_manifest(semantic_manifest)
    check_no_errors_or_warnings(results)


def test_non_time_dimension_ok() -> None:  # noqa: D103
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="model_with_non_time_dim",
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
                dimensions=[
                    PydanticDimension(
                        name="category",
                        type=DimensionType.CATEGORICAL,
                        description="",
                        metadata=None,
                    )
                ],
            )
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[],
        ),
    )

    validator = SemanticManifestValidator[PydanticSemanticManifest]([TimeDimensionHasGranularityRule()])
    results = validator.validate_semantic_manifest(semantic_manifest)
    check_no_errors_or_warnings(results)
