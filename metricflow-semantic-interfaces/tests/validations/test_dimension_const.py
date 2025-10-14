from __future__ import annotations

import pytest
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    MetricType,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.dimension_const import DimensionConsistencyRule
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from metricflow_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_incompatible_dimension_type() -> None:  # noqa: D103
    with pytest.raises(SemanticManifestValidationException, match=r"type conflict for dimension"):
        dim_name = "dim"
        measure_name = "measure"
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]([DimensionConsistencyRule()])
        model_validator.checked_validations(
            PydanticSemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="dim1",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                    semantic_model_with_guaranteed_meta(
                        name="categoricaldim",
                        dimensions=[PydanticDimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.SIMPLE,
                        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                    )
                ],
                project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa: D103
    with pytest.raises(SemanticManifestValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_name = "dim1"
        measure_name = "measure"
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]([DimensionConsistencyRule()])
        model_validator.checked_validations(
            PydanticSemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="dim1",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=True,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                    semantic_model_with_guaranteed_meta(
                        name="dim2",
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=False,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.SIMPLE,
                        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                    )
                ],
                project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
            )
        )
