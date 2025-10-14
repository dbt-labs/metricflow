from __future__ import annotations

from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticCumulativeTypeParams,
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import (
    PydanticSemanticModel,
    PydanticSemanticModelDefaults,
)
from metricflow_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)
from metricflow_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from metricflow_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)


def _project_config() -> PydanticProjectConfiguration:
    return PydanticProjectConfiguration()


def test_e2e_measure_create_metric_then_cumulative_uses_metric_input() -> None:
    """End-to-end: measure create_metric=True, cumulative references created metric by name."""
    sm_name = "sm"
    time_dim_name = "ds"
    sm = PydanticSemanticModel(
        name=sm_name,
        defaults=PydanticSemanticModelDefaults(agg_time_dimension=time_dim_name),
        node_relation=PydanticNodeRelation(alias=sm_name, schema_name="schema"),
        entities=[
            PydanticEntity(
                name="user",
                type=EntityType.PRIMARY,
                expr="user_id",
            ),
        ],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            ),
            PydanticDimension(
                name="created_at",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            ),
            PydanticDimension(
                name="ds_partitioned",
                type=DimensionType.TIME,
                is_partition=True,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            ),
            PydanticDimension(
                name="home_state",
                type=DimensionType.CATEGORICAL,
            ),
            PydanticDimension(
                name="last_profile_edit_ts",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.MILLISECOND),
            ),
            PydanticDimension(
                name="bio_added_ts",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.SECOND),
            ),
            PydanticDimension(
                name="last_login_ts",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.MINUTE),
            ),
            PydanticDimension(
                name="archived_at",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.HOUR),
            ),
        ],
        measures=[
            PydanticMeasure(
                name="archived_users",
                agg=AggregationType.SUM,
                expr="1",
                create_metric=True,
            )
        ],
    )

    metrics = [
        PydanticMetric(
            name="subdaily_cumulative_window_metric",
            type=MetricType.CUMULATIVE,
            description="m1_cumulative_1 description",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(name="archived_users"),
                cumulative_type_params=PydanticCumulativeTypeParams(
                    window=PydanticMetricTimeWindow(count=3, granularity="hour"),
                ),
            ),
        ),
        PydanticMetric(
            name="subdaily_cumulative_grain_to_date_metric",
            type=MetricType.CUMULATIVE,
            description="m1_cumulative_2 description",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(name="archived_users"),
                cumulative_type_params=PydanticCumulativeTypeParams(
                    grain_to_date="hour",
                ),
            ),
        ),
        PydanticMetric(
            name="subdaily_offset_window_metric",
            type=MetricType.DERIVED,
            description="archived_users_offset_window description",
            type_params=PydanticMetricTypeParams(
                expr="archived_users",
                metrics=[
                    PydanticMetricInput(
                        name="archived_users",
                        offset_window=PydanticMetricTimeWindow(count=1, granularity="hour"),
                    )
                ],
            ),
        ),
        PydanticMetric(
            name="subdaily_offset_grain_to_date_metric",
            type=MetricType.DERIVED,
            description="offset grain to date metric with a sub-daily agg time dim",
            type_params=PydanticMetricTypeParams(
                expr="archived_users",
                metrics=[
                    PydanticMetricInput(
                        name="archived_users",
                        offset_to_grain="hour",
                    )
                ],
            ),
        ),
        PydanticMetric(
            name="subdaily_join_to_time_spine_metric",
            type=MetricType.SIMPLE,
            description="simple metric with sub-daily agg time dim that joins to time spine",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(
                    name="archived_users",
                    join_to_timespine=True,
                ),
            ),
        ),
        PydanticMetric(
            name="simple_subdaily_metric_default_day",
            type=MetricType.SIMPLE,
            description="simple metric with sub-daily agg time dim that doesn't specify default granularity",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(
                    name="archived_users",
                ),
            ),
        ),
        PydanticMetric(
            name="simple_subdaily_metric_default_hour",
            type=MetricType.SIMPLE,
            description="simple metric with sub-daily agg time dim that has an explicit default granularity",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(
                    name="archived_users",
                ),
            ),
            time_granularity="hour",
        ),
        PydanticMetric(
            name="archived_users_join_to_time_spine",
            type=MetricType.SIMPLE,
            description="subdaily metric joining to time spine",
            type_params=PydanticMetricTypeParams(
                measure=PydanticMetricInputMeasure(
                    name="archived_users",
                    join_to_timespine=True,
                ),
            ),
        ),
    ]

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=metrics,
        project_configuration=_project_config(),
    )

    transformed = PydanticSemanticManifestTransformer.transform(model=manifest)

    model_validator = SemanticManifestValidator[PydanticSemanticManifest]()
    model_validator.checked_validations(transformed)

    # Expect exactly 1 new metric - the proxy simple metric created for the measure
    assert len(transformed.metrics) == len(metrics) + 1
    assert any(m for m in transformed.metrics if m.type == MetricType.SIMPLE and m.name == "archived_users")
