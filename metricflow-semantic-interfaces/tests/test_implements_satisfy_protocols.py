from __future__ import annotations

from typing import List, Protocol, runtime_checkable

from hypothesis import given
from hypothesis.strategies import booleans, builds, from_type, just, lists, none, text
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
    PydanticDimensionValidityParams,
)
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
    PydanticNonAdditiveDimensionParameters,
)
from metricflow_semantic_interfaces.implementations.export import PydanticExport
from metricflow_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.implementations.metric import (
    PydanticConversionTypeParams,
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from metricflow_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from metricflow_semantic_interfaces.implementations.saved_query import (
    PydanticSavedQuery,
    PydanticSavedQueryQueryParams,
)
from metricflow_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from metricflow_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from metricflow_semantic_interfaces.implementations.time_spine_table_configuration import (
    PydanticTimeSpineTableConfiguration,
)
from metricflow_semantic_interfaces.protocols import Dimension as DimensionProtocol
from metricflow_semantic_interfaces.protocols import Entity as EntityProtocol
from metricflow_semantic_interfaces.protocols import Measure as MeasureProtocol
from metricflow_semantic_interfaces.protocols import Metadata as MetadataProtocol
from metricflow_semantic_interfaces.protocols import Metric as MetricProtocol
from metricflow_semantic_interfaces.protocols import SavedQuery as SavedQueryProtocol
from metricflow_semantic_interfaces.protocols import (
    SemanticManifest as SemanticManifestProtocol,
)
from metricflow_semantic_interfaces.protocols import SemanticModel as SemanticModelProtocol
from metricflow_semantic_interfaces.protocols.time_spine_configuration import (
    TimeSpineTableConfiguration as TimeSpineTableConfigurationProtocol,
)
from metricflow_semantic_interfaces.type_enums import DimensionType, MetricType

OPTIONAL_STR_STRATEGY = text() | none()
OPTIONAL_METADATA_STRATEGY = builds(PydanticMetadata) | none()

CATEGORICAL_DIMENSION_STRATEGY = builds(
    PydanticDimension,
    description=OPTIONAL_STR_STRATEGY,
    type=just(DimensionType.CATEGORICAL),
    expr=OPTIONAL_STR_STRATEGY,
    metadata=OPTIONAL_METADATA_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    config=builds(PydanticSemanticLayerElementConfig),
)

DIMENSION_VALIDITY_PARAMS_STRATEGY = builds(
    PydanticDimensionValidityParams,
    is_start=just(False),
    is_end=just(False),
)

TIME_DIMENSION_STRATEGY = builds(
    PydanticDimension,
    description=OPTIONAL_STR_STRATEGY,
    type=just(DimensionType.TIME),
    type_params=builds(PydanticDimensionTypeParams) | none(),
    expr=OPTIONAL_STR_STRATEGY,
    metadata=OPTIONAL_METADATA_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    config=builds(PydanticSemanticLayerElementConfig),
)

DIMENSION_STRATEGY = TIME_DIMENSION_STRATEGY | CATEGORICAL_DIMENSION_STRATEGY

ENTITY_STRATEGY = builds(
    PydanticEntity,
    description=OPTIONAL_STR_STRATEGY,
    role=OPTIONAL_STR_STRATEGY,
    expr=OPTIONAL_STR_STRATEGY,
    metadata=OPTIONAL_METADATA_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    config=builds(PydanticSemanticLayerElementConfig),
)

MEASURE_STRATEGY = builds(
    PydanticMeasure,
    description=OPTIONAL_STR_STRATEGY,
    create_metric=booleans() | none(),
    expr=OPTIONAL_STR_STRATEGY,
    agg_params=builds(PydanticMeasureAggregationParameters) | none(),
    non_additive_dimesnion=builds(PydanticNonAdditiveDimensionParameters) | none(),
    agg_time_dimension=OPTIONAL_STR_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    config=builds(PydanticSemanticLayerElementConfig),
)

SEMANTIC_MODEL_STRATEGY = builds(
    PydanticSemanticModel,
    dimensions=lists(DIMENSION_STRATEGY),
    entities=lists(ENTITY_STRATEGY),
    measures=lists(MEASURE_STRATEGY),
)

SIMPLE_METRIC_STRATEGY = builds(
    PydanticMetric,
    description=OPTIONAL_STR_STRATEGY,
    type=just(MetricType.SIMPLE),
    type_params=builds(PydanticMetricTypeParams, measure=builds(PydanticMetricInputMeasure)),
    filter=builds(PydanticWhereFilter) | none(),
    metadata=OPTIONAL_METADATA_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    config=builds(PydanticSemanticLayerElementConfig),
)

SAVED_QUERY_STRATEGY = builds(
    PydanticSavedQuery,
    query_params=builds(PydanticSavedQueryQueryParams),
    where=from_type(List[PydanticWhereFilter]),
    description=OPTIONAL_STR_STRATEGY,
    metadata=OPTIONAL_METADATA_STRATEGY,
    label=OPTIONAL_STR_STRATEGY,
    exports=from_type(List[PydanticExport]),
)


@runtime_checkable
class RuntimeCheckableSemanticManifest(SemanticManifestProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(
    builds(
        PydanticSemanticManifest,
        semantic_models=lists(SEMANTIC_MODEL_STRATEGY),
        metrics=lists(SIMPLE_METRIC_STRATEGY),
        saved_queries=lists(SAVED_QUERY_STRATEGY),
        project_configuration=builds(PydanticProjectConfiguration),
    )
)
def test_semantic_manifest_protocol(semantic_manifest: PydanticSemanticManifest) -> None:  # noqa: D103
    assert isinstance(semantic_manifest, RuntimeCheckableSemanticManifest)


@runtime_checkable
class RuntimeCheckableSemanticModel(SemanticModelProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(SEMANTIC_MODEL_STRATEGY)
def test_semantic_model_protocol(semantic_model: PydanticSemanticModel) -> None:  # noqa: D103
    assert isinstance(semantic_model, RuntimeCheckableSemanticModel)


@runtime_checkable
class RuntimeCheckableMetric(MetricProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(SIMPLE_METRIC_STRATEGY)
def test_metric_protocol_simple(metric: PydanticMetric) -> None:  # noqa: D103
    assert isinstance(metric, RuntimeCheckableMetric)


@given(
    builds(
        PydanticMetric,
        type=just(MetricType.RATIO),
        type_params=builds(
            PydanticMetricTypeParams,
            numerator=builds(PydanticMetricInput),
            denominator=builds(PydanticMetricInput),
        ),
    )
)
def test_metric_protocol_ratio(metric: PydanticMetric) -> None:  # noqa: D103
    assert isinstance(metric, RuntimeCheckableMetric)


@given(
    builds(
        PydanticMetric,
        type=just(MetricType.DERIVED),
        type_params=builds(PydanticMetricTypeParams, metrics=lists(builds(PydanticMetricInput))),
        expr=builds(str),
    )
)
def test_metric_protocol_derived(metric: PydanticMetric) -> None:  # noqa: D103
    assert isinstance(metric, RuntimeCheckableMetric)


@given(
    builds(
        PydanticMetric,
        type=just(MetricType.CONVERSION),
        type_params=builds(
            PydanticMetricTypeParams,
            conversion_type_params=builds(
                PydanticConversionTypeParams,
                base_measure=builds(PydanticMetricInputMeasure),
                conversion_measure=builds(PydanticMetricInputMeasure),
                entity=builds(str),
            ),
        ),
        expr=builds(str),
    )
)
def test_metric_protocol_conversion(metric: PydanticMetric) -> None:  # noqa: D103
    assert isinstance(metric, RuntimeCheckableMetric)


@runtime_checkable
class RuntimeCheckableEntity(EntityProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(ENTITY_STRATEGY)
def test_entity_protocol(entity: PydanticEntity) -> None:  # noqa: D103
    assert isinstance(entity, RuntimeCheckableEntity)


@runtime_checkable
class RuntimeCheckableMeasure(MeasureProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(MEASURE_STRATEGY)
def test_measure_protocol(measure: PydanticMeasure) -> None:  # noqa: D103
    assert isinstance(measure, RuntimeCheckableMeasure)


@runtime_checkable
class RuntimeCheckableDimension(DimensionProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(DIMENSION_STRATEGY)
def test_dimension_protocol(dimension: PydanticDimension) -> None:  # noqa: D103
    assert isinstance(dimension, RuntimeCheckableDimension)


@runtime_checkable
class RuntimeCheckableMetadata(MetadataProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticMetadata))
def test_metadata_protocol(metadata: PydanticMetadata) -> None:  # noqa: D103
    assert isinstance(metadata, RuntimeCheckableMetadata)


@runtime_checkable
class RuntimeCheckableSavedQuery(SavedQueryProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(SAVED_QUERY_STRATEGY)
def test_saved_query_protocol(saved_query: PydanticSavedQuery) -> None:  # noqa: D103
    assert isinstance(saved_query, RuntimeCheckableSavedQuery)


@runtime_checkable
class RuntimeCheckableTimeSpineConfiguration(TimeSpineTableConfigurationProtocol, Protocol):
    """We don't want runtime_checkable versions of protocols in the package, but we want them for tests."""

    pass


@given(builds(PydanticTimeSpineTableConfiguration))
def test_time_spine_table_configuration_protocol(time_spine: PydanticTimeSpineTableConfiguration) -> None:  # noqa: D103
    assert isinstance(time_spine, RuntimeCheckableTimeSpineConfiguration)
