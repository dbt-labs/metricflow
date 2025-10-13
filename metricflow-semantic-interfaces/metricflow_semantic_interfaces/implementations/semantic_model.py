from __future__ import annotations

from typing import List, Optional, Sequence

from msi_pydantic_shim import Field
from typing_extensions import override

from metricflow_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.implementations.elements.dimension import PydanticDimension
from metricflow_semantic_interfaces.implementations.elements.entity import PydanticEntity
from metricflow_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from metricflow_semantic_interfaces.implementations.metadata import PydanticMetadata
from metricflow_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from metricflow_semantic_interfaces.protocols import (
    ProtocolHint,
    SemanticModel,
    SemanticModelDefaults,
)
from metricflow_semantic_interfaces.protocols.metric import Metric
from metricflow_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MeasureReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from metricflow_semantic_interfaces.type_enums.metric_type import MetricType


class PydanticSemanticModelDefaults(HashableBaseModel, ProtocolHint[SemanticModelDefaults]):  # noqa: D101
    @override
    def _implements_protocol(self) -> SemanticModelDefaults:  # noqa: D102
        return self

    agg_time_dimension: Optional[str]


class PydanticSemanticModel(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[SemanticModel]):
    """Describes a semantic model."""

    @override
    def _implements_protocol(self) -> SemanticModel:
        return self

    name: str
    defaults: Optional[PydanticSemanticModelDefaults]
    description: Optional[str]
    node_relation: PydanticNodeRelation

    primary_entity: Optional[str]
    entities: Sequence[PydanticEntity] = Field(default_factory=list)
    measures: Sequence[PydanticMeasure] = Field(default_factory=list)
    dimensions: Sequence[PydanticDimension] = Field(default_factory=list)
    label: Optional[str] = None

    metadata: Optional[PydanticMetadata]
    config: Optional[PydanticSemanticLayerElementConfig]

    @property
    def entity_references(self) -> List[LinkableElementReference]:  # noqa: D102
        return [i.reference for i in self.entities]

    @property
    def dimension_references(self) -> List[LinkableElementReference]:  # noqa: D102
        return [i.reference for i in self.dimensions]

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D102
        return [i.reference for i in self.measures]

    @property
    def has_validity_dimensions(self) -> bool:  # noqa: D102
        return any([dim.validity_params is not None for dim in self.dimensions])

    @property
    def validity_start_dimension(self) -> Optional[PydanticDimension]:  # noqa: D102
        validity_start_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_start]
        if not validity_start_dims:
            return None
        assert (
            len(validity_start_dims) == 1
        ), "Found more than one validity start dimension. This should have been blocked in validation!"
        return validity_start_dims[0]

    @property
    def validity_end_dimension(self) -> Optional[PydanticDimension]:  # noqa: D102
        validity_end_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_end]
        if not validity_end_dims:
            return None
        assert (
            len(validity_end_dims) == 1
        ), "Found more than one validity end dimension. This should have been blocked in validation!"
        return validity_end_dims[0]

    @property
    def partitions(self) -> List[PydanticDimension]:  # noqa: D102
        return [dim for dim in self.dimensions or [] if dim.is_partition]

    @property
    def partition(self) -> Optional[PydanticDimension]:  # noqa: D102
        partitions = self.partitions
        if not partitions:
            return None
        if len(partitions) > 1:
            raise ValueError(f"too many partitions for semantic_model {self.name}")
        return partitions[0]

    @property
    def reference(self) -> SemanticModelReference:  # noqa: D102
        return SemanticModelReference(semantic_model_name=self.name)

    def get_measure(self, measure_reference: MeasureReference) -> PydanticMeasure:  # noqa: D102
        for measure in self.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in semantic_model with name ({self.name})"
        )

    def get_dimension(self, dimension_reference: DimensionReference) -> PydanticDimension:  # noqa: D102
        for dim in self.dimensions:
            if dim.reference == dimension_reference:
                return dim

        raise ValueError(f"No dimension with name ({dimension_reference}) in semantic_model with name ({self.name})")

    def get_entity(self, entity_reference: LinkableElementReference) -> PydanticEntity:  # noqa: D102
        for entity in self.entities:
            if entity.reference == entity_reference:
                return entity

        raise ValueError(f"No entity with name ({entity_reference}) in semantic_model with name ({self.name})")

    def _get_default_agg_time_dimension(self) -> Optional[str]:  # noqa: D102
        return self.defaults.agg_time_dimension if self.defaults is not None else None

    def checked_agg_time_dimension_for_simple_metric(  # noqa: D102
        self,
        metric: Metric,
    ) -> TimeDimensionReference:
        metric_time_dimension_name = None
        assert metric.type == MetricType.SIMPLE, "Only simple metrics can have an agg time dimension."
        metric_agg_params = metric.type_params.metric_aggregation_params
        # There are validations elsewhere to check this for metrics and provide messaging for it.
        assert metric_agg_params, "Simple metrics must have metric_aggregation_params."
        # This indicates a validation bug / dev error, not a user error that should appear
        # in a user's YAML.
        assert (
            metric_agg_params.semantic_model == self.name
        ), "Cannot retrieve the agg time dimension for a metric from a different model "
        f"than the one that the metric belongs to. Metric `{metric.name}` belongs to model "
        f"`{metric_agg_params.semantic_model}`, but we requested the agg time dimension from model `{self.name}`."

        if (
            metric.type_params
            and metric.type_params.metric_aggregation_params
            and metric.type_params.metric_aggregation_params.agg_time_dimension
        ):
            metric_time_dimension_name = metric.type_params.metric_aggregation_params.agg_time_dimension

        default_agg_time_dimension = self._get_default_agg_time_dimension()
        agg_time_dimension_name = metric_time_dimension_name or default_agg_time_dimension

        assert agg_time_dimension_name is not None, (
            f"Aggregation time dimension for metric {metric.name} is not set! This should either be set directly on "
            f"the metric specification in the model, or else defaulted to the time dimension in the data "
            f"source containing the metric."
        )
        return TimeDimensionReference(element_name=agg_time_dimension_name)

    def checked_agg_time_dimension_for_measure(  # noqa: D102
        self,
        measure_reference: MeasureReference,
    ) -> TimeDimensionReference:
        measure = self.get_measure(measure_reference=measure_reference)
        default_agg_time_dimension = self.defaults.agg_time_dimension if self.defaults is not None else None

        agg_time_dimension_name = measure.agg_time_dimension or default_agg_time_dimension
        assert agg_time_dimension_name is not None, (
            f"Aggregation time dimension for measure {measure.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the primary time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(element_name=agg_time_dimension_name)

    @property
    def primary_entity_reference(self) -> Optional[EntityReference]:  # noqa: D102
        return EntityReference(element_name=self.primary_entity) if self.primary_entity is not None else None
