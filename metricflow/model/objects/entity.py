from __future__ import annotations

from typing import List, Optional, Sequence

from metricflow.instances import MetricFlowEntityReference
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.conversions import MetricFlowMetricFlowEntity
from metricflow.model.objects.base import ModelWithMetadataParsing, HashableBaseModel
from metricflow.object_utils import ExtendedEnum
from metricflow.references import LinkableElementReference, MeasureReference


class MetricFlowEntity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity"""

    name: str
    description: Optional[str]
    sql_table: Optional[str]
    sql_query: Optional[str]

    identifiers: Sequence[Identifier] = []
    measures: Sequence[Measure] = []
    dimensions: Sequence[Dimension] = []

    mutability: Mutability = Mutability(type=MutabilityType.FULL_MUTATION)

    origin: MetricFlowEntityOrigin = MetricFlowEntityOrigin.SOURCE
    metadata: Optional[Metadata]

    @property
    def identifier_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.identifiers]

    @property
    def dimension_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.dimensions]

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
        return [i.reference for i in self.measures]

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        for measure in self.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in entity with name ({self.name})"
        )

    def get_dimension(self, dimension_reference: LinkableElementReference) -> Dimension:  # noqa: D
        for dim in self.dimensions:
            if dim.reference == dimension_reference:
                return dim

        raise ValueError(f"No dimension with name ({dimension_reference}) in entity with name ({self.name})")

    def get_identifier(self, identifier_reference: LinkableElementReference) -> Identifier:  # noqa: D
        for ident in self.identifiers:
            if ident.reference == identifier_reference:
                return ident

        raise ValueError(f"No identifier with name ({identifier_reference}) in entity with name ({self.name})")

    @property
    def has_validity_dimensions(self) -> bool:
        """Returns True if there are validity params set on one or more dimensions"""
        return any([dim.validity_params is not None for dim in self.dimensions])

    @property
    def validity_start_dimension(self) -> Optional[Dimension]:
        """Returns the validity window start dimension, if one is set"""
        validity_start_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_start]
        if not validity_start_dims:
            return None
        assert (
            len(validity_start_dims) == 1
        ), "Found more than one validity start dimension. This should have been blocked in validation!"
        return validity_start_dims[0]

    @property
    def validity_end_dimension(self) -> Optional[Dimension]:
        """Returns the validity window end dimension, if one is set"""
        validity_end_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_end]
        if not validity_end_dims:
            return None
        assert (
            len(validity_end_dims) == 1
        ), "Found more than one validity end dimension. This should have been blocked in validation!"
        return validity_end_dims[0]

    @property
    def partitions(self) -> List[Dimension]:  # noqa: D
        return [dim for dim in self.dimensions or [] if dim.is_partition]

    @property
    def partition(self) -> Optional[Dimension]:  # noqa: D
        partitions = self.partitions
        if not partitions:
            return None
        if len(partitions) > 1:
            raise ValueError(f"too many partitions for entity {self.name}")
        return partitions[0]

    @property
    def reference(self) -> MetricFlowEntityReference:  # noqa: D
        return MetricFlowEntityReference(entity_name=self.name)
