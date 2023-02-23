"""Protocols for classes meant to manage semantic information of useful object types.

These are useful as more generic descriptors of interfaces around classes like semantic containers and such, which
might be best used in a read-only mode. These containers could also pull in extra dependencies in their internal
implementations, dependencies which are best isolated inside the packages containing the concrete objects rather
than the interface specifications.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Dict, FrozenSet, List, Optional, Protocol, Sequence, Set, Tuple
from dbt.contracts.graph.entities import EntityOrigin
from dbt.contracts.graph.dimensions import Dimension
from dbt.contracts.graph.identifiers import Identifier
from dbt.contracts.graph.measures import Measure
from dbt.contracts.graph.nodes import Metric, Entity
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from dbt.semantic.references import (
    DimensionReference,
    IdentifierReference,
    MeasureReference,
    TimeDimensionReference,
    MetricReference,
    EntityReference,
    EntityElementReference
)

from metricflow.specs import (
    LinkableInstanceSpec,
    MeasureSpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
)


class EntitySemanticsAccessor(Protocol):
    """Protocol defining core interface for accessing semantic information about a set of entity objects

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the EntitySemantics class might implement this protocol but also include some
    public methods for adding or removing entities from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected entities.
    """

    @abstractmethod
    def get_dimension_references(self) -> List[DimensionReference]:
        """Retrieve all dimension references from the collection of entities"""
        raise NotImplementedError

    @abstractmethod
    def get_dimension(
        self, dimension_reference: DimensionReference, origin: Optional[EntityOrigin] = None
    ) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference"""
        raise NotImplementedError

    @abstractmethod
    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference"""
        raise NotImplementedError

    @property
    @abstractmethod
    def measure_references(self) -> List[MeasureReference]:
        """Return all measure references from the collection of entities"""
        raise NotImplementedError

    @property
    @abstractmethod
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:
        """Return a mapping from all semi-additive measures to their corresponding non additive dimension parameters

        This includes all measures with non-additive dimension parameters, if any, from the collection of entities.
        """
        raise NotImplementedError

    @abstractmethod
    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Retrieve the measure model object associated with the measure reference"""
        raise NotImplementedError

    @abstractmethod
    def get_identifier_references(self) -> List[IdentifierReference]:
        """Retrieve all identifier references from the collection of entities"""
        raise NotImplementedError

    @abstractmethod
    def get_entities_for_measure(self, measure_reference: MeasureReference) -> List[Entity]:
        """Retrieve a list of all entity model objects associated with the measure reference"""
        raise NotImplementedError

    @abstractmethod
    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> TimeDimensionReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference"""

    @abstractmethod
    def get_identifier_in_entity(self, ref: EntityElementReference) -> Optional[Identifier]:
        """Retrieve the identifier matching the element -> entity mapping, if any"""
        raise NotImplementedError

    @abstractmethod
    def get(self, entity_name: str) -> Optional[Entity]:
        """Retrieve the entity model object matching the given name, if any"""
        raise NotImplementedError

    @abstractmethod
    def get_by_reference(self, entity_reference: EntityReference) -> Optional[Entity]:
        """Retrieve the entity model object matching the input entity reference, if any"""
        raise NotImplementedError

    @property
    @abstractmethod
    def entity_references(self) -> Sequence[EntityReference]:
        """Return all EntityReference objects associated with the entities in the collection"""
        raise NotImplementedError

    @abstractmethod
    def get_aggregation_time_dimensions_with_measures(
        self, entity_reference: EntityReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all aggregation time dimensions in the given entity with their associated measures"""
        raise NotImplementedError

    @abstractmethod
    def get_entities_for_identifier(self, identifier_reference: IdentifierReference) -> Set[Entity]:
        """Return all entities associated with an identifier reference"""
        raise NotImplementedError


class MetricSemanticsAccessor(Protocol):
    """Protocol defining core interface for accessing semantic information about a set of metric objects

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the MetricSemantics class might implement this protocol but also include some
    public methods for adding or removing metrics from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected metrics.
    """

    @abstractmethod
    def element_specs_for_metrics(
        self,
        metric_references: List[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> List[LinkableInstanceSpec]:
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)"""
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self, metric_references: List[MetricReference]) -> List[Metric]:
        """Retrieve the Metric model objects associated with the provided metric specs"""
        raise NotImplementedError

    @property
    @abstractmethod
    def metric_references(self) -> List[MetricReference]:
        """Return the metric references"""
        raise NotImplementedError

    @abstractmethod
    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa:D
        raise NotImplementedError

    @property
    @abstractmethod
    def valid_hashes(self) -> Set[str]:
        """Return all of the hashes of the metric definitions."""
        raise NotImplementedError

    @abstractmethod
    def measures_for_metric(self, metric_reference: MetricReference) -> Tuple[MetricInputMeasureSpec, ...]:
        """Return the measure specs required to compute the metric."""
        raise NotImplementedError

    @abstractmethod
    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        raise NotImplementedError

    @abstractmethod
    def metric_input_specs_for_metric(self, metric_spec: MetricReference) -> Tuple[MetricSpec, ...]:
        """Returns the metric input specs required to compute the metric."""
        raise NotImplementedError
