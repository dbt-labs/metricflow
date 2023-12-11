"""Protocols for classes meant to manage semantic information of useful object types.

These are useful as more generic descriptors of interfaces around classes like semantic containers and such, which
might be best used in a read-only mode. These containers could also pull in extra dependencies in their internal
implementations, dependencies which are best isolated inside the packages containing the concrete objects rather
than the interface specifications.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, FrozenSet, Optional, Sequence, Set

from dbt_semantic_interfaces.protocols.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.metric import Metric, MetricInputMeasure
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MeasureReference,
    MetricReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)

from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.specs.specs import (
    LinkableInstanceSpec,
    MeasureSpec,
    NonAdditiveDimensionSpec,
)


class SemanticModelAccessor(ABC):
    """Interface for accessing semantic information about a set of semantic model objects.

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the SemanticModelLookup class might implement this protocol but also include some
    public methods for adding or removing semantic models from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected semantic models.
    """

    @abstractmethod
    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of semantic models."""
        raise NotImplementedError

    @abstractmethod
    def get_dimension(self, dimension_reference: DimensionReference) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference."""
        raise NotImplementedError

    @abstractmethod
    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference."""
        raise NotImplementedError

    @property
    @abstractmethod
    def measure_references(self) -> Sequence[MeasureReference]:
        """Return all measure references from the collection of semantic models."""
        raise NotImplementedError

    @property
    @abstractmethod
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:
        """Return a mapping from all semi-additive measures to their corresponding non additive dimension parameters.

        This includes all measures with non-additive dimension parameters, if any, from the collection of semantic models.
        """
        raise NotImplementedError

    @abstractmethod
    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Retrieve the measure model object associated with the measure reference."""
        raise NotImplementedError

    @abstractmethod
    def get_entity_references(self) -> Sequence[EntityReference]:
        """Retrieve all entity references from the collection of semantic models."""
        raise NotImplementedError

    @abstractmethod
    def get_semantic_models_for_measure(self, measure_reference: MeasureReference) -> Sequence[SemanticModel]:
        """Retrieve a list of all semantic model model objects associated with the measure reference."""
        raise NotImplementedError

    @abstractmethod
    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> TimeDimensionReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference."""

    @abstractmethod
    def get_entity_in_semantic_model(self, ref: SemanticModelElementReference) -> Optional[Entity]:
        """Retrieve the entity matching the element -> semantic model mapping, if any."""
        raise NotImplementedError

    @abstractmethod
    def get_by_reference(self, semantic_model_reference: SemanticModelReference) -> Optional[SemanticModel]:
        """Retrieve the semantic model object matching the input semantic model reference, if any."""
        raise NotImplementedError

    @property
    @abstractmethod
    def semantic_model_references(self) -> Sequence[SemanticModelReference]:
        """Return all SemanticModelReference objects associated with the semantic models in the collection."""
        raise NotImplementedError

    @abstractmethod
    def get_aggregation_time_dimensions_with_measures(
        self, semantic_model_reference: SemanticModelReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all aggregation time dimensions in the given semantic model with their associated measures."""
        raise NotImplementedError

    @abstractmethod
    def get_semantic_models_for_entity(self, entity_reference: EntityReference) -> Set[SemanticModel]:
        """Return all semantic models associated with an entity reference."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def entity_links_for_local_elements(semantic_model: SemanticModel) -> Sequence[EntityReference]:
        """Return the entity prefix that can be used to access dimensions defined in the semantic model."""
        raise NotImplementedError

    @abstractmethod
    def get_element_spec_for_name(self, element_name: str) -> LinkableInstanceSpec:
        """Returns the spec for the given name of a linkable element (dimension or entity)."""
        raise NotImplementedError


class MetricAccessor(ABC):
    """Interface for accessing semantic information about a set of metric objects.

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the MetricLookup class might implement this protocol but also include some
    public methods for adding or removing metrics from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected metrics.
    """

    @abstractmethod
    def element_specs_for_metrics(
        self,
        metric_references: Sequence[MetricReference],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> Sequence[LinkableInstanceSpec]:
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)."""
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self, metric_references: Sequence[MetricReference]) -> Sequence[Metric]:
        """Retrieve the Metric model objects associated with the provided metric specs."""
        raise NotImplementedError

    @property
    @abstractmethod
    def metric_references(self) -> Sequence[MetricReference]:
        """Return the metric references."""
        raise NotImplementedError

    @abstractmethod
    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa:D
        raise NotImplementedError

    @abstractmethod
    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        raise NotImplementedError

    @abstractmethod
    def configured_input_measure_for_metric(self, metric_reference: MetricReference) -> Optional[MetricInputMeasure]:
        """Get input measure defined in the original metric config, if exists.

        When SemanticModel is constructed, input measures from input metrics are added to the list of input measures
        for a metric. Here, use rules about metric types to determine which input measures were defined in the config:
        - Simple & cumulative metrics require one input measure, and can't take any input metrics.
        - Derived & ratio metrics take no input measures, only input metrics.
        """
        raise NotImplementedError

    @abstractmethod
    def group_by_item_specs_for_measure(
        self,
        measure_reference: MeasureReference,
        with_any_of: Optional[Set[LinkableElementProperties]] = None,
        without_any_of: Optional[Set[LinkableElementProperties]] = None,
    ) -> Sequence[LinkableInstanceSpec]:
        """Return group-by-items that are possible for a measure."""
        raise NotImplementedError

    @abstractmethod
    def group_by_item_specs_for_no_metrics_query(
        self,
        with_any_of: Optional[Set[LinkableElementProperties]] = None,
        without_any_of: Optional[Set[LinkableElementProperties]] = None,
    ) -> Sequence[LinkableInstanceSpec]:
        """Return the possible group-by-items for a dimension values query with no metrics."""
        raise NotImplementedError
