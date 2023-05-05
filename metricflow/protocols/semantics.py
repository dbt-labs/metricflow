"""Protocols for classes meant to manage semantic information of useful object types.

These are useful as more generic descriptors of interfaces around classes like semantic containers and such, which
might be best used in a read-only mode. These containers could also pull in extra dependencies in their internal
implementations, dependencies which are best isolated inside the packages containing the concrete objects rather
than the interface specifications.
"""

from __future__ import annotations

from abc import abstractmethod, ABC
from typing import Dict, FrozenSet, Optional, Sequence, Set

from dbt_semantic_interfaces.objects.data_source import DataSource, DataSourceOrigin
from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.references import (
    DataSourceElementReference,
    DataSourceReference,
    DimensionReference,
    EntityReference,
    MeasureReference,
    TimeDimensionReference,
    MetricReference,
)
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.specs import (
    LinkableInstanceSpec,
    MeasureSpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
    ColumnAssociationResolver,
)


class DataSourceSemanticsAccessor(ABC):
    """Interface for accessing semantic information about a set of data source objects

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the DataSourceSemantics class might implement this protocol but also include some
    public methods for adding or removing data sources from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected data sources.
    """

    @abstractmethod
    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of data sources"""
        raise NotImplementedError

    @abstractmethod
    def get_dimension(
        self, dimension_reference: DimensionReference, origin: Optional[DataSourceOrigin] = None
    ) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference"""
        raise NotImplementedError

    @abstractmethod
    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieve the dimension model object associated with the time dimension reference"""
        raise NotImplementedError

    @property
    @abstractmethod
    def measure_references(self) -> Sequence[MeasureReference]:
        """Return all measure references from the collection of data sources"""
        raise NotImplementedError

    @property
    @abstractmethod
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:
        """Return a mapping from all semi-additive measures to their corresponding non additive dimension parameters

        This includes all measures with non-additive dimension parameters, if any, from the collection of data sources.
        """
        raise NotImplementedError

    @abstractmethod
    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Retrieve the measure model object associated with the measure reference"""
        raise NotImplementedError

    @abstractmethod
    def get_entity_references(self) -> Sequence[EntityReference]:
        """Retrieve all entity references from the collection of data sources"""
        raise NotImplementedError

    @abstractmethod
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> Sequence[DataSource]:
        """Retrieve a list of all data source model objects associated with the measure reference"""
        raise NotImplementedError

    @abstractmethod
    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> TimeDimensionReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference"""

    @abstractmethod
    def get_entity_in_data_source(self, ref: DataSourceElementReference) -> Optional[Entity]:
        """Retrieve the entity matching the element -> data source mapping, if any"""
        raise NotImplementedError

    @abstractmethod
    def get_by_reference(self, data_source_reference: DataSourceReference) -> Optional[DataSource]:
        """Retrieve the data source model object matching the input data source reference, if any"""
        raise NotImplementedError

    @property
    @abstractmethod
    def data_source_references(self) -> Sequence[DataSourceReference]:
        """Return all DataSourceReference objects associated with the data sources in the collection"""
        raise NotImplementedError

    @abstractmethod
    def get_aggregation_time_dimensions_with_measures(
        self, data_source_reference: DataSourceReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all aggregation time dimensions in the given data source with their associated measures"""
        raise NotImplementedError

    @abstractmethod
    def get_data_sources_for_entity(self, entity_reference: EntityReference) -> Set[DataSource]:
        """Return all data sources associated with an entity reference"""
        raise NotImplementedError


class MetricSemanticsAccessor(ABC):
    """Interface for accessing semantic information about a set of metric objects

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the MetricSemantics class might implement this protocol but also include some
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
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)"""
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self, metric_references: Sequence[MetricReference]) -> Sequence[Metric]:
        """Retrieve the Metric model objects associated with the provided metric specs"""
        raise NotImplementedError

    @property
    @abstractmethod
    def metric_references(self) -> Sequence[MetricReference]:
        """Return the metric references"""
        raise NotImplementedError

    @abstractmethod
    def get_metric(self, metric_reference: MetricReference) -> Metric:  # noqa:D
        raise NotImplementedError

    @abstractmethod
    def measures_for_metric(
        self,
        metric_reference: MetricReference,
        column_association_resolver: ColumnAssociationResolver,
    ) -> Sequence[MetricInputMeasureSpec]:
        """Return the measure specs required to compute the metric."""
        raise NotImplementedError

    @abstractmethod
    def contains_cumulative_or_time_offset_metric(self, metric_references: Sequence[MetricReference]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric or a derived metric with time offset."""
        raise NotImplementedError

    @abstractmethod
    def metric_input_specs_for_metric(
        self,
        metric_reference: MetricReference,
        column_association_resolver: ColumnAssociationResolver,
    ) -> Sequence[MetricSpec]:
        """Returns the metric input specs required to compute the metric."""
        raise NotImplementedError
