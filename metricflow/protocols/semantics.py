"""Protocols for classes meant to manage semantic information of useful object types.

These are useful as more generic descriptors of interfaces around classes like semantic containers and such, which
might be best used in a read-only mode. These containers could also pull in extra dependencies in their internal
implementations, dependencies which are best isolated inside the packages containing the concrete objects rather
than the interface specifications.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Dict, FrozenSet, List, Optional, Protocol, Sequence, Set, Tuple

from metricflow.instances import DataSourceElementReference, DataSourceReference
from metricflow.model.objects.data_source import DataSource, DataSourceOrigin
from metricflow.model.objects.elements.dimension import Dimension
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.metric import Metric
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.semantics.linkable_spec_resolver import LinkableElementProperties
from metricflow.references import DimensionReference, IdentifierReference, MeasureReference, TimeDimensionReference
from metricflow.specs import (
    LinkableInstanceSpec,
    MeasureSpec,
    MetricInputMeasureSpec,
    MetricSpec,
    NonAdditiveDimensionSpec,
)


class DataSourceSemanticsAccessor(Protocol):
    """Protocol defining core interface for accessing semantic information about a set of data source objects

    This is primarily useful for restricting caller access to the subset of container methods and imports we want
    them to use. For example, the DataSourceSemantics class might implement this protocol but also include some
    public methods for adding or removing data sources from the container, while this protocol only allows the
    caller to invoke the accessor methods which retrieve semantic information about the collected data sources.
    """

    @abstractmethod
    def get_dimension_references(self) -> List[DimensionReference]:
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
    def measure_references(self) -> List[MeasureReference]:
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
    def get_identifier_references(self) -> List[IdentifierReference]:
        """Retrieve all identifier references from the collection of data sources"""
        raise NotImplementedError

    @abstractmethod
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> List[DataSource]:
        """Retrieve a list of all data source model objects associated with the measure reference"""
        raise NotImplementedError

    @abstractmethod
    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> TimeDimensionReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference"""

    @abstractmethod
    def get_identifier_in_data_source(self, ref: DataSourceElementReference) -> Optional[Identifier]:
        """Retrieve the identifier matching the element -> data source mapping, if any"""
        raise NotImplementedError

    @abstractmethod
    def get(self, data_source_name: str) -> Optional[DataSource]:
        """Retrieve the data source model object matching the given name, if any"""
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
    def get_data_sources_for_identifier(self, identifier_reference: IdentifierReference) -> Set[DataSource]:
        """Return all data sources associated with an identifier reference"""
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
        metric_specs: List[MetricSpec],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> List[LinkableInstanceSpec]:
        """Retrieve the matching set of linkable elements common to all metrics requested (intersection)"""
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self, metric_names: List[MetricSpec]) -> List[Metric]:
        """Retrieve the Metric model objects associated with the provided metric specs"""
        raise NotImplementedError

    @property
    @abstractmethod
    def metric_names(self) -> List[MetricSpec]:
        """Return the metric specs"""
        raise NotImplementedError

    @abstractmethod
    def get_metric(self, metric_name: MetricSpec) -> Metric:  # noqa:D
        raise NotImplementedError

    @property
    @abstractmethod
    def valid_hashes(self) -> Set[str]:
        """Return all of the hashes of the metric definitions."""
        raise NotImplementedError

    @abstractmethod
    def measures_for_metric(self, metric_spec: MetricSpec) -> Tuple[MetricInputMeasureSpec, ...]:
        """Return the measure specs required to compute the metric."""
        raise NotImplementedError

    @abstractmethod
    def contains_cumulative_metric(self, metric_specs: Sequence[MetricSpec]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric."""
        raise NotImplementedError
