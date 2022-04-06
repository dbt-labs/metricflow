from __future__ import annotations

import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Set, Type, Optional, Sequence, Tuple

from metricflow.errors.errors import (
    DuplicateMetricError,
    MetricNotFoundError,
    NonExistentMeasureError,
    InvalidDataSourceError,
)
from metricflow.instances import DataSourceReference, DataSourceElementReference
from metricflow.model.graph import MultiDiGraph
from metricflow.model.objects.common import Element
from metricflow.model.objects.data_source import DataSource, DataSourceOrigin
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import Metric, MetricType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_container import PydanticDataSourceContainer
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
    LinkableElementProperties,
)
from metricflow.model.semantics.links_new import JoinLink, JOIN_TYPE_MAPPING
from metricflow.specs import (
    LinkableInstanceSpec,
    LinkableElementReference,
    MeasureSpec,
    MeasureReference,
    DimensionReference,
    IdentifierReference,
    MetricSpec,
    TimeDimensionReference,
    LinkableSpecSet,
)

logger = logging.getLogger(__name__)

MAX_JOIN_HOPS = 2


class MetricSemantics:  # noqa: D
    def __init__(  # noqa: D
        self, user_configured_model: UserConfiguredModel, data_source_semantics: DataSourceSemantics
    ) -> None:
        self._user_configured_model = user_configured_model
        self._metrics: Dict[MetricSpec, Metric] = {}
        self._data_source_semantics = data_source_semantics

        # Dict from the name of the metric to the hash.
        self._metric_hashes: Dict[MetricSpec, str] = {}

        for metric in self._user_configured_model.metrics:
            self.add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            user_configured_model=self._user_configured_model,
            primary_time_dimension_reference=self._data_source_semantics.primary_time_dimension_reference,
            max_identifier_links=MAX_JOIN_HOPS,
        )

    def element_specs_for_metrics(
        self,
        metric_specs: List[MetricSpec],
        local_only: bool = False,
        dimensions_only: bool = False,
        exclude_multi_hop: bool = False,
        exclude_derived_time_granularities: bool = True,
        exclude_local_linked_primary_time: bool = True,
    ) -> List[LinkableInstanceSpec]:
        """Dimensions common to all metrics requested (intersection)"""
        without_any_property = set()

        if exclude_local_linked_primary_time:
            without_any_property.add(LinkableElementProperties.LOCAL_LINKED_PRIMARY_TIME)

        if exclude_derived_time_granularities:
            without_any_property.add(LinkableElementProperties.DERIVED_TIME_GRANULARITY)

        if local_only:
            with_any_property = frozenset({LinkableElementProperties.LOCAL})
        else:
            with_any_property = LinkableElementProperties.all_properties()

        if exclude_multi_hop:
            without_any_property.add(LinkableElementProperties.MULTI_HOP)

        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_specs=metric_specs,
            with_any_of=with_any_property,
            without_any_of=frozenset(without_any_property),
        ).as_spec_set

        if dimensions_only:
            all_linkable_specs = LinkableSpecSet(
                dimension_specs=all_linkable_specs.dimension_specs,
                time_dimension_specs=all_linkable_specs.time_dimension_specs,
            )

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

    def get_metrics(self, metric_names: List[MetricSpec]) -> List[Metric]:  # noqa: D
        res = []
        for metric_name in metric_names:
            if metric_name not in self._metrics:
                raise MetricNotFoundError(f"Unable to find metric `{metric_name}`. Perhaps it has not been registered")
            res.append(self._metrics[metric_name])

        return res

    @property
    def metric_names(self) -> List[MetricSpec]:  # noqa: D
        return list(self._metrics.keys())

    def get_metric(self, metric_name: MetricSpec) -> Metric:  # noqa:D
        if metric_name not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_name}`. Perhaps it has not been registered")
        return self._metrics[metric_name]

    def add_metric(self, metric: Metric) -> None:
        """Add metric, validating presence of required measures"""
        metric_spec = MetricSpec(element_name=metric.name)
        if metric_spec in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_names:
            if measure_reference not in self._data_source_semantics.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_spec] = metric
        self._metric_hashes[metric_spec] = metric.definition_hash

    @property
    def valid_hashes(self) -> Set[str]:
        """Return all of the hashes of the metric definitions."""
        return set(self._metric_hashes.values())

    def measures_for_metric(self, metric_spec: MetricSpec) -> Tuple[MeasureSpec, ...]:
        """Return the measure specs required to compute the metric."""
        metric = self.get_metric(metric_spec)

        return tuple(
            MeasureSpec(
                element_name=x.element_name,
            )
            for x in metric.measure_names
        )

    def contains_cumulative_metric(self, metric_specs: Sequence[MetricSpec]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric."""
        for metric_spec in metric_specs:
            if self.get_metric(metric_spec).type == MetricType.CUMULATIVE:
                return True
        return False


class DataSourceSemantics:
    """Tracks semantic information for data source held in a set of DataSourceContainers"""

    def __init__(  # noqa: D
        self,
        model: UserConfiguredModel,
        configured_data_source_container: PydanticDataSourceContainer,
    ) -> None:
        self._model = model
        self._element_types: Dict[LinkableElementReference, Type[Element]] = {}
        self._measure_index: Dict[MeasureReference, List[DataSource]] = defaultdict(list)
        self._measure_aggs: Dict[
            MeasureReference, AggregationType
        ] = {}  # maps measures to their one consistent aggregation
        self._dimension_index: Dict[DimensionReference, List[DataSource]] = defaultdict(list)
        self._linkable_reference_index: Dict[LinkableElementReference, List[DataSource]] = defaultdict(list)
        self._entity_index: Dict[Optional[str], List[DataSource]] = defaultdict(list)
        self._identifier_ref_to_entity: Dict[IdentifierReference, Optional[str]] = {}
        # Graph where nodes are DataSource names and edges are Join objects that represent all valid
        # joins possible between the nodes (data sources)
        self._data_source_links = MultiDiGraph[str, JoinLink]()

        self._data_source_names: Set[str] = set()

        self._configured_data_source_container = configured_data_source_container

        # Add semantic tracking for data sources from configured_data_source_container
        for data_source in self._configured_data_source_container.values():
            assert isinstance(data_source, DataSource)
            self.add_configured_data_source(data_source)

    @property
    def data_sources(self) -> Sequence[DataSource]:  # noqa: D
        return self._model.data_sources

    def add_configured_data_source(self, data_source: DataSource) -> None:
        """Dont use this unless you mean it (ie in tests). The configured data sources are supposed to be static"""
        self._configured_data_source_container._put(data_source)
        self.add_data_source(data_source)

    def get_linkable_element_references(self) -> List[LinkableElementReference]:  # noqa: D
        return list(self._linkable_reference_index.keys())

    def get_dimension_references(self) -> List[DimensionReference]:  # noqa: D
        return list(self._dimension_index.keys())

    def get_linkable(  # noqa: D
        self,
        linkable_name: LinkableElementReference,
        origin: Optional[DataSourceOrigin] = None,
        use_identifier_dundered_name: bool = False,
    ) -> Element:
        """Retrieves a full linkable element object by name

        use_identifier_dundered_name returns the Dimension with the identifier-dundered name (if the requested linkable element
        has an identifier-dunder) otherwise the identifier is stripped from the name
        """
        for dimension_source in self._linkable_reference_index[linkable_name]:
            if origin and dimension_source.origin != origin:
                continue
            linkable_element = dimension_source.get_element(linkable_name)
            # find the data source that has the requested linkable_element by the requested identifier

            return deepcopy(linkable_element)

        raise ValueError(
            f"Could not find dimension with name ({linkable_name.element_name}) in configured data sources"
        )

    def get_dimension(
        self, dimension_reference: DimensionReference, origin: Optional[DataSourceOrigin] = None
    ) -> Dimension:
        """Retrieves a full dimension object by name"""
        for dimension_source in self._dimension_index[dimension_reference]:
            if origin and dimension_source.origin != origin:
                continue
            dimension = dimension_source.get_dimension(dimension_reference)
            # find the data source that has the requested dimension by the requested identifier

            return deepcopy(dimension)

        raise ValueError(
            f"Could not find dimension with name ({dimension_reference.element_name}) in configured data sources"
        )

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
        return list(self._measure_index.keys())

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured data sources")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across data sources, so just use the first one.
        return list(self._measure_index[measure_reference])[0].get_measure(measure_reference)

    def get_identifier_references(self) -> List[IdentifierReference]:  # noqa: D
        return list(self._identifier_ref_to_entity.keys())

    def _get_joinable_data_sources_by_identifier(
        self, data_source_name: str, identifier_spec: LinkableInstanceSpec
    ) -> List[str]:
        """Returns list of data sources that can be joined to `data_source_name` via `identifier_spec`"""
        res = []
        for name, links in self._data_source_links.adj(data_source_name).items():
            for link in links:
                if link.via_from.name.element_name == identifier_spec.element_name:
                    res.append(name)
                    break

        return res

    def _get_joinable_data_sources(self, data_source_name: str) -> Dict[str, List[JoinLink]]:  # noqa: D
        return self._data_source_links.adj(data_source_name)

    # DSC interface
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> List[DataSource]:  # noqa: D
        return self._measure_index[measure_reference]

    def dimension_is_partitioned(self, dimension_reference: DimensionReference) -> bool:  # noqa: D
        return self.get_dimension(dimension_reference).is_partition

    def keys(self) -> List[str]:  # noqa: D
        return list(self._data_source_names)

    def get_data_source_element(self, ref: DataSourceElementReference) -> Optional[Element]:  # Noqa: d
        data_source = self[ref.data_source_name]
        for elem in data_source.elements:
            if elem.name.element_name == ref.element_name:
                return elem

        return None

    def __getitem__(self, item: str) -> DataSource:  # noqa: D
        res = self.get(item)
        if res:
            return res

        raise ValueError(f"Cannot find data source with name ({item})")

    def get(self, data_source_name: str) -> Optional[DataSource]:  # noqa: D
        if data_source_name in self._configured_data_source_container:
            data_source = self._configured_data_source_container.get(data_source_name)
            assert isinstance(data_source, DataSource)
            return data_source

        return None

    def get_by_reference(self, data_source_reference: DataSourceReference) -> Optional[DataSource]:  # noqa: D
        return self.get(data_source_reference.data_source_name)

    def add_data_source(
        self,
        data_source: DataSource,
        fail_on_error: bool = True,
        logging_context: str = "",
    ) -> None:
        """Add data source semantic information, validating consistency with existing data sources and establishing links"""
        errors = []

        if data_source.name in self._data_source_names:
            errors.append(f"name {data_source.name} already registered - please ensure data source names are unique")

        if data_source.origin is DataSourceOrigin.DERIVED:
            raise ValueError(
                f"Cannot add derived data source (with name: {data_source.name}) to New DataSourceSemantics... for now"
            )

        for measure in data_source.measures:
            if measure.name in self._measure_aggs and self._measure_aggs[measure.name] != measure.agg:
                errors.append(
                    f"conflicting aggregation (agg) for measure `{measure.name}` registered as `{self._measure_aggs[measure.name]}`; "
                    f"Got `{measure.agg}"
                )

        if errors:
            error_prefix = "\n  - "
            error_msg = (
                f"Unable to add data source `{data_source.name}` "
                f"{'while ' + logging_context + ' ' if logging_context else ''}"
                f"{'... skipping' if not fail_on_error else ''}.\n"
                f"Errors: {error_prefix + error_prefix.join(errors)}"
            )
            if fail_on_error:
                raise InvalidDataSourceError(error_msg)
            logger.warning(error_msg)
            return

        self._data_source_names.add(data_source.name)

        for elem in data_source.elements:
            if elem.name not in self._element_types:
                self._element_types[elem.name] = type(elem)

        for meas in data_source.measures:
            self._measure_aggs[meas.name] = meas.agg
            self._measure_index[meas.name].append(data_source)
        for dim in data_source.dimensions:
            self._linkable_reference_index[dim.name].append(data_source)
            self._dimension_index[dim.name].append(data_source)
        for ident in data_source.identifiers:
            self._identifier_ref_to_entity[ident.name] = ident.entity
            self._entity_index[ident.entity].append(data_source)
            self._linkable_reference_index[ident.name].append(data_source)

        dsource_partition = data_source.partition
        # Add links to other data sources based on the names and types of identifiers available.
        for ident in data_source.identifiers:
            # We only need to consider other data sources with the same identifier.
            for other in self._entity_index[ident.entity]:
                if other.name == data_source.name:
                    continue

                # handle whether or not this join is partitioned
                partitions: List[DimensionReference] = list()
                other_partition = other.partition
                if dsource_partition and other_partition and dsource_partition.name != other_partition.name:
                    continue
                if dsource_partition and other_partition:
                    partitions.append(dsource_partition.name)

                for other_ident in other.identifiers:
                    # TODO: Replace with entity check when entities are supported.
                    if other_ident.name != ident.name:
                        continue

                    # add edge data_source -> other "other is joinable to data_source"
                    join_type = JOIN_TYPE_MAPPING.get((ident.type, other_ident.type), None)
                    if join_type:
                        self._data_source_links.add_edge(
                            from_key=data_source.name,
                            to_key=other.name,
                            value=JoinLink(
                                join_type=join_type,
                                via_from=ident,
                                via_to=other_ident,
                                partitions=tuple([str(p) for p in partitions]),
                            ),
                        )

                    # add edge other -> data_source "data_source is joinable to other"
                    reversed_join_type = JOIN_TYPE_MAPPING.get((other_ident.type, ident.type), None)
                    if reversed_join_type:
                        self._data_source_links.add_edge(
                            from_key=other.name,
                            to_key=data_source.name,
                            value=JoinLink(
                                join_type=reversed_join_type,
                                via_from=other_ident,
                                via_to=ident,
                                partitions=tuple([str(p) for p in partitions]),
                            ),
                        )

    @property
    def primary_time_dimension_reference(self) -> TimeDimensionReference:
        """Gets the primary time dimension that's used in all data sources."""
        for data_source in self._configured_data_source_container.values():
            for dimension in data_source.dimensions:
                if dimension.type == DimensionType.TIME and dimension.type_params and dimension.type_params.is_primary:
                    return TimeDimensionReference(element_name=dimension.name.element_name)
        raise RuntimeError("No primary time dimension found in the model")

    @property
    def data_source_references(self) -> Sequence[DataSourceReference]:  # noqa: D
        data_source_names_sorted = sorted(self._data_source_names)
        return tuple(DataSourceReference(data_source_name=x) for x in data_source_names_sorted)
