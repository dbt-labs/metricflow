import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Set, Sequence

from dbt_semantic_interfaces.objects.data_source import DataSource, DataSourceOrigin
from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.user_configured_model import UserConfiguredModel
from dbt_semantic_interfaces.objects.aggregation_type import AggregationType
from dbt_semantic_interfaces.references import (
    DataSourceReference,
    DataSourceElementReference,
    MeasureReference,
    TimeDimensionReference,
    DimensionReference,
    LinkableElementReference,
    EntityReference,
)
from metricflow.errors.errors import InvalidDataSourceError
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.spec_converters import MeasureConverter
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.specs import NonAdditiveDimensionSpec, MeasureSpec

logger = logging.getLogger(__name__)


class DataSourceSemantics(DataSourceSemanticsAccessor):
    """Tracks semantic information for data source held in a set of DataSourceContainers

    This implements both the DataSourceSemanticsAccessors protocol, the interface type we use throughout the codebase.
    That interface prevents unwanted calls to methods for adding data sources to the container.
    """

    def __init__(  # noqa: D
        self,
        model: UserConfiguredModel,
    ) -> None:
        self._model = model
        self._measure_index: Dict[MeasureReference, List[DataSource]] = defaultdict(list)
        self._measure_aggs: Dict[
            MeasureReference, AggregationType
        ] = {}  # maps measures to their one consistent aggregation
        self._measure_agg_time_dimension: Dict[MeasureReference, TimeDimensionReference] = {}
        self._measure_non_additive_dimension_specs: Dict[MeasureReference, NonAdditiveDimensionSpec] = {}
        self._dimension_index: Dict[DimensionReference, List[DataSource]] = defaultdict(list)
        self._linkable_reference_index: Dict[LinkableElementReference, List[DataSource]] = defaultdict(list)
        self._entity_index: Dict[Optional[str], List[DataSource]] = defaultdict(list)
        self._entity_ref_to_entity: Dict[EntityReference, Optional[str]] = {}
        self._data_source_names: Set[str] = set()

        self._data_source_to_aggregation_time_dimensions: Dict[
            DataSourceReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        self._data_source_reference_to_data_source: Dict[DataSourceReference, DataSource] = {}
        for data_source in self._model.data_sources:
            self._add_data_source(data_source)

    def get_dimension_references(self) -> Sequence[DimensionReference]:  # noqa: D
        return tuple(self._dimension_index.keys())

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

    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieves a full dimension object by name"""
        dimension_reference = time_dimension_reference.dimension_reference()

        if dimension_reference not in self._dimension_index:
            raise ValueError(
                f"Could not find dimension with name ({dimension_reference.element_name}) in configured data sources"
            )

        for dimension_source in self._dimension_index[dimension_reference]:
            dimension = dimension_source.get_dimension(dimension_reference)
            # TODO: Unclear if the deepcopy is necessary.
            return deepcopy(dimension)

        assert False, f"{time_dimension_reference} should have been in the dimension index"

    @property
    def measure_references(self) -> Sequence[MeasureReference]:  # noqa: D
        return list(self._measure_index.keys())

    @property
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:  # noqa: D
        return self._measure_non_additive_dimension_specs

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured data sources")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across data sources, so just use the first one.
        return list(self._measure_index[measure_reference])[0].get_measure(measure_reference)

    def get_entity_references(self) -> Sequence[EntityReference]:  # noqa: D
        return list(self._entity_ref_to_entity.keys())

    # DSC interface
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> Sequence[DataSource]:  # noqa: D
        return self._measure_index[measure_reference]

    def get_agg_time_dimension_for_measure(  # noqa: D
        self, measure_reference: MeasureReference
    ) -> TimeDimensionReference:
        return self._measure_agg_time_dimension[measure_reference]

    def get_entity_in_data_source(self, ref: DataSourceElementReference) -> Optional[Entity]:  # Noqa: d
        data_source = self.get_by_reference(ref.data_source_reference)
        if not data_source:
            return None

        for identifier in data_source.identifiers:
            if identifier.reference.element_name == ref.element_name:
                return identifier

        return None

    def get_by_reference(self, data_source_reference: DataSourceReference) -> Optional[DataSource]:  # noqa: D
        return self._data_source_reference_to_data_source.get(data_source_reference)

    def _add_data_source(self, data_source: DataSource) -> None:
        """Add data source semantic information, validating consistency with existing data sources."""
        errors = []

        if data_source.reference in self._data_source_reference_to_data_source:
            errors.append(f"Data source {data_source.reference} already added.")

        for measure in data_source.measures:
            if measure.reference in self._measure_aggs and self._measure_aggs[measure.reference] != measure.agg:
                errors.append(
                    f"Conflicting aggregation (agg) for measure {measure.reference}. Currently registered as "
                    f"{self._measure_aggs[measure.reference]} but got {measure.agg}."
                )

        if len(errors) > 0:
            raise InvalidDataSourceError(f"Error adding {data_source.reference}. Got errors: {errors}")

        self._data_source_to_aggregation_time_dimensions[data_source.reference] = ElementGrouper[
            TimeDimensionReference, MeasureSpec
        ]()

        for measure in data_source.measures:
            self._measure_aggs[measure.reference] = measure.agg
            self._measure_index[measure.reference].append(data_source)
            agg_time_dimension = measure.checked_agg_time_dimension
            self._data_source_to_aggregation_time_dimensions[data_source.reference].add_value(
                key=agg_time_dimension,
                value=MeasureConverter.convert_to_measure_spec(measure=measure),
            )
            self._measure_agg_time_dimension[measure.reference] = agg_time_dimension
            if measure.non_additive_dimension:
                non_additive_dimension_spec = NonAdditiveDimensionSpec(
                    name=measure.non_additive_dimension.name,
                    window_choice=measure.non_additive_dimension.window_choice,
                    window_groupings=tuple(measure.non_additive_dimension.window_groupings),
                )
                self._measure_non_additive_dimension_specs[measure.reference] = non_additive_dimension_spec
        for dim in data_source.dimensions:
            self._linkable_reference_index[dim.reference].append(data_source)
            self._dimension_index[dim.reference].append(data_source)
        for ident in data_source.identifiers:
            self._entity_ref_to_entity[ident.reference] = ident.name
            self._entity_index[ident.name].append(data_source)
            self._linkable_reference_index[ident.reference].append(data_source)

        self._data_source_reference_to_data_source[data_source.reference] = data_source

    @property
    def data_source_references(self) -> Sequence[DataSourceReference]:  # noqa: D
        data_source_names_sorted = sorted(self._data_source_names)
        return tuple(DataSourceReference(data_source_name=x) for x in data_source_names_sorted)

    def get_aggregation_time_dimensions_with_measures(
        self, data_source_reference: DataSourceReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all time dimensions in a data source with their associated measures."""
        assert (
            data_source_reference in self._data_source_to_aggregation_time_dimensions
        ), f"Data Source {data_source_reference} is not known"
        return self._data_source_to_aggregation_time_dimensions[data_source_reference]

    def get_data_sources_for_entity(self, entity_reference: EntityReference) -> Set[DataSource]:
        """Return all data sources associated with an identifier reference"""
        entity = self._entity_ref_to_entity[entity_reference]
        return set(self._entity_index[entity])
