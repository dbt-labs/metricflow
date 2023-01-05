import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Set, Sequence

from metricflow.aggregation_properties import AggregationType
from metricflow.errors.errors import InvalidDataSourceError
from metricflow.instances import DataSourceReference, DataSourceElementReference
from metricflow.model.objects.data_source import DataSource, DataSourceOrigin
from metricflow.model.objects.elements.dimension import Dimension
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_container import PydanticDataSourceContainer
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.spec_converters import MeasureConverter
from metricflow.references import (
    MeasureReference,
    TimeDimensionReference,
    DimensionReference,
    LinkableElementReference,
    IdentifierReference,
)
from metricflow.specs import NonAdditiveDimensionSpec, MeasureSpec

logger = logging.getLogger(__name__)


class DataSourceSemantics:
    """Tracks semantic information for data source held in a set of DataSourceContainers

    This implements both the DataSourceSemanticsAccessors protocol, the interface type we use throughout the codebase.
    That interface prevents unwanted calls to methods for adding data sources to the container.
    """

    def __init__(  # noqa: D
        self,
        model: UserConfiguredModel,
        configured_data_source_container: PydanticDataSourceContainer,
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
        self._identifier_ref_to_entity: Dict[IdentifierReference, Optional[str]] = {}
        self._data_source_names: Set[str] = set()

        self._configured_data_source_container = configured_data_source_container
        self._data_source_to_aggregation_time_dimensions: Dict[
            DataSourceReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        # Add semantic tracking for data sources from configured_data_source_container
        for data_source in self._configured_data_source_container.values():
            assert isinstance(data_source, DataSource)
            self.add_configured_data_source(data_source)

    def add_configured_data_source(self, data_source: DataSource) -> None:
        """Dont use this unless you mean it (ie in tests). The configured data sources are supposed to be static"""
        self._configured_data_source_container._put(data_source)
        self._add_data_source(data_source)

    def get_dimension_references(self) -> List[DimensionReference]:  # noqa: D
        return list(self._dimension_index.keys())

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
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
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

    def get_identifier_references(self) -> List[IdentifierReference]:  # noqa: D
        return list(self._identifier_ref_to_entity.keys())

    # DSC interface
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> List[DataSource]:  # noqa: D
        return self._measure_index[measure_reference]

    def get_agg_time_dimension_for_measure(  # noqa: D
        self, measure_reference: MeasureReference
    ) -> TimeDimensionReference:
        return self._measure_agg_time_dimension[measure_reference]

    def get_identifier_in_data_source(self, ref: DataSourceElementReference) -> Optional[Identifier]:  # Noqa: d
        data_source = self.get(ref.data_source_name)
        if not data_source:
            return None

        for identifier in data_source.identifiers:
            if identifier.reference.element_name == ref.element_name:
                return identifier

        return None

    def get(self, data_source_name: str) -> Optional[DataSource]:  # noqa: D
        if data_source_name in self._configured_data_source_container:
            data_source = self._configured_data_source_container.get(data_source_name)
            assert isinstance(data_source, DataSource)
            return data_source

        return None

    def get_by_reference(self, data_source_reference: DataSourceReference) -> Optional[DataSource]:  # noqa: D
        return self.get(data_source_reference.data_source_name)

    def _add_data_source(
        self,
        data_source: DataSource,
        fail_on_error: bool = True,
        logging_context: str = "",
    ) -> None:
        """Add data source semantic information, validating consistency with existing data sources."""
        errors = []

        if data_source.name in self._data_source_names:
            errors.append(f"name {data_source.name} already registered - please ensure data source names are unique")

        for measure in data_source.measures:
            if measure.reference in self._measure_aggs and self._measure_aggs[measure.reference] != measure.agg:
                errors.append(
                    f"conflicting aggregation (agg) for measure `{measure.reference.element_name}` registered as "
                    f"`{self._measure_aggs[measure.reference]}`; Got `{measure.agg}"
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
            self._identifier_ref_to_entity[ident.reference] = ident.entity
            self._entity_index[ident.entity].append(data_source)
            self._linkable_reference_index[ident.reference].append(data_source)

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

    def get_data_sources_for_identifier(self, identifier_reference: IdentifierReference) -> Set[DataSource]:
        """Return all data sources associated with an identifier reference"""
        identifier_entity = self._identifier_ref_to_entity[identifier_reference]
        return set(self._entity_index[identifier_entity])
