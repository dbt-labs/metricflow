from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence, Set

from dbt_semantic_interfaces.protocols.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    MeasureReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import AggregationType, DimensionType, TimeGranularity

from metricflow_semantics.errors.error_classes import InvalidSemanticModelError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.dimension_lookup import DimensionLookup
from metricflow_semantics.model.semantics.element_group import ElementGrouper
from metricflow_semantics.model.semantics.measure_lookup import MeasureLookup
from metricflow_semantics.model.semantics.semantic_model_helper import SemanticModelHelper
from metricflow_semantics.model.spec_converters import MeasureConverter
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.measure_spec import MeasureSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.time_dimension_spec import DEFAULT_TIME_GRANULARITY, TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

logger = logging.getLogger(__name__)


class SemanticModelLookup:
    """Tracks semantic information for semantic models held in a set of SemanticModelContainers."""

    def __init__(self, model: SemanticManifest, custom_granularities: Dict[str, ExpandedTimeGranularity]) -> None:
        """Initializer.

        Args:
            model: the semantic manifest used for loading semantic model definitions
        """
        self._custom_granularities = custom_granularities
        self._measure_index: Dict[MeasureReference, SemanticModel] = {}
        self._measure_aggs: Dict[MeasureReference, AggregationType] = {}
        self._measure_agg_time_dimension: Dict[MeasureReference, TimeDimensionReference] = {}
        self._measure_non_additive_dimension_specs: Dict[MeasureReference, NonAdditiveDimensionSpec] = {}
        self._dimension_index: Dict[DimensionReference, List[SemanticModel]] = {}
        self._entity_index: Dict[EntityReference, List[SemanticModel]] = {}

        self._dimension_ref_to_spec: Dict[DimensionReference, DimensionSpec] = {}
        self._entity_ref_to_spec: Dict[EntityReference, EntitySpec] = {}

        self._semantic_model_to_aggregation_time_dimensions: Dict[
            SemanticModelReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        sorted_semantic_models = sorted(model.semantic_models, key=lambda semantic_model: semantic_model.name)
        for semantic_model in sorted_semantic_models:
            self._add_semantic_model(semantic_model)

        # Cache for defined time granularity.
        self._time_dimension_to_defined_time_granularity: Dict[TimeDimensionReference, TimeGranularity] = {}

        # Cache for agg. time dimension for measure.
        self._measure_reference_to_agg_time_dimension_specs: Dict[MeasureReference, Sequence[TimeDimensionSpec]] = {}

        self._measure_lookup = MeasureLookup(sorted_semantic_models, custom_granularities)
        self._dimension_lookup = DimensionLookup(sorted_semantic_models)

    @property
    def custom_granularity_names(self) -> Sequence[str]:
        """Returns all the custom_granularity names."""
        return list(self._custom_granularities.keys())

    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of semantic models."""
        return tuple(self._dimension_index.keys())

    def get_dimension(self, dimension_reference: DimensionReference) -> Dimension:
        """Retrieves a full dimension object by name."""
        # If the reference passed is a TimeDimensionReference, convert to DimensionReference.
        dimension_reference = DimensionReference(dimension_reference.element_name)

        semantic_models = self._dimension_index.get(dimension_reference)
        if not semantic_models:
            raise ValueError(
                f"Could not find dimension with name '{dimension_reference.element_name}' in configured semantic models"
            )

        return SemanticModelHelper.get_dimension_from_semantic_model(
            # Dimension object should match across semantic models, so just use the first semantic model.
            semantic_model=semantic_models[0],
            dimension_reference=dimension_reference,
        )

    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieves a full dimension object by name."""
        return self.get_dimension(dimension_reference=time_dimension_reference.dimension_reference)

    @property
    def measure_references(self) -> Sequence[MeasureReference]:
        """Return all measure references from the collection of semantic models."""
        return list(self._measure_index.keys())

    @property
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:
        """Return a mapping from all semi-additive measures to their corresponding non additive dimension parameters.

        This includes all measures with non-additive dimension parameters, if any, from the collection of semantic models.
        """
        return self._measure_non_additive_dimension_specs

    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Retrieve the measure model object associated with the measure reference."""
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured semantic models")

        return SemanticModelHelper.get_measure_from_semantic_model(
            semantic_model=self.get_semantic_model_for_measure(measure_reference), measure_reference=measure_reference
        )

    def get_entity_references(self) -> Sequence[EntityReference]:
        """Retrieve all entity references from the collection of semantic models."""
        return list(self._entity_index.keys())

    def get_semantic_model_for_measure(self, measure_reference: MeasureReference) -> SemanticModel:  # noqa: D102
        semantic_model = self._measure_index.get(measure_reference)
        assert semantic_model, (
            f"Semantic model not found for measure: {repr(measure_reference)}. "
            f"This indicates either internal misconfiguration or that the measure does not exist."
        )
        return semantic_model

    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> TimeDimensionReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference.

        This is the time dimension along which the measure will be aggregated when a metric built on this measure
        is queried with metric_time.
        """
        return self._measure_agg_time_dimension[measure_reference]

    def get_entity_in_semantic_model(self, ref: SemanticModelElementReference) -> Optional[Entity]:
        """Retrieve the entity matching the element -> semantic model mapping, if any."""
        semantic_model = self.get_by_reference(ref.semantic_model_reference)
        if not semantic_model:
            return None

        for entity in semantic_model.entities:
            if entity.reference.element_name == ref.element_name:
                return entity

        return None

    def get_by_reference(self, semantic_model_reference: SemanticModelReference) -> Optional[SemanticModel]:
        """Retrieve the semantic model object matching the input semantic model reference, if any."""
        return self._semantic_model_reference_to_semantic_model.get(semantic_model_reference)

    def _add_semantic_model(self, semantic_model: SemanticModel) -> None:
        """Add semantic model semantic information, validating consistency with existing semantic models."""
        errors = []

        if semantic_model.reference in self._semantic_model_reference_to_semantic_model:
            errors.append(f"Semantic model {semantic_model.reference} already added.")

        for measure in semantic_model.measures:
            if measure.reference in self._measure_aggs and self._measure_aggs[measure.reference] != measure.agg:
                errors.append(
                    f"Conflicting aggregation (agg) for measure {measure.reference}. Currently registered as "
                    f"{self._measure_aggs[measure.reference]} but got {measure.agg}."
                )

        if len(errors) > 0:
            raise InvalidSemanticModelError(f"Error adding {semantic_model.reference}. Got errors: {errors}")

        self._semantic_model_to_aggregation_time_dimensions[semantic_model.reference] = ElementGrouper[
            TimeDimensionReference, MeasureSpec
        ]()

        for measure in semantic_model.measures:
            self._measure_aggs[measure.reference] = measure.agg
            self._measure_index[measure.reference] = semantic_model
            agg_time_dimension_reference = semantic_model.checked_agg_time_dimension_for_measure(measure.reference)

            matching_dimensions = tuple(
                dimension
                for dimension in semantic_model.dimensions
                if dimension.type == DimensionType.TIME
                and dimension.time_dimension_reference == agg_time_dimension_reference
            )

            matching_dimensions_count = len(matching_dimensions)
            if matching_dimensions_count != 1:
                raise RuntimeError(
                    f"Found {matching_dimensions_count} matching dimensions for {agg_time_dimension_reference} "
                    f"in {semantic_model}"
                )
            agg_time_dimension = matching_dimensions[0]
            if agg_time_dimension.type_params is None or agg_time_dimension.type_params.time_granularity is None:
                raise RuntimeError(
                    f"Aggregation time dimension does not have a time granularity set: {agg_time_dimension}"
                )

            self._semantic_model_to_aggregation_time_dimensions[semantic_model.reference].add_value(
                key=TimeDimensionReference(
                    element_name=agg_time_dimension.name,
                ),
                value=MeasureConverter.convert_to_measure_spec(measure=measure),
            )
            self._measure_agg_time_dimension[measure.reference] = TimeDimensionReference(
                element_name=agg_time_dimension.name
            )

            if measure.non_additive_dimension:
                non_additive_dimension_spec = NonAdditiveDimensionSpec(
                    name=measure.non_additive_dimension.name,
                    window_choice=measure.non_additive_dimension.window_choice,
                    window_groupings=tuple(measure.non_additive_dimension.window_groupings),
                )
                self._measure_non_additive_dimension_specs[measure.reference] = non_additive_dimension_spec
        for dim in semantic_model.dimensions:
            semantic_models_for_dimension = self._dimension_index.get(dim.reference, []) + [semantic_model]
            self._dimension_index[dim.reference] = semantic_models_for_dimension

            if not StructuredLinkableSpecName.from_name(dim.name).is_element_name:
                # TODO: [custom granularity] change this to an assertion once we're sure there aren't exceptions
                logger.warning(
                    LazyFormat(
                        lambda: f"Dimension name `{dim.name}` contains annotations, but this name should be the plain element name "
                        "from the original model. This should have been blocked by validation!"
                    )
                )

            # TODO: Construct these specs correctly. All of the time dimension specs have the default granularity
            self._dimension_ref_to_spec[dim.time_dimension_reference or dim.reference] = (
                TimeDimensionSpec(element_name=dim.name, entity_links=())
                if dim.type is DimensionType.TIME
                else DimensionSpec(element_name=dim.name, entity_links=())
            )

        for entity in semantic_model.entities:
            semantic_models_for_entity = self._entity_index.get(entity.reference, []) + [semantic_model]
            self._entity_index[entity.reference] = semantic_models_for_entity

            self._entity_ref_to_spec[entity.reference] = EntitySpec(element_name=entity.name, entity_links=())

        self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

    def get_aggregation_time_dimensions_with_measures(
        self, semantic_model_reference: SemanticModelReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all time dimensions in a semantic model with their associated measures."""
        assert (
            semantic_model_reference in self._semantic_model_to_aggregation_time_dimensions
        ), f"Semantic Model {semantic_model_reference} is not known"
        return self._semantic_model_to_aggregation_time_dimensions[semantic_model_reference]

    def get_semantic_models_for_entity(self, entity_reference: EntityReference) -> Set[SemanticModel]:
        """Return all semantic models associated with an entity reference."""
        return set(self._entity_index.get(entity_reference, []))

    def get_semantic_models_for_dimension(self, dimension_reference: DimensionReference) -> Set[SemanticModel]:
        """Return all semantic models associated with a dimension reference."""
        return set(self._dimension_index.get(dimension_reference, []))

    def get_element_spec_for_name(self, element_name: str) -> LinkableInstanceSpec:
        """Returns the spec for the given name of a linkable element (dimension or entity)."""
        if TimeDimensionReference(element_name=element_name) in self._dimension_ref_to_spec:
            return self._dimension_ref_to_spec[TimeDimensionReference(element_name=element_name)]
        elif DimensionReference(element_name=element_name) in self._dimension_ref_to_spec:
            return self._dimension_ref_to_spec[DimensionReference(element_name=element_name)]
        elif EntityReference(element_name=element_name) in self._entity_ref_to_spec:
            return self._entity_ref_to_spec[EntityReference(element_name=element_name)]
        else:
            raise ValueError(f"Unable to find linkable element {element_name} in manifest")

    def get_agg_time_dimension_specs_for_measure(
        self, measure_reference: MeasureReference
    ) -> Sequence[TimeDimensionSpec]:
        """Get the agg time dimension specs that can be used in place of metric time for this measure."""
        result = self._measure_reference_to_agg_time_dimension_specs.get(measure_reference)
        if result is not None:
            return result

        result = self._get_agg_time_dimension_specs_for_measure(measure_reference)
        self._measure_reference_to_agg_time_dimension_specs[measure_reference] = result
        return result

    def _get_agg_time_dimension_specs_for_measure(
        self, measure_reference: MeasureReference
    ) -> Sequence[TimeDimensionSpec]:
        agg_time_dimension = self.get_agg_time_dimension_for_measure(measure_reference)
        # A measure's agg_time_dimension is required to be in the same semantic model as the measure,
        # so we can assume the same semantic model for both measure and dimension.
        semantic_model = self.get_semantic_model_for_measure(measure_reference)
        entity_link = SemanticModelHelper.resolved_primary_entity(semantic_model)
        return TimeDimensionSpec.generate_possible_specs_for_time_dimension(
            time_dimension_reference=agg_time_dimension,
            entity_links=(entity_link,),
            custom_granularities=self._custom_granularities,
        )

    def get_defined_time_granularity(self, time_dimension_reference: TimeDimensionReference) -> TimeGranularity:
        """Time granularity from the time dimension's YAML definition. If not set, defaults to DAY."""
        result = self._time_dimension_to_defined_time_granularity.get(time_dimension_reference)

        if result is not None:
            return result

        result = self._get_defined_time_granularity(time_dimension_reference)
        self._time_dimension_to_defined_time_granularity[time_dimension_reference] = result
        return result

    def _get_defined_time_granularity(self, time_dimension_reference: TimeDimensionReference) -> TimeGranularity:
        time_dimension = self.get_dimension(time_dimension_reference)

        defined_time_granularity = DEFAULT_TIME_GRANULARITY
        if time_dimension.type_params and time_dimension.type_params.time_granularity:
            defined_time_granularity = time_dimension.type_params.time_granularity

        return defined_time_granularity

    @property
    def measure_lookup(self) -> MeasureLookup:  # noqa: D102
        return self._measure_lookup

    @property
    def dimension_lookup(self) -> DimensionLookup:  # noqa: D102
        return self._dimension_lookup
