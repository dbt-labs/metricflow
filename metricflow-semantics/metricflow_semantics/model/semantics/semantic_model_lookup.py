from __future__ import annotations

import logging
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.protocols.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MeasureReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import AggregationType, DimensionType, EntityType, TimeGranularity

from metricflow_semantics.errors.error_classes import InvalidSemanticModelError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat
from metricflow_semantics.model.semantics.element_group import ElementGrouper
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

    def __init__(
        self, semantic_manifest: SemanticManifest, custom_granularities: Dict[str, ExpandedTimeGranularity]
    ) -> None:
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
        self._primary_entity_index: Dict[EntityReference, List[SemanticModel]] = {}

        # TODO: remove this dict
        self._dimension_ref_to_spec: Dict[DimensionReference, DimensionSpec] = {}

        self._entity_ref_to_spec: Dict[EntityReference, EntitySpec] = {}

        self._semantic_model_to_aggregation_time_dimensions: Dict[
            SemanticModelReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        for semantic_model in sorted(semantic_manifest.semantic_models, key=lambda semantic_model: semantic_model.name):
            self._add_semantic_model(semantic_model)

        # Cache for defined time granularity.
        self._time_dimension_to_defined_time_granularity: Dict[SemanticModelElementReference, TimeGranularity] = {}

        # Cache for agg. time dimension for measure.
        self._measure_reference_to_agg_time_dimension_specs: Dict[MeasureReference, Set[TimeDimensionSpec]] = {}

    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of semantic models."""
        return tuple(self._dimension_index.keys())

    @staticmethod
    def get_dimension_from_semantic_model(
        semantic_model: SemanticModel, dimension_reference: DimensionReference
    ) -> Dimension:
        """Get dimension from semantic model."""
        for dim in semantic_model.dimensions:
            # Check element_name instead of direct reference in case the input is a TimeDimensionReference
            if dim.reference.element_name == dimension_reference.element_name:
                return dim
        raise ValueError(
            f"No dimension with name '{dimension_reference.element_name}' in semantic_model '{semantic_model.name}'."
        )

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

    @staticmethod
    def get_measure_from_semantic_model(semantic_model: SemanticModel, measure_reference: MeasureReference) -> Measure:
        """Get measure from semantic model."""
        for measure in semantic_model.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in semantic_model with name ({semantic_model.name})"
        )

    def get_measure(self, measure_reference: MeasureReference) -> Measure:
        """Retrieve the measure model object associated with the measure reference."""
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured semantic models")

        return SemanticModelLookup.get_measure_from_semantic_model(
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

    def get_semantic_model(self, semantic_model_reference: SemanticModelReference) -> SemanticModel:
        """Retrieve the semantic model object matching the input semantic model reference."""
        semantic_model = self._semantic_model_reference_to_semantic_model.get(semantic_model_reference)
        assert semantic_model, f"Semantic model {semantic_model_reference} not found in manifest."
        return semantic_model

    def get_agg_time_dimension_for_measure(self, measure_reference: MeasureReference) -> SemanticModelElementReference:
        """Retrieves the aggregate time dimension that is associated with the measure reference.

        This is the time dimension along which the measure will be aggregated when a metric built on this measure
        is queried with metric_time.
        """
        agg_time_dimension = self._measure_agg_time_dimension.get(measure_reference)
        if not agg_time_dimension:
            raise ValueError(
                f"No agg_time_dimension found for measure {measure_reference}. This indicates internal misconfiguration"
                " since validations should ensure an agg_time_dimension for every measure."
            )

        # A measure's agg_time_dimension is required to be in the same semantic model as the measure,
        # so we can assume the same semantic model for both measure and dimension.
        semantic_model = self.get_semantic_model_for_measure(measure_reference)
        return SemanticModelElementReference.create_from_references(
            element_reference=agg_time_dimension, semantic_model_reference=semantic_model.reference
        )

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

            # TODO: do we need this here? This should be handled in validations
            self.get_primary_entity_else_error(semantic_model)

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

        for entity in semantic_model.entities:
            semantic_models_for_entity = self._entity_index.get(entity.reference, []) + [semantic_model]
            self._entity_index[entity.reference] = semantic_models_for_entity

            self._entity_ref_to_spec[entity.reference] = EntitySpec(element_name=entity.name, entity_links=())

        primary_entity = self.resolved_primary_entity(semantic_model)
        if primary_entity:
            self._primary_entity_index[primary_entity] = self._primary_entity_index.get(primary_entity, []) + [
                semantic_model
            ]

        self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

    def get_primary_entity_else_error(self, semantic_model: SemanticModel) -> EntityReference:
        """Get primary entity from semantic model and error if it doesn't exist.

        If there are dimensions in the semantic model, there must be a primary entity. If there are measures, we can
        also assume there must be a primary entity because measures are required to have an `agg_time_dimension`
        defined in the same semantic model.
        """
        primary_entity = SemanticModelLookup.resolved_primary_entity(semantic_model)
        if primary_entity is None:
            raise RuntimeError(
                f"The semantic model should have a primary entity since there are dimensions, but it does not. "
                f"Semantic model is:\n{mf_pformat(semantic_model)}"
            )
        return primary_entity

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

    @staticmethod
    def get_entity_from_semantic_model(
        semantic_model: SemanticModel, entity_reference: LinkableElementReference
    ) -> Entity:
        """Get entity from semantic model."""
        for entity in semantic_model.entities:
            if entity.reference == entity_reference:
                return entity

        raise ValueError(
            f"No entity with name ({entity_reference}) in semantic_model with name ({semantic_model.name})"
        )

    @staticmethod
    def resolved_primary_entity(semantic_model: SemanticModel) -> Optional[EntityReference]:
        """Return the primary entity for dimensions in the model."""
        primary_entity_reference = semantic_model.primary_entity_reference

        entities_with_type_primary = tuple(
            entity for entity in semantic_model.entities if entity.type == EntityType.PRIMARY
        )

        # This should be caught by the validation, but adding a sanity check.
        assert len(entities_with_type_primary) <= 1, f"Found > 1 primary entity in {semantic_model}"
        if primary_entity_reference is not None:
            assert len(entities_with_type_primary) == 0, (
                f"The primary_entity field was set to {primary_entity_reference}, but there are non-zero entities with "
                f"type {EntityType.PRIMARY} in {semantic_model}"
            )
            return primary_entity_reference

        if len(entities_with_type_primary) > 0:
            return entities_with_type_primary[0].reference

        return None

    @staticmethod
    def entity_links_for_local_elements(semantic_model: SemanticModel) -> Sequence[EntityReference]:
        """Return the entity prefix that can be used to access dimensions defined in the semantic model."""
        primary_entity_reference = semantic_model.primary_entity_reference

        possible_entity_links = set()
        if primary_entity_reference is not None:
            possible_entity_links.add(primary_entity_reference)

        for entity in semantic_model.entities:
            if entity.is_linkable_entity_type:
                possible_entity_links.add(entity.reference)

        return sorted(possible_entity_links, key=lambda entity_reference: entity_reference.element_name)

    # TODO: remove this method
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

    def local_entity_links_for_time_dimension(
        self, time_dimension_element_reference: SemanticModelElementReference
    ) -> Tuple[EntityReference]:
        """Return the primary entity as entity links that should be used for the time dimension when no joins are involved."""
        semantic_model = self.get_semantic_model(time_dimension_element_reference.semantic_model_reference)
        return (self.get_primary_entity_else_error(semantic_model),)

    def generate_possible_time_dimension_specs_from_element_reference(
        self,
        time_dimension_element_reference: SemanticModelElementReference,
    ) -> Set[TimeDimensionSpec]:
        """Generate all possible time dimension specs that use the given time dimension.

        This means a spec for every possible granularity and date part.
        """
        return set(
            TimeDimensionSpec.generate_possible_specs_for_time_dimension(
                time_dimension_reference=TimeDimensionReference(time_dimension_element_reference.element_name),
                entity_links=self.local_entity_links_for_time_dimension(time_dimension_element_reference),
                custom_granularities=self._custom_granularities,
            )
        )

    def get_agg_time_dimension_specs_for_measure(self, measure_reference: MeasureReference) -> Set[TimeDimensionSpec]:
        """Get the agg time dimension specs that can be used in place of metric time for this measure."""
        result = self._measure_reference_to_agg_time_dimension_specs.get(measure_reference)
        if result is not None:
            return result

        agg_time_element_reference = self.get_agg_time_dimension_for_measure(measure_reference)
        agg_time_dimension_specs = self.generate_possible_time_dimension_specs_from_element_reference(
            agg_time_element_reference
        )
        self._measure_reference_to_agg_time_dimension_specs[measure_reference] = agg_time_dimension_specs
        return agg_time_dimension_specs

    def get_defined_time_granularity(
        self, time_dimension_element_reference: SemanticModelElementReference
    ) -> TimeGranularity:
        """Time granularity from the time dimension's YAML definition. If not set, defaults to DAY."""
        result = self._time_dimension_to_defined_time_granularity.get(time_dimension_element_reference)

        if result is not None:
            return result

        result = self._get_defined_time_granularity(time_dimension_element_reference)
        self._time_dimension_to_defined_time_granularity[time_dimension_element_reference] = result
        return result

    def _get_defined_time_granularity(
        self, time_dimension_element_reference: SemanticModelElementReference
    ) -> TimeGranularity:
        semantic_model = self.get_semantic_model(time_dimension_element_reference.semantic_model_reference)
        time_dimension = self.get_dimension_from_semantic_model(
            dimension_reference=TimeDimensionReference(time_dimension_element_reference.element_name),
            semantic_model=semantic_model,
        )

        defined_time_granularity = DEFAULT_TIME_GRANULARITY
        if time_dimension.type_params and time_dimension.type_params.time_granularity:
            defined_time_granularity = time_dimension.type_params.time_granularity

        return defined_time_granularity

    def get_semantic_models_for_primary_entity(self, entity_reference: EntityReference) -> List[SemanticModel]:
        """Return all semantic models associated with the primary entity reference."""
        return self._primary_entity_index.get(entity_reference, [])

    def get_semantic_model_for_dimension(
        self, dimension_reference: DimensionReference, entity_links: Sequence[EntityReference]
    ) -> SemanticModel:
        """Use the entity links proveded to deterine the semantic model where this dimension is defined."""
        if not entity_links:
            raise ValueError(
                f"No entity links received for dimension {dimension_reference}. "
                "Entity links are required to determine semantic model for dimension."
            )
        primary_entity = entity_links[-1]
        semantic_models = self.get_semantic_models_for_entity(primary_entity)
        for semantic_model in semantic_models:
            try:
                self.get_dimension_from_semantic_model(
                    semantic_model=semantic_model, dimension_reference=dimension_reference
                )
                return semantic_model
            except ValueError:
                continue
        raise ValueError(
            f"Could not find dimension {dimension_reference} in any semantic model associated with primary entity {primary_entity}."
        )

    def dimension_is_partition(self, dimension_spec: DimensionSpec) -> bool:  # noqa: D102
        semantic_model = self.get_semantic_model_for_dimension(
            dimension_reference=dimension_spec.reference, entity_links=dimension_spec.entity_links
        )
        return self.get_dimension_from_semantic_model(
            dimension_reference=dimension_spec.reference, semantic_model=semantic_model
        ).is_partition
