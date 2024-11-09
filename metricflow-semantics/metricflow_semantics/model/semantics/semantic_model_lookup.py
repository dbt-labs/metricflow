from __future__ import annotations

import logging
from functools import cached_property
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.protocols.entity import Entity
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
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow_semantics.errors.error_classes import InvalidSemanticModelError
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.model.semantics.dimension_lookup import DimensionLookup
from metricflow_semantics.model.semantics.element_group import ElementGrouper
from metricflow_semantics.model.semantics.measure_lookup import MeasureLookup
from metricflow_semantics.model.spec_converters import MeasureConverter
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.measure_spec import MeasureSpec
from metricflow_semantics.specs.non_additive_dimension_spec import NonAdditiveDimensionSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
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

        self._measure_lookup = MeasureLookup(sorted_semantic_models, custom_granularities)
        self._dimension_lookup = DimensionLookup(sorted_semantic_models)

    @cached_property
    def custom_granularity_names(self) -> Tuple[str, ...]:
        """Returns all the custom_granularity names."""
        return tuple(self._custom_granularities.keys())

    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of semantic models."""
        return tuple(self._dimension_index.keys())

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

    def get_entity_references(self) -> Sequence[EntityReference]:
        """Retrieve all entity references from the collection of semantic models."""
        return list(self._entity_index.keys())

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

        if len(errors) > 0:
            raise InvalidSemanticModelError(f"Error adding {semantic_model.reference}. Got errors: {errors}")

        self._semantic_model_to_aggregation_time_dimensions[semantic_model.reference] = ElementGrouper[
            TimeDimensionReference, MeasureSpec
        ]()

        for measure in semantic_model.measures:
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

            if not StructuredLinkableSpecName.from_name(
                qualified_name=dim.name, custom_granularity_names=self.custom_granularity_names
            ).is_element_name:
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

    @property
    def measure_lookup(self) -> MeasureLookup:  # noqa: D102
        return self._measure_lookup

    @property
    def dimension_lookup(self) -> DimensionLookup:  # noqa: D102
        return self._dimension_lookup
