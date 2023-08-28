from __future__ import annotations

import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Sequence, Set

from dbt_semantic_interfaces.pretty_print import pformat_big_objects
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
from dbt_semantic_interfaces.type_enums import DimensionType, EntityType
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from typing_extensions import override

from metricflow.errors.errors import InvalidSemanticModelError
from metricflow.model.semantics.element_group import ElementGrouper
from metricflow.model.spec_converters import MeasureConverter
from metricflow.protocols.semantics import SemanticModelAccessor
from metricflow.specs.specs import MeasureSpec, NonAdditiveDimensionSpec

logger = logging.getLogger(__name__)


class SemanticModelLookup(SemanticModelAccessor):
    """Tracks semantic information for semantic model held in a set of SemanticModelContainers.

    This implements both the SemanticModelAccessors protocol, the interface type we use throughout the codebase.
    That interface prevents unwanted calls to methods for adding semantic models to the container.
    """

    def __init__(  # noqa: D
        self,
        model: SemanticManifest,
    ) -> None:
        self._model = model
        self._measure_index: Dict[MeasureReference, List[SemanticModel]] = defaultdict(list)
        self._measure_aggs: Dict[
            MeasureReference, AggregationType
        ] = {}  # maps measures to their one consistent aggregation
        self._measure_agg_time_dimension: Dict[MeasureReference, TimeDimensionReference] = {}
        self._measure_non_additive_dimension_specs: Dict[MeasureReference, NonAdditiveDimensionSpec] = {}
        self._dimension_index: Dict[DimensionReference, List[SemanticModel]] = defaultdict(list)
        self._linkable_reference_index: Dict[LinkableElementReference, List[SemanticModel]] = defaultdict(list)
        self._entity_index: Dict[Optional[str], List[SemanticModel]] = defaultdict(list)
        self._entity_ref_to_entity: Dict[EntityReference, Optional[str]] = {}
        self._semantic_model_names: Set[str] = set()

        self._semantic_model_to_aggregation_time_dimensions: Dict[
            SemanticModelReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        for semantic_model in self._model.semantic_models:
            self._add_semantic_model(semantic_model)

    def get_dimension_references(self) -> Sequence[DimensionReference]:  # noqa: D
        return tuple(self._dimension_index.keys())

    @staticmethod
    def get_dimension_from_semantic_model(
        semantic_model: SemanticModel, dimension_reference: LinkableElementReference
    ) -> Dimension:
        """Get dimension from semantic model."""
        for dim in semantic_model.dimensions:
            if dim.reference == dimension_reference:
                return dim
        raise ValueError(
            f"No dimension with name ({dimension_reference}) in semantic_model with name ({semantic_model.name})"
        )

    def get_dimension(self, dimension_reference: DimensionReference) -> Dimension:
        """Retrieves a full dimension object by name."""
        for dimension_source in self._dimension_index[dimension_reference]:
            dimension = SemanticModelLookup.get_dimension_from_semantic_model(
                semantic_model=dimension_source, dimension_reference=dimension_reference
            )

            return deepcopy(dimension)

        raise ValueError(
            f"Could not find dimension with name ({dimension_reference.element_name}) in configured semantic models"
        )

    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieves a full dimension object by name."""
        dimension_reference = time_dimension_reference.dimension_reference()

        if dimension_reference not in self._dimension_index:
            raise ValueError(
                f"Could not find dimension with name ({dimension_reference.element_name}) in configured semantic models"
            )

        for dimension_source in self._dimension_index[dimension_reference]:
            dimension = SemanticModelLookup.get_dimension_from_semantic_model(
                semantic_model=dimension_source, dimension_reference=dimension_reference
            )
            # TODO: Unclear if the deepcopy is necessary.
            return deepcopy(dimension)

        assert False, f"{time_dimension_reference} should have been in the dimension index"

    @property
    def measure_references(self) -> Sequence[MeasureReference]:  # noqa: D
        return list(self._measure_index.keys())

    @property
    def non_additive_dimension_specs_by_measure(self) -> Dict[MeasureReference, NonAdditiveDimensionSpec]:  # noqa: D
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

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured semantic models")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across semantic models, so just use the first one.
        semantic_model = list(self._measure_index[measure_reference])[0]
        return SemanticModelLookup.get_measure_from_semantic_model(
            semantic_model=semantic_model, measure_reference=measure_reference
        )

    def get_entity_references(self) -> Sequence[EntityReference]:  # noqa: D
        return list(self._entity_ref_to_entity.keys())

    # DSC interface
    def get_semantic_models_for_measure(  # noqa: D
        self, measure_reference: MeasureReference
    ) -> Sequence[SemanticModel]:
        return self._measure_index[measure_reference]

    def get_agg_time_dimension_for_measure(  # noqa: D
        self, measure_reference: MeasureReference
    ) -> TimeDimensionReference:
        return self._measure_agg_time_dimension[measure_reference]

    def get_entity_in_semantic_model(self, ref: SemanticModelElementReference) -> Optional[Entity]:  # Noqa: d
        semantic_model = self.get_by_reference(ref.semantic_model_reference)
        if not semantic_model:
            return None

        for entity in semantic_model.entities:
            if entity.reference.element_name == ref.element_name:
                return entity

        return None

    def get_by_reference(self, semantic_model_reference: SemanticModelReference) -> Optional[SemanticModel]:  # noqa: D
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
            self._measure_index[measure.reference].append(semantic_model)
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

            primary_entity = SemanticModelLookup._resolved_primary_entity(semantic_model)

            if primary_entity is None:
                raise RuntimeError(
                    f"The semantic model should have a primary entity since there are dimensions, but it does not. "
                    f"Semantic model is:\n{pformat_big_objects(semantic_model)}"
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
            self._linkable_reference_index[dim.reference].append(semantic_model)
            self._dimension_index[dim.reference].append(semantic_model)
        for entity in semantic_model.entities:
            self._entity_ref_to_entity[entity.reference] = entity.name
            self._entity_index[entity.name].append(semantic_model)
            self._linkable_reference_index[entity.reference].append(semantic_model)

        self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

    @property
    def semantic_model_references(self) -> Sequence[SemanticModelReference]:  # noqa: D
        semantic_model_names_sorted = sorted(self._semantic_model_names)
        return tuple(SemanticModelReference(semantic_model_name=x) for x in semantic_model_names_sorted)

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
        entity = self._entity_ref_to_entity[entity_reference]
        return set(self._entity_index[entity])

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
    def _resolved_primary_entity(semantic_model: SemanticModel) -> Optional[EntityReference]:
        """Return the primary entity for dimensions in the model."""
        primary_entity_reference = semantic_model.primary_entity_reference

        entities_with_type_primary = tuple(
            entity for entity in semantic_model.entities if entity.type == EntityType.PRIMARY
        )

        # This should be caught by the validation, but adding a sanity check.
        assert len(entities_with_type_primary) <= 1, f"Found >1 primary entity in {semantic_model}"
        if primary_entity_reference is not None:
            assert len(entities_with_type_primary) == 0, (
                f"The primary_entity field was set to {primary_entity_reference}, but there are non-zero entities with "
                f"type {EntityType.PRIMARY} in {semantic_model}"
            )

        if primary_entity_reference is not None:
            return primary_entity_reference

        if len(entities_with_type_primary) > 0:
            return entities_with_type_primary[0].reference

        return None

    @staticmethod
    @override
    def entity_links_for_local_elements(semantic_model: SemanticModel) -> Sequence[EntityReference]:
        primary_entity_reference = semantic_model.primary_entity_reference

        possible_entity_links = set()
        if primary_entity_reference is not None:
            possible_entity_links.add(primary_entity_reference)

        for entity in semantic_model.entities:
            if entity.is_linkable_entity_type:
                possible_entity_links.add(entity.reference)

        return sorted(possible_entity_links, key=lambda entity_reference: entity_reference.element_name)
