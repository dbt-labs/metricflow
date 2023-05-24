from __future__ import annotations

import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Sequence, Set

from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MeasureReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType

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

    def get_dimension(self, dimension_reference: DimensionReference) -> Dimension:
        """Retrieves a full dimension object by name."""
        for dimension_source in self._dimension_index[dimension_reference]:
            dimension = dimension_source.get_dimension(dimension_reference)
            # find the semantic model that has the requested dimension by the requested entity

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
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured semantic models")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across semantic models, so just use the first one.
        return list(self._measure_index[measure_reference])[0].get_measure(measure_reference)

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
            agg_time_dimension = measure.checked_agg_time_dimension
            self._semantic_model_to_aggregation_time_dimensions[semantic_model.reference].add_value(
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
