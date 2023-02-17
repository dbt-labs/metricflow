import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Optional, Set, Sequence

from metricflow.aggregation_properties import AggregationType
from metricflow.errors.errors import InvalidEntityError
from metricflow.instances import EntityReference, EntityElementReference
from metricflow.model.objects.entity import Entity, EntityOrigin
from metricflow.model.objects.elements.dimension import Dimension
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure
from dbt.dbt_semantic.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.entity_container import PydanticEntityContainer
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


class EntitySemantics:
    """Tracks semantic information for entity held in a set of EntityContainers

    This implements both the EntitySemanticsAccessors protocol, the interface type we use throughout the codebase.
    That interface prevents unwanted calls to methods for adding entities to the container.
    """

    def __init__(  # noqa: D
        self,
        model: UserConfiguredModel,
        configured_entity_container: PydanticEntityContainer,
    ) -> None:
        self._model = model
        self._measure_index: Dict[MeasureReference, List[Entity]] = defaultdict(list)
        self._measure_aggs: Dict[
            MeasureReference, AggregationType
        ] = {}  # maps measures to their one consistent aggregation
        self._measure_agg_time_dimension: Dict[MeasureReference, TimeDimensionReference] = {}
        self._measure_non_additive_dimension_specs: Dict[MeasureReference, NonAdditiveDimensionSpec] = {}
        self._dimension_index: Dict[DimensionReference, List[Entity]] = defaultdict(list)
        self._linkable_reference_index: Dict[LinkableElementReference, List[Entity]] = defaultdict(list)
        self._entity_index: Dict[Optional[str], List[Entity]] = defaultdict(list)
        self._identifier_ref_to_entity: Dict[IdentifierReference, Optional[str]] = {}
        self._entity_names: Set[str] = set()

        self._configured_entity_container = configured_entity_container
        self._entity_to_aggregation_time_dimensions: Dict[
            EntityReference, ElementGrouper[TimeDimensionReference, MeasureSpec]
        ] = {}

        # Add semantic tracking for entities from configured_entity_container
        for entity in self._configured_entity_container.values():
            assert isinstance(entity, Entity)
            self.add_configured_entity(entity)

    def add_configured_entity(self, entity: Entity) -> None:
        """Dont use this unless you mean it (ie in tests). The configured entities are supposed to be static"""
        self._configured_entity_container._put(entity)
        self._add_entity(entity)

    def get_dimension_references(self) -> List[DimensionReference]:  # noqa: D
        return list(self._dimension_index.keys())

    def get_dimension(
        self, dimension_reference: DimensionReference, origin: Optional[EntityOrigin] = None
    ) -> Dimension:
        """Retrieves a full dimension object by name"""
        for dimension_source in self._dimension_index[dimension_reference]:
            if origin and dimension_source.origin != origin:
                continue
            dimension = dimension_source.get_dimension(dimension_reference)
            # find the entity that has the requested dimension by the requested identifier

            return deepcopy(dimension)

        raise ValueError(
            f"Could not find dimension with name ({dimension_reference.element_name}) in configured entities"
        )

    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieves a full dimension object by name"""
        dimension_reference = time_dimension_reference.dimension_reference()

        if dimension_reference not in self._dimension_index:
            raise ValueError(
                f"Could not find dimension with name ({dimension_reference.element_name}) in configured entities"
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
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured entities")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across entities, so just use the first one.
        return list(self._measure_index[measure_reference])[0].get_measure(measure_reference)

    def get_identifier_references(self) -> List[IdentifierReference]:  # noqa: D
        return list(self._identifier_ref_to_entity.keys())

    # DSC interface
    def get_entities_for_measure(self, measure_reference: MeasureReference) -> List[Entity]:  # noqa: D
        return self._measure_index[measure_reference]

    def get_agg_time_dimension_for_measure(  # noqa: D
        self, measure_reference: MeasureReference
    ) -> TimeDimensionReference:
        return self._measure_agg_time_dimension[measure_reference]

    def get_identifier_in_entity(self, ref: EntityElementReference) -> Optional[Identifier]:  # Noqa: d
        entity = self.get(ref.entity_name)
        if not entity:
            return None

        for identifier in entity.identifiers:
            if identifier.reference.element_name == ref.element_name:
                return identifier

        return None

    def get(self, entity_name: str) -> Optional[Entity]:  # noqa: D
        if entity_name in self._configured_entity_container:
            entity = self._configured_entity_container.get(entity_name)
            assert isinstance(entity, Entity)
            return entity

        return None

    def get_by_reference(self, entity_reference: EntityReference) -> Optional[Entity]:  # noqa: D
        return self.get(entity_reference.entity_name)

    def _add_entity(
        self,
        entity: Entity,
        fail_on_error: bool = True,
        logging_context: str = "",
    ) -> None:
        """Add entity semantic information, validating consistency with existing entities."""
        errors = []

        if entity.name in self._entity_names:
            errors.append(f"name {entity.name} already registered - please ensure entity names are unique")

        for measure in entity.measures:
            if measure.reference in self._measure_aggs and self._measure_aggs[measure.reference] != measure.agg:
                errors.append(
                    f"conflicting aggregation (agg) for measure `{measure.reference.element_name}` registered as "
                    f"`{self._measure_aggs[measure.reference]}`; Got `{measure.agg}"
                )

        if errors:
            error_prefix = "\n  - "
            error_msg = (
                f"Unable to add entity `{entity.name}` "
                f"{'while ' + logging_context + ' ' if logging_context else ''}"
                f"{'... skipping' if not fail_on_error else ''}.\n"
                f"Errors: {error_prefix + error_prefix.join(errors)}"
            )
            if fail_on_error:
                raise InvalidEntityError(error_msg)
            logger.warning(error_msg)
            return

        self._entity_names.add(entity.name)
        self._entity_to_aggregation_time_dimensions[entity.reference] = ElementGrouper[
            TimeDimensionReference, MeasureSpec
        ]()

        for measure in entity.measures:
            self._measure_aggs[measure.reference] = measure.agg
            self._measure_index[measure.reference].append(entity)
            agg_time_dimension = measure.checked_agg_time_dimension
            self._entity_to_aggregation_time_dimensions[entity.reference].add_value(
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
        for dim in entity.dimensions:
            self._linkable_reference_index[dim.reference].append(entity)
            self._dimension_index[dim.reference].append(entity)
        for ident in entity.identifiers:
            self._identifier_ref_to_entity[ident.reference] = ident.entity
            self._entity_index[ident.entity].append(entity)
            self._linkable_reference_index[ident.reference].append(entity)

    @property
    def entity_references(self) -> Sequence[EntityReference]:  # noqa: D
        entity_names_sorted = sorted(self._entity_names)
        return tuple(EntityReference(entity_name=x) for x in entity_names_sorted)

    def get_aggregation_time_dimensions_with_measures(
        self, entity_reference: EntityReference
    ) -> ElementGrouper[TimeDimensionReference, MeasureSpec]:
        """Return all time dimensions in a entity with their associated measures."""
        assert (
            entity_reference in self._entity_to_aggregation_time_dimensions
        ), f"entity {entity_reference} is not known"
        return self._entity_to_aggregation_time_dimensions[entity_reference]

    def get_entities_for_identifier(self, identifier_reference: IdentifierReference) -> Set[Entity]:
        """Return all entities associated with an identifier reference"""
        identifier_entity = self._identifier_ref_to_entity[identifier_reference]
        return set(self._entity_index[identifier_entity])
