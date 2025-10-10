from __future__ import annotations

import logging
from functools import cached_property
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    SemanticModelElementReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import DimensionType

from metricflow_semantics.errors.error_classes import InvalidSemanticModelError
from metricflow_semantics.model.semantics.dimension_lookup import DimensionLookup
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.entity_spec import EntitySpec
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.time_dimension_spec import DEFAULT_TIME_GRANULARITY, TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


class SemanticModelLookup:
    """Tracks semantic information for semantic models held in a set of SemanticModelContainers."""

    def __init__(
        self, semantic_manifest: SemanticManifest, custom_granularities: Dict[str, ExpandedTimeGranularity]
    ) -> None:
        """Initializer.

        Args:
            semantic_manifest: the semantic manifest used for loading semantic model definitions
        """
        self.custom_granularities = custom_granularities
        self._dimension_index: Dict[DimensionReference, List[SemanticModel]] = {}
        self.entity_index: Dict[EntityReference, List[SemanticModel]] = {}

        self._dimension_ref_to_spec: Dict[DimensionReference, DimensionSpec] = {}
        self._entity_ref_to_spec: Dict[EntityReference, EntitySpec] = {}

        self._semantic_model_reference_to_semantic_model: Dict[SemanticModelReference, SemanticModel] = {}
        sorted_semantic_models = sorted(
            semantic_manifest.semantic_models, key=lambda semantic_model: semantic_model.name
        )
        for semantic_model in sorted_semantic_models:
            self._add_semantic_model(semantic_model)

        self._dimension_lookup = DimensionLookup(sorted_semantic_models)

    @cached_property
    def custom_granularity_names(self) -> Tuple[str, ...]:
        """Returns all the custom_granularity names."""
        return tuple(self.custom_granularities.keys())

    def get_dimension_references(self) -> Sequence[DimensionReference]:
        """Retrieve all dimension references from the collection of semantic models."""
        return tuple(self._dimension_index.keys())

    def get_entity_references(self) -> Sequence[EntityReference]:
        """Retrieve all entity references from the collection of semantic models."""
        return list(self.entity_index.keys())

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
            raise InvalidSemanticModelError(
                LazyFormat(
                    "Got errors adding the given semantic model.", semantic_model=semantic_model.name, errors=errors
                )
            )

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

            if dim.type is DimensionType.TIME:
                defined_granularity = dim.type_params.time_granularity if dim.type_params else DEFAULT_TIME_GRANULARITY
                assert dim.time_dimension_reference, f"Time dimension {dim} does not have a time dimension reference"
                self._dimension_ref_to_spec[dim.time_dimension_reference] = TimeDimensionSpec(
                    element_name=dim.name,
                    entity_links=(),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(defined_granularity),
                )
            else:
                self._dimension_ref_to_spec[dim.reference] = DimensionSpec(element_name=dim.name, entity_links=())

        for entity in semantic_model.entities:
            semantic_models_for_entity = self.entity_index.get(entity.reference, []) + [semantic_model]
            self.entity_index[entity.reference] = semantic_models_for_entity

            self._entity_ref_to_spec[entity.reference] = EntitySpec(element_name=entity.name, entity_links=())

        self._semantic_model_reference_to_semantic_model[semantic_model.reference] = semantic_model

    def get_semantic_models_for_entity(self, entity_reference: EntityReference) -> Set[SemanticModel]:
        """Return all semantic models associated with an entity reference."""
        return set(self.entity_index.get(entity_reference, []))

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
    def dimension_lookup(self) -> DimensionLookup:  # noqa: D102
        return self._dimension_lookup

    @cached_property
    def model_reference_to_model(self) -> Mapping[SemanticModelReference, SemanticModel]:  # noqa: D102
        return {
            model_reference: self._semantic_model_reference_to_semantic_model[model_reference]
            for model_reference in sorted(self._semantic_model_reference_to_semantic_model)
        }
