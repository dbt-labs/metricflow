from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Mapping
from functools import cached_property
from typing import Iterable

from dbt_semantic_interfaces.protocols import Entity
from dbt_semantic_interfaces.type_enums import EntityType
from typing_extensions import override

from metricflow_semantics.toolkit.collections.ordered_set import MutableOrderedSet, OrderedSet
from metricflow_semantics.toolkit.mf_logging.attribute_pretty_format import AttributeMapping, AttributePrettyFormattable

logger = logging.getLogger(__name__)


class EntityLookup(AttributePrettyFormattable):
    """A lookup for entities within a semantic model."""

    def __init__(self, entities: Iterable[Entity]) -> None:  # noqa: D107
        self._entity_name_to_entity: Mapping[str, Entity] = {entity.name: entity for entity in entities}

    @cached_property
    def entity_name_to_type(self) -> Mapping[str, EntityType]:  # noqa: D102
        return {entity_name: entity.type for entity_name, entity in self._entity_name_to_entity.items()}

    @cached_property
    def entity_type_to_names(self) -> Mapping[EntityType, OrderedSet[str]]:  # noqa: D102
        entity_type_to_names: dict[EntityType, MutableOrderedSet[str]] = defaultdict(MutableOrderedSet)
        for entity_name, entity_type in self.entity_name_to_type.items():
            entity_type_to_names[entity_type].add(entity_name)
        return {entity_type: entity_names.as_frozen() for entity_type, entity_names in entity_type_to_names.items()}

    @cached_property
    @override
    def _attribute_mapping(self) -> AttributeMapping:
        return dict(
            **super()._attribute_mapping,
            **{
                attribute_name: getattr(self, attribute_name)
                for attribute_name in (
                    "entity_name_to_type",
                    "entity_type_to_names",
                )
            },
        )
