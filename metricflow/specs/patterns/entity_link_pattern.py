from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from more_itertools import is_sorted
from typing_extensions import override

from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec, InstanceSpecSet, LinkableInstanceSpec

logger = logging.getLogger(__name__)


class ParameterSetField(Enum):
    """The fields of the EntityLinkPatternParameterSet class used for matching in the EntityLinkPattern.

    Considering moving this to be a part of the specs module / classes.
    """

    ELEMENT_NAME = "element_name"
    ENTITY_LINKS = "entity_links"
    TIME_GRANULARITY = "time_granularity"
    DATE_PART = "date_part"

    def __lt__(self, other: Any) -> bool:  # type: ignore[misc]
        """Allow for ordering so that a sequence of these can be consistently represented for test snapshots."""
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


@dataclass(frozen=True)
class EntityLinkPatternParameterSet:
    """See EntityPathPattern for more details."""

    # Specify the field values to compare. None can't be used to signal "don't compare" because sometimes a pattern
    # needs to match a spec where the field is None. These should be sorted.
    fields_to_compare: Tuple[ParameterSetField, ...]

    # The name of the element in the semantic model
    element_name: Optional[str] = None
    # The entities used for joining semantic models.
    entity_links: Optional[Tuple[EntityReference, ...]] = None
    # Properties of time dimensions to match.
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None

    @staticmethod
    def from_parameters(  # noqa: D
        fields_to_compare: Sequence[ParameterSetField],
        element_name: Optional[str] = None,
        entity_links: Optional[Sequence[EntityReference]] = None,
        time_granularity: Optional[TimeGranularity] = None,
        date_part: Optional[DatePart] = None,
    ) -> EntityLinkPatternParameterSet:
        return EntityLinkPatternParameterSet(
            fields_to_compare=tuple(sorted(fields_to_compare)),
            element_name=element_name,
            entity_links=tuple(entity_links) if entity_links is not None else None,
            time_granularity=time_granularity,
            date_part=date_part,
        )

    def __post_init__(self) -> None:
        """Check that fields_to_compare is sorted so that patterns that do the same thing can be compared."""
        assert is_sorted(self.fields_to_compare)


@dataclass(frozen=True)
class EntityLinkPattern(SpecPattern):
    """A pattern that matches group-by-items using the entity-link-path specification.

    The entity link path specifies how a group-by-item for a metric query should be constructed. The group-by-item
    is obtained by joining the semantic model containing the measure to a semantic model containing the group-by-
    item using a specified entity. Additional semantic models can be joined using additional entities to obtain the
    group-by-item. The series of entities that are used form the entity path. Since the entity path does not specify
    which semantic models need to be used, additional resolution is done in later stages to generate the necessary SQL.
    """

    parameter_set: EntityLinkPatternParameterSet

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        filtered_candidate_specs = InstanceSpecSet.from_specs(candidate_specs).linkable_specs

        matching_specs: List[LinkableInstanceSpec] = []

        # Using some Python introspection magic to figure out specs that match the listed fields.
        keys_to_check = set(field_to_compare.value for field_to_compare in self.parameter_set.fields_to_compare)
        # Checks that EntityLinkPatternParameterSetField is valid wrt to the parameter set.
        parameter_set_values = tuple(getattr(self.parameter_set, key_to_check) for key_to_check in keys_to_check)

        for spec in filtered_candidate_specs:
            spec_values = tuple(getattr(spec, key_to_check, None) for key_to_check in keys_to_check)
            if spec_values == parameter_set_values:
                matching_specs.append(spec)

        return matching_specs
