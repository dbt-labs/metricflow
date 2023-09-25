from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from typing_extensions import override

from metricflow.specs.merge_builder import MergeBuilder
from metricflow.specs.patterns.spec_pattern import QueryItemNamingScheme, ScoringResults, SpecPattern
from metricflow.specs.specs import InstanceSpecSet, InstanceSpecSetTransform, LinkableInstanceSpec
from metricflow.time.date_part import DatePart


@dataclass(frozen=True)
class EntityPathPatternParameterSet:
    """See EntityPathPattern for more details."""

    # The name of the element in the semantic model
    element_name: str
    # The entities used for joining semantic models.
    entity_links: Sequence[EntityReference]
    # If specified, match only time dimensions with the following properties.
    time_granularity: Optional[TimeGranularity]
    date_part: Optional[DatePart]

    # The string that was used to specify this parameter set, and the naming scheme that was used. This is needed for
    # generating suggestions in case there are no matches.
    input_string: str
    naming_scheme: QueryItemNamingScheme[LinkableInstanceSpec]


@dataclass(frozen=True)
class EntityPathPattern(SpecPattern[LinkableInstanceSpec]):
    """A pattern that matches group by items using the entity link path specification.

    The generic parameter LinkableInstanceSpecT determines the types of specs that this match / score.

    The entity link path specifies how a group by item for a metric query should be constructed. The group by item
    is obtained by joining the semantic model containing the measure to a semantic model containing the group by
    item using a specified entity. Additional semantic models can be joined using additional entities to obtain the
    group by item. The series of entities that are used is the entity path. Since the entity path does not specify
    which semantic models need to be used, additional resolution is done in later stages to generate the necessary SQL.

    The logic for matching / scoring a set of specs is:

    * Look for specs that match all entity path parameters. If there are any such matches, return those and score the
      rest by edit distance of the name as defined by the naming scheme.
    * If the entity path parameters does not specify the time granularity / date part, but there are time dimension
      specs that match the entity path and the element name, consider the spec with the finest granularity as the only
      match and score the rest as above.

    The logic above follows the MF query interface.
    """

    parameter_set: EntityPathPatternParameterSet

    @override
    def score(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> ScoringResults:
        spec_set = MergeBuilder.merge_iterable(
            initial_item=InstanceSpecSet(),
            other_iterable=tuple(candidate_spec.as_spec_set for candidate_spec in candidate_specs),
        )

        return _ScoreByEntityPathTransform(self.parameter_set).transform(spec_set)

    @property
    @override
    def naming_scheme(self) -> QueryItemNamingScheme[LinkableInstanceSpec]:
        return self.parameter_set.naming_scheme


@dataclass(frozen=True)
class DimensionMatchingEntityPathPattern(SpecPattern[LinkableInstanceSpec]):
    """Similar to EntityPathPattern but only matches dimensions."""

    parameter_set: EntityPathPatternParameterSet

    @override
    def score(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> ScoringResults:
        spec_set = MergeBuilder.merge_iterable(
            initial_item=InstanceSpecSet(),
            other_iterable=tuple(
                InstanceSpecSet(dimension_specs=candidate_spec.as_spec_set.dimension_specs)
                for candidate_spec in candidate_specs
            ),
        )

        return _ScoreByEntityPathTransform(self.parameter_set).transform(spec_set)

    @property
    @override
    def naming_scheme(self) -> QueryItemNamingScheme[LinkableInstanceSpec]:
        return self.parameter_set.naming_scheme


@dataclass(frozen=True)
class TimeDimensionMatchingEntityPathPattern(SpecPattern[LinkableInstanceSpec]):
    """Similar to EntityPathPattern but only matches time dimensions."""

    parameter_set: EntityPathPatternParameterSet

    @override
    def score(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> ScoringResults:
        spec_set = MergeBuilder.merge_iterable(
            initial_item=InstanceSpecSet(),
            other_iterable=tuple(
                InstanceSpecSet(time_dimension_specs=candidate_spec.as_spec_set.time_dimension_specs)
                for candidate_spec in candidate_specs
            ),
        )

        return _ScoreByEntityPathTransform(self.parameter_set).transform(spec_set)

    @property
    @override
    def naming_scheme(self) -> QueryItemNamingScheme[LinkableInstanceSpec]:
        return self.parameter_set.naming_scheme


@dataclass(frozen=True)
class EntityMatchingEntityPathPattern(SpecPattern[LinkableInstanceSpec]):
    """Similar to EntityPathPattern but only matches entities."""

    parameter_set: EntityPathPatternParameterSet

    @override
    def score(self, candidate_specs: Sequence[LinkableInstanceSpec]) -> ScoringResults:
        spec_set = MergeBuilder.merge_iterable(
            initial_item=InstanceSpecSet(),
            other_iterable=tuple(
                InstanceSpecSet(entity_specs=candidate_spec.as_spec_set.entity_specs)
                for candidate_spec in candidate_specs
            ),
        )

        return _ScoreByEntityPathTransform(self.parameter_set).transform(spec_set)

    @property
    @override
    def naming_scheme(self) -> QueryItemNamingScheme[LinkableInstanceSpec]:
        return self.parameter_set.naming_scheme


class _ScoreByEntityPathTransform(InstanceSpecSetTransform[ScoringResults]):
    """Scores specs according to the description in EntityPathPattern."""

    def __init__(
        self,
        parameter_set: EntityPathPatternParameterSet,
    ) -> None:  # noqa: D
        self._parameter_set = parameter_set

    @override
    def transform(self, spec_set: InstanceSpecSet) -> ScoringResults:
        assert len(spec_set.metadata_specs) == 0
        assert len(spec_set.metric_specs) == 0

        # Check for matches.
        parameter_set = self._parameter_set
        matching_specs: List[LinkableInstanceSpec] = []
        all_specs = spec_set.dimension_specs + spec_set.time_dimension_specs + spec_set.entity_specs

        # TODO: Remove me.
        # # If time_granularity or date_part, the only ones that can match are time dimensions.
        # if parameter_set.time_granularity is not None or parameter_set.date_part is not None:
        #     for time_dimension_spec in spec_set.time_dimension_specs:
        #         if (
        #             time_dimension_spec.element_name == parameter_set.element_name
        #             and time_dimension_spec.entity_links == parameter_set.entity_links
        #             and time_dimension_spec.time_granularity == parameter_set.time_granularity
        #             and time_dimension_spec.date_part == parameter_set.date_part
        #         ):
        #             matching_specs.append(time_dimension_spec)
        #
        #     return SpecPattern.make_scoring_results(
        #         matching_specs=matching_specs,
        #         non_matching_specs=tuple(spec for spec in all_specs if spec not in matching_specs),
        #         input_str=parameter_set.input_string,
        #         naming_scheme=parameter_set.naming_scheme,
        #     )

        # # At this point, we know the time granularity / date part was not specified. See if there's a time dimension
        # # spec that could match.
        # time_dimension_specs_that_could_match = [
        #     time_dimension_spec
        #     for time_dimension_spec in spec_set.time_dimension_specs
        #     if time_dimension_spec.element_name == parameter_set.element_name
        #     and time_dimension_spec.entity_links == parameter_set.entity_links
        # ]
        # if len(time_dimension_specs_that_could_match) > 0:
        #     return SpecPattern.make_scoring_results(
        #         matching_specs=(
        #             min(
        #                 time_dimension_specs_that_could_match,
        #                 key=lambda candidate_spec: candidate_spec.time_granularity,
        #             ),
        #         ),
        #         non_matching_specs=tuple(spec for spec in all_specs if spec not in matching_specs),
        #         input_str=parameter_set.input_string,
        #         naming_scheme=parameter_set.naming_scheme,
        #     )

        for spec in spec_set.time_dimension_specs:
            # If the time granularity was specified but it doesn't match the granularity of the spec, then it can't
            # be an exact match.
            if parameter_set.time_granularity is not None and spec.time_granularity != parameter_set.time_granularity:
                continue
            # Likewise for the date part.
            if parameter_set.date_part is not None and spec.date_part != parameter_set.date_part:
                continue

            if

        # At this point, we know that no time dimension spec matches, so check the other types.
        for spec in spec_set.dimension_specs + spec_set.entity_specs:
            if spec.element_name == parameter_set.element_name and spec.entity_links == parameter_set.entity_links:
                matching_specs.append(spec)

        return SpecPattern.make_scoring_results(
            matching_specs=matching_specs,
            non_matching_specs=tuple(spec for spec in all_specs if spec not in matching_specs),
            input_str=parameter_set.input_string,
            naming_scheme=parameter_set.naming_scheme,
        )
