from __future__ import annotations

import logging
from typing import Sequence, Optional

import pytest
from dbt_semantic_interfaces.pretty_print import pformat_big_objects
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity

from metricflow.specs.patterns.dunder_scheme import DunderNamingScheme
from metricflow.specs.patterns.entity_path_pattern import EntityPathPattern, EntityPathPatternParameterSet
from metricflow.specs.patterns.spec_pattern import QueryItemNamingScheme, SpecPattern
from metricflow.specs.specs import DimensionSpec, EntitySpec, LinkableInstanceSpec
from metricflow.test.time.metric_time_dimension import MTD_SPEC_MONTH, MTD_SPEC_WEEK, MTD_SPEC_YEAR

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def specs() -> Sequence[LinkableInstanceSpec]:  # noqa: D
    return (
        # Time dimensions
        MTD_SPEC_WEEK,
        MTD_SPEC_MONTH,
        MTD_SPEC_YEAR,
        # Dimensions
        DimensionSpec(
            element_name="country",
            entity_links=(
                EntityReference(element_name="listing"),
                EntityReference(element_name="user"),
            ),
        ),
        DimensionSpec(element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)),
        # Entities
        EntitySpec(
            element_name="listing",
            entity_links=(EntityReference(element_name="booking"),),
        ),
        EntitySpec(
            element_name="host",
            entity_links=(EntityReference(element_name="booking"),),
        ),
    )


def compare_scoring_results(  # noqa: D
        specs: Sequence[LinkableInstanceSpec],
        naming_scheme: QueryItemNamingScheme[LinkableInstanceSpec],
        pattern: SpecPattern[LinkableInstanceSpec],
        expected_spec_strs: Sequence[str],
        expected_matching_spec: Optional[LinkableInstanceSpec],
) -> None:
    result = pattern.score(specs)

    if expected_matching_spec is not None:
        assert result.has_exactly_one_match
        assert result.matching_spec == expected_matching_spec
    else:
        assert not result.has_exactly_one_match

    # TODO: Remove
    logger.error(
        f"Result is:\n" + pformat_big_objects(
            tuple(naming_scheme.input_str(scored_spec.spec) for scored_spec in result.scored_specs)
        )
    )

    actual_spec_strs = tuple(naming_scheme.input_str(scored_spec.spec) for scored_spec in result.scored_specs)
    assert actual_spec_strs == expected_spec_strs


def test_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    naming_scheme = DunderNamingScheme()
    compare_scoring_results(
        specs=specs,
        naming_scheme=DunderNamingScheme(),
        pattern=EntityPathPattern(
            EntityPathPatternParameterSet(
                element_name="is_instant",
                entity_links=(EntityReference(element_name="booking"),),
                time_granularity=None,
                date_part=None,
                input_string="booking__is_instant",
                naming_scheme=naming_scheme,
            )
        ),
        expected_spec_strs=(
            'booking__is_instant',
            'booking__listing',
            'booking__host',
            'listing__user__country',
            'metric_time__month',
            'metric_time__year',
            'metric_time__week'
        ),
        expected_matching_spec=DimensionSpec(
            element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)
        )
    )


def test_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    naming_scheme = DunderNamingScheme()
    pattern = EntityPathPattern(
        EntityPathPatternParameterSet(
            element_name="is_instant",
            entity_links=(EntityReference(element_name="booking"),),
            time_granularity=None,
            date_part=None,
            input_string="booking__is_instant",
            naming_scheme=naming_scheme,
        )
    )

    result = pattern.score(specs)
    assert result.has_exactly_one_match
    assert result.matching_spec == DimensionSpec(
        element_name="is_instant", entity_links=(EntityReference(element_name="booking"),)
    )

    # TODO: Remove
    logger.error(
        f"Result is:\n" + pformat_big_objects(
            tuple(naming_scheme.input_str(scored_spec.spec) for scored_spec in result.scored_specs)
        )
    )

    assert tuple(naming_scheme.input_str(scored_spec.spec) for scored_spec in result.scored_specs) == (
        'booking__is_instant',
        'booking__listing',
        'booking__host',
        'listing__user__country',
        'metric_time__month',
        'metric_time__year',
        'metric_time__week'
    )


def test_entity_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    naming_scheme = DunderNamingScheme()
    compare_scoring_results(
        specs=specs,
        naming_scheme=DunderNamingScheme(),
        pattern=EntityPathPattern(
            EntityPathPatternParameterSet(
                element_name="listing",
                entity_links=(EntityReference(element_name="booking"),),
                time_granularity=None,
                date_part=None,
                input_string="booking__listing",
                naming_scheme=naming_scheme,
            )
        ),
        expected_spec_strs=(
            'booking__listing',
            'booking__host',
            'booking__is_instant',
            'listing__user__country',
            'metric_time__month',
            'metric_time__week',
            'metric_time__year'),
        expected_matching_spec=EntitySpec(
            element_name="listing", entity_links=(EntityReference(element_name="booking"),)
        )
    )


def test_time_dimension_match(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    naming_scheme = DunderNamingScheme()
    compare_scoring_results(
        specs=specs,
        naming_scheme=DunderNamingScheme(),
        pattern=EntityPathPattern(
            EntityPathPatternParameterSet(
                element_name="metric_time",
                entity_links=(),
                time_granularity=TimeGranularity.WEEK,
                date_part=None,
                input_string="metric_time__week",
                naming_scheme=naming_scheme,
            )
        ),
        expected_spec_strs=(
            'metric_time__week',
            'metric_time__year',
            'metric_time__month',
            'listing__user__country',
            'booking__listing',
            'booking__is_instant',
            'booking__host'
        ),
        expected_matching_spec=MTD_SPEC_WEEK,
    )


def test_time_dimension_match_without_specified(specs: Sequence[LinkableInstanceSpec]) -> None:  # noqa: D
    naming_scheme = DunderNamingScheme()
    compare_scoring_results(
        specs=specs,
        naming_scheme=DunderNamingScheme(),
        pattern=EntityPathPattern(
            EntityPathPatternParameterSet(
                element_name="metric_time",
                entity_links=(),
                time_granularity=TimeGranularity.WEEK,
                date_part=None,
                input_string="metric_time__week",
                naming_scheme=naming_scheme,
            )
        ),
        expected_spec_strs=(
            'metric_time__week',
            'metric_time__year',
            'metric_time__month',
            'listing__user__country',
            'booking__listing',
            'booking__is_instant',
            'booking__host'
        ),
        expected_matching_spec=MTD_SPEC_WEEK,
    )