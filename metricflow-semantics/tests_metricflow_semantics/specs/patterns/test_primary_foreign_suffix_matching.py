"""Test for PRIMARY/FOREIGN entity path suffix matching behavior.

This tests the new behavior added in PR #XXXX to fix Issue #1780.
"""

from __future__ import annotations

from dbt_semantic_interfaces.references import EntityReference
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)


def test_primary_foreign_suffix_matching() -> None:
    """Test that pattern with N+1 entity links matches candidate with N entity links.

    This tests the new behavior added to fix Issue #1780 where MetricFlow should
    recognize that job__businessunit__businessunit_name (FOREIGN path) matches
    businessunit__businessunit_name (PRIMARY path) when they differ by exactly
    one entity link and the shorter path is a suffix of the longer path.
    """
    # Pattern representing job__businessunit__businessunit_name (FOREIGN path)
    pattern = EntityLinkPattern(
        SpecPatternParameterSet.from_parameters(
            element_name="businessunit_name",
            entity_links=(
                EntityReference(element_name="job"),
                EntityReference(element_name="businessunit"),
            ),
            time_granularity_name=None,
            date_part=None,
            fields_to_compare=(
                ParameterSetField.DATE_PART,
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
                ParameterSetField.TIME_GRANULARITY,
            ),
        )
    )

    # Candidate representing businessunit__businessunit_name (PRIMARY path)
    primary_candidate = DimensionSpec(
        element_name="businessunit_name",
        entity_links=(EntityReference(element_name="businessunit"),),
    )

    # Should match because:
    # 1. Pattern has exactly 1 more entity link than candidate (2 vs 1)
    # 2. The candidate's entity links are a suffix of the pattern's entity links
    #    (["businessunit"] is a suffix of ["job", "businessunit"])
    matches = pattern.match([primary_candidate])
    assert len(matches) == 1, (
        "Expected pattern job__businessunit__businessunit_name to match "
        "businessunit__businessunit_name (PRIMARY/FOREIGN entity path equivalence)"
    )
    assert matches[0] == primary_candidate
