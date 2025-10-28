"""Module for testing linkable element set operations.

Note this module departs from our typical approach of defining a bajillion fixtures and wiring them into functions
because the base object types involved here are highly specific to the assertions we want to make in these tests.
Rather than making function calls, we simply initialize these things at module scope. This opens us up to possible
output divergence if someone updates one of these things by reference inside the LinkableElementSet, but since we
are not supposed to be doing that anyway that's actually a reasonably handy feature.
"""

from __future__ import annotations

import pytest
from dbt_semantic_interfaces.references import (
    EntityReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.linkable_element_property import GroupByItemProperty
from metricflow_semantics.model.semantics.linkable_element import (
    LinkableElementType,
)
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.semantic_graph.model_id import SemanticModelId
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@pytest.fixture(scope="session")
def linkable_set() -> GroupByItemSet:  # noqa: D103
    measure_source = SemanticModelReference("measure_source")
    entity_0 = EntityReference("entity_0")
    entity_1 = EntityReference("entity_1")
    entity_2 = EntityReference("entity_2")
    entity_3 = EntityReference("entity_3")
    entity_4 = EntityReference("entity_4")

    return GroupByItemSet.create(
        AnnotatedSpec.create(
            element_type=LinkableElementType.DIMENSION,
            element_name="dimension_element",
            entity_links=(entity_0,),
            time_grain=None,
            date_part=None,
            metric_subquery_entity_links=None,
            properties=frozenset(),
            origin_model_ids=(SemanticModelId.get_instance("dimension_source"),),
            derived_from_semantic_models=(
                SemanticModelReference("dimension_source"),
                SemanticModelReference("entity_0_source"),
                measure_source,
            ),
        ),
        AnnotatedSpec.create(
            element_type=LinkableElementType.TIME_DIMENSION,
            element_name="time_dimension_element",
            entity_links=(entity_1,),
            time_grain=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            date_part=None,
            metric_subquery_entity_links=None,
            properties=frozenset(),
            origin_model_ids=(SemanticModelId.get_instance("time_dimension_source"),),
            derived_from_semantic_models=(
                SemanticModelReference("time_dimension_source"),
                SemanticModelReference("entity_1_source"),
                measure_source,
            ),
        ),
        AnnotatedSpec.create(
            element_type=LinkableElementType.ENTITY,
            element_name="entity_element",
            entity_links=(entity_2,),
            time_grain=None,
            date_part=None,
            metric_subquery_entity_links=None,
            properties=frozenset(),
            origin_model_ids=(SemanticModelId.get_instance("entity_source"),),
            derived_from_semantic_models=(
                SemanticModelReference("entity_source"),
                SemanticModelReference("entity_2_source"),
                measure_source,
            ),
        ),
        AnnotatedSpec.create(
            element_type=LinkableElementType.METRIC,
            element_name="metric_element",
            entity_links=(entity_4, entity_3, entity_2),
            time_grain=None,
            date_part=None,
            metric_subquery_entity_links=(entity_2,),
            properties=frozenset([GroupByItemProperty.METRIC, GroupByItemProperty.JOINED]),
            origin_model_ids=(SemanticModelId.get_instance("metric_semantic_model"),),
            derived_from_semantic_models=(
                SemanticModelReference("metric_semantic_model"),
                SemanticModelReference("entity_3_source"),
                SemanticModelReference("entity_4_source"),
                measure_source,
            ),
        ),
    )


def test_derived_semantic_models(linkable_set: GroupByItemSet) -> None:
    """Tests that the semantic models in the element set are returned via `derived_from_semantic_models`."""
    assert set(linkable_set.derived_from_semantic_models) == {
        SemanticModelReference(semantic_model_name="dimension_source"),
        SemanticModelReference(semantic_model_name="entity_0_source"),
        SemanticModelReference(semantic_model_name="entity_1_source"),
        SemanticModelReference(semantic_model_name="entity_2_source"),
        SemanticModelReference(semantic_model_name="entity_3_source"),
        SemanticModelReference(semantic_model_name="entity_4_source"),
        SemanticModelReference(semantic_model_name="entity_source"),
        SemanticModelReference(semantic_model_name="measure_source"),
        SemanticModelReference(semantic_model_name="metric_semantic_model"),
        SemanticModelReference(semantic_model_name="time_dimension_source"),
    }


def test_filter_by_pattern(linkable_set: GroupByItemSet) -> None:
    """Tests that the specs produced by the set are properly filtered by spec patterns."""
    spec_pattern = EntityLinkPattern(
        SpecPatternParameterSet(
            fields_to_compare=(ParameterSetField.ENTITY_LINKS,),
            element_name=None,
            entity_links=(EntityReference("entity_1"),),
        )
    )

    assert tuple(linkable_set.filter_by_spec_patterns((spec_pattern,)).specs) == (
        TimeDimensionSpec(
            element_name="time_dimension_element",
            entity_links=(EntityReference("entity_1"),),
            time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            date_part=None,
        ),
    )
