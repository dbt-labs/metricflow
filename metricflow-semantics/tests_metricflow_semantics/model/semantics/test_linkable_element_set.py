"""Module for testing linkable element set operations.

Note this module departs from our typical approach of defining a bajillion fixtures and wiring them into functions
because the base object types involved here are highly specific to the assertions we want to make in these tests.
Rather than making function calls, we simply initialize these things at module scope. This opens us up to possible
output divergence if someone updates one of these things by reference inside the LinkableElementSet, but since we
are not supposed to be doing that anyway that's actually a reasonably handy feature.
"""

from __future__ import annotations

import pytest
from dbt_semantic_interfaces.protocols.dimension import DimensionType
from dbt_semantic_interfaces.references import (
    EntityReference,
    MetricReference,
    SemanticModelReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.linkable_element import (
    ElementPathKey,
    LinkableDimension,
    LinkableElementType,
    LinkableEntity,
    LinkableMetric,
    MetricSubqueryJoinPathElement,
    SemanticModelJoinPath,
    SemanticModelJoinPathElement,
    SemanticModelToMetricSubqueryJoinPath,
)
from metricflow_semantics.model.semantics.linkable_element_set import LinkableElementSet
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    ParameterSetField,
    SpecPatternParameterSet,
)
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity


@pytest.fixture(scope="session")
def linkable_set() -> LinkableElementSet:  # noqa: D103
    measure_source = SemanticModelReference("measure_source")
    entity_0 = EntityReference("entity_0")
    entity_0_source = SemanticModelReference("entity_0_source")
    entity_1 = EntityReference("entity_1")
    entity_1_source = SemanticModelReference("entity_1_source")
    entity_2 = EntityReference("entity_2")
    entity_2_source = SemanticModelReference("entity_2_source")
    entity_3 = EntityReference("entity_3")
    entity_3_source = SemanticModelReference("entity_3_source")
    entity_4 = EntityReference("entity_4")
    entity_4_source = SemanticModelReference("entity_4_source")

    return LinkableElementSet(
        path_key_to_linkable_dimensions={
            ElementPathKey(
                element_name="dimension_element",
                entity_links=(entity_0,),
                element_type=LinkableElementType.DIMENSION,
            ): (
                LinkableDimension.create(
                    defined_in_semantic_model=SemanticModelReference("dimension_source"),
                    element_name="dimension_element",
                    dimension_type=DimensionType.CATEGORICAL,
                    entity_links=(entity_0,),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=measure_source,
                        path_elements=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=entity_0_source,
                                join_on_entity=entity_0,
                            ),
                        ),
                    ),
                    properties=frozenset(),
                    time_granularity=None,
                    date_part=None,
                ),
            ),
            ElementPathKey(
                element_name="time_dimension_element",
                entity_links=(entity_1,),
                element_type=LinkableElementType.TIME_DIMENSION,
                time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
            ): (
                LinkableDimension.create(
                    defined_in_semantic_model=SemanticModelReference("time_dimension_source"),
                    element_name="time_dimension_element",
                    dimension_type=DimensionType.TIME,
                    entity_links=(entity_1,),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=measure_source,
                        path_elements=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=entity_1_source,
                                join_on_entity=entity_1,
                            ),
                        ),
                    ),
                    properties=frozenset(),
                    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
                    date_part=None,
                ),
            ),
        },
        path_key_to_linkable_entities={
            ElementPathKey(
                element_name="entity_element",
                entity_links=(entity_2,),
                element_type=LinkableElementType.ENTITY,
            ): (
                LinkableEntity.create(
                    defined_in_semantic_model=SemanticModelReference("entity_source"),
                    element_name="entity_element",
                    entity_links=(entity_2,),
                    join_path=SemanticModelJoinPath(
                        left_semantic_model_reference=measure_source,
                        path_elements=(
                            SemanticModelJoinPathElement(
                                semantic_model_reference=entity_2_source,
                                join_on_entity=entity_2,
                            ),
                        ),
                    ),
                    properties=frozenset(),
                ),
            )
        },
        path_key_to_linkable_metrics={
            ElementPathKey(
                element_name="metric_element",
                entity_links=(entity_4, entity_3, entity_2),
                element_type=LinkableElementType.METRIC,
                metric_subquery_entity_links=(entity_2,),
            ): (
                LinkableMetric.create(
                    properties=frozenset([LinkableElementProperty.METRIC, LinkableElementProperty.JOINED]),
                    join_path=SemanticModelToMetricSubqueryJoinPath(
                        metric_subquery_join_path_element=MetricSubqueryJoinPathElement(
                            metric_reference=MetricReference("metric_element"),
                            derived_from_semantic_models=(
                                SemanticModelReference(semantic_model_name="metric_semantic_model"),
                            ),
                            join_on_entity=entity_2,
                            entity_links=(entity_4, entity_3),
                            metric_to_entity_join_path=SemanticModelJoinPath(
                                left_semantic_model_reference=measure_source,
                                path_elements=(
                                    SemanticModelJoinPathElement(
                                        semantic_model_reference=entity_4_source, join_on_entity=entity_4
                                    ),
                                    SemanticModelJoinPathElement(
                                        semantic_model_reference=entity_3_source, join_on_entity=entity_3
                                    ),
                                ),
                            ),
                        ),
                        semantic_model_join_path=SemanticModelJoinPath.from_single_element(
                            left_semantic_model_reference=measure_source,
                            right_semantic_model_reference=entity_3_source,
                            join_on_entity=entity_3,
                        ),
                    ),
                ),
            )
        },
    )


def test_derived_semantic_models(linkable_set: LinkableElementSet) -> None:
    """Tests that the semantic models in the element set are returned via `derived_from_semantic_models`."""
    assert tuple(linkable_set.derived_from_semantic_models) == (
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
    )


def test_filter_by_pattern(linkable_set: LinkableElementSet) -> None:
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
