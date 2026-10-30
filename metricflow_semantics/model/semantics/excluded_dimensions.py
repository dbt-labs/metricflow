from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Sequence

from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseGroupByItemSet
from metricflow_semantics.naming.linkable_spec_name import DUNDER
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import GroupByItemSet
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import ParameterSetField, SpecPatternParameterSet
from metricflow_semantics.specs.patterns.typed_patterns import DimensionPattern

from metricflow_semantic_interfaces.references import EntityReference


@lru_cache(maxsize=None)
def _pattern_for_declared_name(declared_name: str) -> DimensionPattern:
    *entity_link_names, element_name = declared_name.split(DUNDER)
    # With ENTITY_LINKS in fields_to_compare, empty entity links match only specs without entity links,
    # so a bare name must leave it out to match the dimension through any entity path.
    fields_to_compare = (
        (ParameterSetField.ELEMENT_NAME, ParameterSetField.ENTITY_LINKS)
        if entity_link_names
        else (ParameterSetField.ELEMENT_NAME,)
    )
    return DimensionPattern(
        parameter_set=SpecPatternParameterSet.from_parameters(
            fields_to_compare=fields_to_compare,
            element_name=element_name,
            entity_links=tuple(EntityReference(name) for name in entity_link_names) if entity_link_names else None,
        ),
        include_time_dimensions=False,
    )


def is_declared_excluded_dimension(spec: LinkableInstanceSpec, declared_names: Iterable[str]) -> bool:
    """Return whether the dimension spec matches any name in `exclude_dimensions`.

    A bare name matches the dimension through any entity path. A name with entity links matches as a suffix of
    the spec's entity links, like a group-by name in a query. Specs are matched one at a time because a pattern
    keeps only the shortest entity path when several specs match.
    """
    return any(len(_pattern_for_declared_name(name).match((spec,))) > 0 for name in declared_names)


def is_declared_excluded_annotated_dimension(annotated_spec: AnnotatedSpec, declared_names: Iterable[str]) -> bool:
    """Same as `is_declared_excluded_dimension`, for an annotated spec."""
    return annotated_spec.element_type is LinkableElementType.DIMENSION and is_declared_excluded_dimension(
        annotated_spec.spec, declared_names
    )


def with_excluded_dimensions_allowed(
    item_sets: Sequence[BaseGroupByItemSet],
    excluded_dimension_names_per_set: Sequence[Sequence[str]],
) -> Sequence[BaseGroupByItemSet]:
    """Add to each declaring input the dimensions it declared in `exclude_dimensions` that the other inputs share.

    Returns the sets as is if every input declares names or the inputs that don't declare any share nothing.
    """
    undeclared_item_sets = tuple(
        item_set
        for item_set, excluded_dimension_names in zip(item_sets, excluded_dimension_names_per_set)
        if len(excluded_dimension_names) == 0
    )
    if len(undeclared_item_sets) == 0:
        return item_sets
    common_item_set_of_undeclared_inputs = undeclared_item_sets[0].intersection(*undeclared_item_sets[1:])
    if common_item_set_of_undeclared_inputs.is_empty:
        return item_sets
    return tuple(
        (
            GroupByItemSet.create(
                *item_set.annotated_specs,
                *(
                    annotated_spec
                    for annotated_spec in common_item_set_of_undeclared_inputs.annotated_specs
                    if is_declared_excluded_annotated_dimension(annotated_spec, excluded_dimension_names)
                ),
            )
            if len(excluded_dimension_names) > 0
            else item_set
        )
        for item_set, excluded_dimension_names in zip(item_sets, excluded_dimension_names_per_set)
    )
