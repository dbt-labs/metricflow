from __future__ import annotations

from typing import Sequence

import pytest
from metricflow_semantics.model.semantics.excluded_dimensions import (
    is_declared_excluded_dimension,
    with_excluded_dimensions_allowed,
)
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.model.semantics.linkable_element_set_base import AnnotatedSpec, BaseGroupByItemSet
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import GroupByItemSet
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

from metricflow_semantic_interfaces.references import EntityReference
from metricflow_semantic_interfaces.type_enums.time_granularity import TimeGranularity


def _dimension(element_name: str, *entity_link_names: str) -> DimensionSpec:
    return DimensionSpec(
        element_name=element_name, entity_links=tuple(EntityReference(name) for name in entity_link_names)
    )


@pytest.mark.parametrize(
    "declared_names, spec, expected",
    [
        (("is_instant",), _dimension("is_instant", "booking"), True),
        (("is_instant",), _dimension("is_instant", "booking", "listing"), True),
        (("is_instant",), _dimension("is_instant"), True),
        (("is_instant",), _dimension("country"), False),
        (("booking__is_instant",), _dimension("is_instant", "booking"), True),
        (("booking__is_instant",), _dimension("is_instant", "listing", "booking"), True),
        (("listing__booking__is_instant",), _dimension("is_instant", "listing", "booking"), True),
        (("listing__booking__is_instant",), _dimension("is_instant", "booking"), False),
        (("booking__is_instant",), _dimension("is_instant", "booking", "listing"), False),
        (("booking__is_instant",), _dimension("is_instant"), False),
        (("booking__is_instant",), _dimension("is_instant", "view"), False),
        (("booking__is_instant",), _dimension("country", "booking"), False),
        (("country", "booking__is_instant"), _dimension("is_instant", "booking"), True),
        (("country", "booking__is_instant"), _dimension("country", "view"), True),
        ((), _dimension("country"), False),
    ],
)
def test_declared_names_match_like_group_by_names(  # noqa: D103
    declared_names: Sequence[str], spec: DimensionSpec, expected: bool
) -> None:
    assert is_declared_excluded_dimension(spec, declared_names) is expected


def test_time_dimension_is_never_matched() -> None:  # noqa: D103
    time_dimension_spec = TimeDimensionSpec(
        element_name="ds",
        entity_links=(EntityReference("booking"),),
        time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
    )
    assert not is_declared_excluded_dimension(time_dimension_spec, ("ds", "booking__ds"))


def test_each_path_of_the_same_dimension_is_matched() -> None:  # noqa: D103
    specs = (
        _dimension("country_latest", "listing"),
        _dimension("country_latest", "booking", "listing"),
        _dimension("country_latest", "view", "listing"),
    )
    assert all(is_declared_excluded_dimension(spec, ("listing__country_latest",)) for spec in specs)
    assert all(is_declared_excluded_dimension(spec, ("country_latest",)) for spec in specs)
    assert [is_declared_excluded_dimension(spec, ("booking__listing__country_latest",)) for spec in specs] == [
        False,
        True,
        False,
    ]


def _item_set(*dunder_names: str) -> GroupByItemSet:
    annotated_specs = []
    for dunder_name in dunder_names:
        *entity_link_names, element_name = dunder_name.split("__")
        annotated_specs.append(
            AnnotatedSpec.create(
                element_type=LinkableElementType.DIMENSION,
                element_name=element_name,
                properties=(),
                origin_model_ids=(),
                derived_from_semantic_models=(),
                entity_links=tuple(EntityReference(name) for name in entity_link_names),
                metric_subquery_entity_links=None,
                time_grain=None,
                date_part=None,
            )
        )
    return GroupByItemSet.create(*annotated_specs)


def _dunder_names(item_sets: Sequence[BaseGroupByItemSet]) -> Sequence[Sequence[str]]:
    return [[annotated_spec.spec.dunder_name for annotated_spec in item_set.annotated_specs] for item_set in item_sets]


def test_declared_input_gets_declared_dimensions_of_undeclared_inputs() -> None:  # noqa: D103
    result = with_excluded_dimensions_allowed(
        item_sets=(_item_set("listing__country"), _item_set("listing__country", "listing__is_instant")),
        excluded_dimension_names_per_set=(("listing__is_instant",), ()),
    )
    assert _dunder_names(result) == [
        ["listing__country", "listing__is_instant"],
        ["listing__country", "listing__is_instant"],
    ]


def test_only_declared_names_are_added() -> None:  # noqa: D103
    result = with_excluded_dimensions_allowed(
        item_sets=(
            _item_set("listing__country"),
            _item_set("listing__country", "listing__is_instant", "listing__city"),
        ),
        excluded_dimension_names_per_set=(("listing__is_instant",), ()),
    )
    assert _dunder_names(result)[0] == ["listing__country", "listing__is_instant"]


def test_undeclared_input_is_unchanged() -> None:  # noqa: D103
    undeclared = _item_set("listing__country", "listing__is_instant")
    result = with_excluded_dimensions_allowed(
        item_sets=(_item_set("listing__country"), undeclared),
        excluded_dimension_names_per_set=(("listing__is_instant",), ()),
    )
    assert result[1] is undeclared


def test_each_declared_input_gets_only_its_own_names() -> None:  # noqa: D103
    result = with_excluded_dimensions_allowed(
        item_sets=(
            _item_set("listing__country"),
            _item_set("listing__country"),
            _item_set("listing__country", "listing__is_instant", "listing__city"),
        ),
        excluded_dimension_names_per_set=(("listing__is_instant",), ("listing__city",), ()),
    )
    assert _dunder_names(result) == [
        ["listing__country", "listing__is_instant"],
        ["listing__country", "listing__city"],
        ["listing__country", "listing__is_instant", "listing__city"],
    ]


def test_names_matching_nothing_add_nothing() -> None:  # noqa: D103
    result = with_excluded_dimensions_allowed(
        item_sets=(_item_set("listing__country"), _item_set("listing__country", "listing__is_instant")),
        excluded_dimension_names_per_set=(("booking__is_instant", "other"), ()),
    )
    assert _dunder_names(result) == [["listing__country"], ["listing__country", "listing__is_instant"]]


def test_inputs_are_returned_as_is_without_undeclared_inputs() -> None:  # noqa: D103
    item_sets = (_item_set("listing__country"), _item_set("listing__is_instant"))
    result = with_excluded_dimensions_allowed(
        item_sets=item_sets, excluded_dimension_names_per_set=(("listing__is_instant",), ("listing__country",))
    )
    assert result is item_sets


def test_inputs_are_returned_as_is_when_undeclared_inputs_share_nothing() -> None:  # noqa: D103
    item_sets = (_item_set("listing__country"), _item_set("listing__city"), _item_set("listing__is_instant"))
    result = with_excluded_dimensions_allowed(
        item_sets=item_sets, excluded_dimension_names_per_set=((), (), ("listing__is_instant",))
    )
    assert result is item_sets
