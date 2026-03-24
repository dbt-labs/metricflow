from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.model.semantics.linkable_element import LinkableElementType
from metricflow_semantics.semantic_graph.attribute_resolution.attribute_recipe import IndexedDunderName
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_descriptor import DunderNameDescriptor
from metricflow_semantics.semantic_graph.trie_resolver.dunder_name_trie import (
    MutableDunderNameTrie,
)
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import (
    assert_object_snapshot_equal,
)
from metricflow_semantics.toolkit.string_helpers import mf_dedent

logger = logging.getLogger(__name__)


def test_add_items(request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration) -> None:  # noqa: D103
    indexed_dunder_names: list[IndexedDunderName] = [
        ("listing",),
        ("listing", "user"),
        ("booking", "host"),
        ("booking", "view"),
        ("booking", "host", "country"),
        ("ambiguous",),
        ("ambiguous",),
    ]

    dunder_name_trie = MutableDunderNameTrie()
    descriptor = DunderNameDescriptor(
        element_type=LinkableElementType.ENTITY,
        time_grain=None,
        date_part=None,
        element_properties=(),
        derived_from_model_ids=(),
        origin_model_ids=(),
        entity_key_queries_for_group_by_metric=(),
    )
    dunder_name_trie.add_name_items(((indexed_dunder_name, descriptor) for indexed_dunder_name in indexed_dunder_names))

    indexed_names = [name for name, _ in dunder_name_trie.name_items()]

    assert_object_snapshot_equal(
        request=request,
        snapshot_configuration=mf_test_configuration,
        obj={"names_after_removing_ambiguous_names": indexed_names, "trie": dunder_name_trie},
        expectation_description=mf_dedent(
            """
            `ambiguous` should not show up since the union operation removes common items.
            """
        ),
    )


def test_union() -> None:  # noqa: D103
    descriptor = DunderNameDescriptor(
        element_type=LinkableElementType.DIMENSION,
        time_grain=None,
        date_part=None,
        element_properties=(),
        derived_from_model_ids=(),
        origin_model_ids=(),
        entity_key_queries_for_group_by_metric=(),
    )

    left_trie = MutableDunderNameTrie()
    left_trie.add_name_items(
        [
            (("listing", "user"), descriptor),
            (("booking",), descriptor),
            (("listing",), descriptor),
        ]
    )

    right_trie = MutableDunderNameTrie()
    right_trie.add_name_items(
        [
            (("listing", "user"), descriptor),
            (("booking",), descriptor),
            (("listing", "country"), descriptor),
        ]
    )
    assert MutableDunderNameTrie.union_exclude_common([left_trie, right_trie]).dunder_names() == (
        "listing",
        "listing__country",
    )


def test_intersection() -> None:  # noqa: D103
    descriptor = DunderNameDescriptor(
        element_type=LinkableElementType.DIMENSION,
        time_grain=None,
        date_part=None,
        element_properties=(),
        derived_from_model_ids=(),
        origin_model_ids=(),
        entity_key_queries_for_group_by_metric=(),
    )

    left_trie = MutableDunderNameTrie()
    left_trie.add_name_items(
        [
            (("booking",), descriptor),
            (("listing", "user"), descriptor),
            (("listing", "country"), descriptor),
        ]
    )

    right_trie = MutableDunderNameTrie()
    right_trie.add_name_items(
        [
            (("booking",), descriptor),
            (("listing", "user"), descriptor),
            (("listing",), descriptor),
        ]
    )
    assert MutableDunderNameTrie.intersection_merge_common([left_trie, right_trie]).dunder_names() == (
        "booking",
        "listing__user",
    )
