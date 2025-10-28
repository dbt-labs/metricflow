from __future__ import annotations

import logging

from metricflow_semantics.toolkit.collections.ordered_set import FrozenOrderedSet, MutableOrderedSet

logger = logging.getLogger(__name__)


def test_create() -> None:  # noqa: D103
    example_set = MutableOrderedSet((1, 2, 3, 2, 1))
    assert tuple(example_set) == (1, 2, 3)
    assert len(example_set) == 3
    assert 2 in example_set and 4 not in example_set


def test_equality() -> None:  # noqa: D103
    a = MutableOrderedSet((1, 2, 3))
    b = MutableOrderedSet((3, 2, 1))
    assert a == b
    assert tuple(a) == (1, 2, 3)
    assert tuple(b) == (3, 2, 1)


def test_intersection() -> None:  # noqa: D103
    left = MutableOrderedSet((1, 2, 3))
    right = {1, 2}
    result = left.intersection(right)
    assert isinstance(result, MutableOrderedSet)
    assert tuple(result) == (1, 2)  # order from *left* operand


def test_union() -> None:  # noqa: D103
    left = MutableOrderedSet((1, 2))
    right = [3, 2]
    assert tuple(left.union(right)) == (1, 2, 3)


def test_difference() -> None:  # noqa: D103
    left = MutableOrderedSet((1, 2))
    others = [(3,), (2,)]
    assert tuple(left.difference(*others)) == (1,)


def test_operators() -> None:  # noqa: D103
    left = MutableOrderedSet((1, 2, 3))
    right = MutableOrderedSet((1, 3, 4))

    assert tuple(left - right) == (2,)
    assert tuple(left | right) == (1, 2, 3, 4)


def test_mutation() -> None:  # noqa: D103
    example_set: MutableOrderedSet[int] = MutableOrderedSet()
    example_set.add(1)
    example_set.update((2, 3), [4])
    assert tuple(example_set) == (1, 2, 3, 4)

    example_set.discard(3)
    assert tuple(example_set) == (1, 2, 4)

    example_set.discard(0)
    assert tuple(example_set) == (1, 2, 4)


def test_eq() -> None:  # noqa: D103
    assert MutableOrderedSet((1,)) == FrozenOrderedSet((1,))


def test_intersection_update() -> None:  # noqa: D103
    example_set = MutableOrderedSet((1, 2, 3))
    example_set.intersection_update({1, 2}, {2})
    assert tuple(example_set) == (2,)


def test_difference_update() -> None:  # noqa: D103
    example_set = MutableOrderedSet((1, 2, 3))
    example_set.difference_update({1}, {3, 4})
    assert tuple(example_set) == (2,)


def test_pop() -> None:  # noqa: D103
    example_set = MutableOrderedSet(
        (
            1,
            2,
        )
    )

    popped_items = []
    i = 0
    while i < len(example_set) + 2:
        try:
            popped_items.append(example_set.pop())
        except KeyError:
            break
        i += 1
    assert i == 2

    assert popped_items == [1, 2]
