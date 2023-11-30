from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple

from typing_extensions import override

from metricflow.collection_helpers.merger import Mergeable


@dataclass(frozen=True)
class NumberTuple(Mergeable):  # noqa: D
    numbers: Tuple[int, ...] = field(default_factory=tuple)

    @override
    def merge(self: NumberTuple, other: NumberTuple) -> NumberTuple:
        return NumberTuple(self.numbers + other.numbers)

    @override
    @classmethod
    def empty_instance(cls) -> NumberTuple:
        return NumberTuple()


def test_merger() -> None:  # noqa: D
    items_to_merge: List[NumberTuple] = [
        NumberTuple(()),
        NumberTuple((1,)),
        NumberTuple((2, 3)),
    ]

    assert NumberTuple.merge_iterable(items_to_merge) == NumberTuple((1, 2, 3))
