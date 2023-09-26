from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from metricflow.specs.merge_builder import Mergeable, MergeBuilder


@dataclass(frozen=True)
class NumberList(Mergeable):  # noqa: D
    numbers: List[int] = field(default_factory=list)

    def merge(self: NumberList, other: NumberList) -> NumberList:  # noqa: D
        return NumberList(self.numbers + other.numbers)


def test_merge_builder() -> None:  # noqa: D
    number_list_builder = MergeBuilder[NumberList](NumberList())

    number_list_builder.add(NumberList([1]))
    number_list_builder.add(NumberList([2, 3]))
    number_list_builder.add_all(
        NumberList([4]),
        NumberList([5]),
    )
    number_list_builder.add_iterable([NumberList([6]), NumberList([7])])

    assert number_list_builder.build_result == NumberList([1, 2, 3, 4, 5, 6, 7])


def test_merge_iterable() -> None:  # noqa: D
    assert MergeBuilder.merge_iterable(NumberList(), [NumberList([1, 2, 3])]) == NumberList([1, 2, 3])
