from __future__ import annotations

from metricflow_semantics.toolkit.orderd_enum import OrderedEnum


class Alphabet(OrderedEnum):  # noqa: D101
    A = "a"
    B = "b"
    C = "c"


def test_ordered_enum() -> None:  # noqa: D103
    assert sorted([Alphabet.C, Alphabet.B, Alphabet.A]) == [Alphabet.A, Alphabet.B, Alphabet.C]
    assert tuple(Alphabet) == (Alphabet.A, Alphabet.B, Alphabet.C)
