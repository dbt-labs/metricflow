from __future__ import annotations

from typing import Dict, Iterable, Tuple, TypeVar

IterableT = TypeVar("IterableT")


def ordered_dedupe(*iterables: Iterable[IterableT]) -> Tuple[IterableT, ...]:
    """De-duplicates the items in the iterables while preserving the order."""
    ordered_results: Dict[IterableT, None] = {}
    for iterable in iterables:
        for item in iterable:
            ordered_results[item] = None

    return tuple(ordered_results.keys())
