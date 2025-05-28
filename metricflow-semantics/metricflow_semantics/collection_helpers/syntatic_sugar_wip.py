from __future__ import annotations

import logging
from typing import Callable, Iterable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import T1, T2, T
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat

logger = logging.getLogger(__name__)


def mf_get_or_else(optional_item: Optional[T], if_absent: Callable[[], T]) -> T:
    if optional_item is not None:
        return optional_item
    return if_absent()


def mf_match_optional(  # type: ignore[misc]
    optional_item: Optional[T1],
    if_present: Callable[[], T2],
    if_absent: Callable[[], T2],
) -> T2:
    if optional_item is None:
        return if_absent()

    return if_present()


def mf_first_item(iterable: Iterable[T]) -> T:
    """Return the first item in an iterable."""
    try:
        return next(iter(iterable))
    except StopIteration as e:
        raise ValueError(
            LazyFormat("Can't return the first item as the iterable has no items", iterable=iterable)
        ) from e
