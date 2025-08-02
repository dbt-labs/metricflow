from __future__ import annotations

import logging
from typing import Callable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import T1, T2, T

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
