from __future__ import annotations

import itertools
import logging
from typing import Callable, Optional

from metricflow_semantics.collection_helpers.mf_type_aliases import T

logger = logging.getLogger(__name__)


mf_flatten = itertools.chain.from_iterable


def mf_require_non_none(
    *args: Optional[T],
    exception: Callable[[], Exception] = lambda: ValueError("Expected at least one non-`None` argument"),
) -> T:
    try:
        return next(args for args in args if args is not None)
    except StopIteration:
        raise exception()
