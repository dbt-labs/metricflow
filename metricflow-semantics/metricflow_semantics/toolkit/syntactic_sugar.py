"""Short functions that act as syntactic sugar / allow for easier auto-completion.

In the process of evaluating whether to keep these.
"""
from __future__ import annotations

import itertools
import logging
from typing import Callable, Iterable, Mapping, Optional

from metricflow_semantics.toolkit.mf_type_aliases import KeyT, T, ValueT

logger = logging.getLogger(__name__)


mf_flatten = itertools.chain.from_iterable
mf_group_by = itertools.groupby


def mf_first_non_none(*args: Optional[T]) -> Optional[T]:
    """Return the first non-`None` argument.

    This is helpful because using `a or b` to handle optional items does not work properly for false-like values
    (e.g. 0 or empty string).

    An issue with this method is that all args are evaluated, so it shouldn't be used with expensive expressions
    as arguments. Arguments could be replaced with lambdas.
    """
    for arg in args:
        if arg is not None:
            return arg
    return None


def mf_first_non_none_or_raise(*args: Optional[T], error_supplier: Optional[Callable[[], Exception]] = None) -> T:
    """Similar to `mf_first_non_none` but raises an exception if no values are present."""
    for arg in args:
        if arg is not None:
            return arg

    if error_supplier is not None:
        raise error_supplier()

    raise ValueError("Expected at least one non-`None` argument")


def mf_ensure_mapping(optional_mapping: Optional[Mapping[KeyT, ValueT]]) -> Mapping[KeyT, ValueT]:
    """Returns an empty mapping if `optional_mapping` is `None`.

    Useful for default argument handling.
    """
    return optional_mapping if optional_mapping is not None else {}


def mf_first_item(iterable: Iterable[T], error_supplier: Optional[Callable[[], Exception]] = None) -> T:
    """Return the first item in an iterable."""
    try:
        return next(iter(iterable))
    except StopIteration as e:
        if error_supplier is not None:
            raise error_supplier()

        raise KeyError("Can't return the first item as the iterable has no items") from e
