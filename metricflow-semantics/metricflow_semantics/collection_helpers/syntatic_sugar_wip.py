from __future__ import annotations

import logging
import pathlib
import typing
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar, Iterable

from metricflow_semantics.collection_helpers.mf_type_aliases import T, T1, T2
from metricflow_semantics.helpers.string_helpers import mf_dedent
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.mf_logging.pretty_print import mf_pformat, mf_pformat_dict
from typing_extensions import Self, override

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
