from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Iterable, TypeVar

from metricflow_semantics.collection_helpers.mf_type_aliases import (
    KeyT,
    MappingItemsTuple,
    Pair,
    ValueT,
)

logger = logging.getLogger(__name__)


T1 = TypeVar("T1")
T2 = TypeVar("T2")


def mf_mapping_to_items(mapping: Mapping[KeyT, ValueT]) -> MappingItemsTuple[KeyT, ValueT]:
    """Converts the items in a mapping to tuples."""
    return tuple((key, value) for key, value in mapping.items())


def mf_items_to_dict(items: Iterable[Pair[KeyT, ValueT]]) -> dict[KeyT, ValueT]:
    """Convert mapping items represented as tuples to a dict."""
    return {key: value for key, value in items}
