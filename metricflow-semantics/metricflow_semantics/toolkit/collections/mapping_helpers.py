from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence, Set
from typing import Iterable

from metricflow_semantics.toolkit.mf_type_aliases import (
    KeyT,
    MappingItemsTuple,
    Pair,
    ValueT,
)

logger = logging.getLogger(__name__)


def mf_mapping_to_items(mapping: Mapping[KeyT, ValueT]) -> MappingItemsTuple[KeyT, ValueT]:
    """Converts the items in a mapping to tuples."""
    return tuple((key, value) for key, value in mapping.items())


def mf_items_to_dict(items: Iterable[Pair[KeyT, ValueT]]) -> dict[KeyT, ValueT]:
    """Convert mapping items represented as tuples to a dict."""
    return {key: value for key, value in items}


def mf_common_keys(dicts: Sequence[Mapping[KeyT, object]]) -> Set[KeyT]:
    """Returns the set of keys that are common to all dictionaries."""
    if not dicts:
        return set()

    first_dict = dicts[0]
    return set(first_dict).intersection(*[other_dict for other_dict in dicts[1:]])
