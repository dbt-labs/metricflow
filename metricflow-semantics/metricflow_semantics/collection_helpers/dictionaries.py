from __future__ import annotations

import logging
from collections.abc import Mapping, Set
from typing import Sequence

from metricflow_semantics.collection_helpers.mf_type_aliases import KeyT

logger = logging.getLogger(__name__)


def mf_common_keys(dicts: Sequence[Mapping[KeyT, object]]) -> Set[KeyT]:
    """Returns the set of keys that are common to all dictionaries."""
    if not dicts:
        return set()

    first_dict = dicts[0]
    return set(first_dict).intersection(*[other_dict for other_dict in dicts[1:]])
