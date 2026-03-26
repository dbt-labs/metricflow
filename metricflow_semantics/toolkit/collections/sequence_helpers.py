from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Iterator

from metricflow_semantics.toolkit.mf_type_aliases import T

logger = logging.getLogger(__name__)


def mf_chunk(seq: Sequence[T], chunk_size: int) -> Iterator[Sequence[T]]:
    """Return `chunk_sized` slices of the given sequence."""
    for i in range(0, len(seq), chunk_size):
        yield seq[i : i + chunk_size]
