from __future__ import annotations
import logging
from hashlib import sha1
from typing import Sequence

from metricflow.sql.sql_column_type import SqlColumnType

logger = logging.getLogger(__name__)


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()
