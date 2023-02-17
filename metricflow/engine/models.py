from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from metricflow.dataflow.sql_table import SqlTable


@dataclass(frozen=True)
class Metric:
    """Dataclass representation of a Metric."""

    name: str
    dimensions: List[Dimension]


@dataclass(frozen=True)
class Dimension:
    """Dataclass representation of a Dimension."""

    name: str

