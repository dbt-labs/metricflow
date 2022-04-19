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


@dataclass(frozen=True)
class Materialization:
    """Object to represent a Metric."""

    name: str
    metrics: List[str]
    dimensions: List[str]
    destination_table: Optional[SqlTable]
