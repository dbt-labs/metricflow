from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional
from metricflow.model.parsing.dir_to_model import ModelBuildResult


class CalculationMethod(Enum):  # noqa: D
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    DERIVED = "derived"
    # 'expresision' is being deprecated for 'derived'
    # thus we will support both
    EXPRESSION = "expression"


class TimeGrains(Enum):  # noqa: D
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


class Window:  # noqa: D
    count: int
    period: TimeGrains


class Filter:  # noqa: D
    field: str
    operator: str
    value: str


@dataclass(frozen=True)
class DbtMetric:  # noqa: D
    name: str
    model: Optional[str]
    label: Optional[str]
    description: Optional[str]
    calculation_method: Optional[CalculationMethod]
    # 'type' is being deprecated for 'calculation_method'.
    # thus we will support both, and ensure one exists
    type: Optional[CalculationMethod]
    expression: Optional[str]
    # 'sql' is being deprecated for 'expression'.
    # thus we will support both, and ensure one exists
    sql: Optional[str]
    timestamp: str
    time_grains: List[TimeGrains]
    dimensions: Optional[List[str]]
    window: Optional[Window]
    filters: Optional[List[Filter]]
    meta: Dict


def parse_dbt_project_to_model(
    directory: str,
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    raise NotImplementedError(
        f"Unable to parse dbt project at {directory} to a UserConfiguredModel, because we haven't implemented it"
    )
