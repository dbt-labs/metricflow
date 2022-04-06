from __future__ import annotations

from hashlib import sha1
from typing import List, Optional

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.utils import ParseableObject, ParseableField, HashableBaseModel
from metricflow.object_utils import ExtendedEnum
from metricflow.specs import MeasureReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity import string_to_time_granularity


class MetricType(ExtendedEnum):
    """Currently supported metric types"""

    MEASURE_PROXY = "measure_proxy"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"


class CumulativeMetricWindow(HashableBaseModel, ParseableField):
    """Describes the window of time the metric should be accumulated over. ie '1 day', '2 weeks', etc"""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    @staticmethod
    def parse(window: str) -> CumulativeMetricWindow:
        """Returns window values if parsing succeeds, None otherwise

        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, ie `28 days`",
            )

        granularity = parts[1]
        # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
        if granularity.endswith("s"):
            # months -> month
            granularity = granularity[:-1]
        if granularity not in [item.value for item in TimeGranularity]:
            raise ParsingException(
                f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
            )

        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return CumulativeMetricWindow(
            count=int(count),
            granularity=string_to_time_granularity(granularity),
        )


class MetricTypeParams(HashableBaseModel, ParseableObject):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    measure: Optional[MeasureReference]
    measures: Optional[List[MeasureReference]]
    numerator: Optional[MeasureReference]
    denominator: Optional[MeasureReference]
    expr: Optional[str]
    window: Optional[CumulativeMetricWindow]
    grain_to_date: Optional[TimeGranularity]


class Metric(HashableBaseModel, ParseableObject):
    """Describes a metric"""

    name: str
    type: MetricType
    type_params: MetricTypeParams
    constraint: Optional[WhereClauseConstraint]

    @property
    def measure_names(self) -> List[MeasureReference]:  # noqa: D
        tp = self.type_params
        res = tp.measures or []
        if tp.measure:
            res.append(tp.measure)
        if tp.numerator:
            res.append(tp.numerator)
        if tp.denominator:
            res.append(tp.denominator)

        return res

    @property
    def definition_hash(self) -> str:  # noqa: D
        values: List[Optional[str]] = [self.name, self.type_params.expr or ""]
        if self.constraint:
            values.append(self.constraint.where)
            if self.constraint.linkable_names:
                values.extend(self.constraint.linkable_names)
        values.extend([m.element_name for m in self.measure_names])

        hash_builder = sha1()
        for s in values:
            if s is None:
                continue
            hash_builder.update(s.encode("utf-8"))
        return hash_builder.hexdigest()
