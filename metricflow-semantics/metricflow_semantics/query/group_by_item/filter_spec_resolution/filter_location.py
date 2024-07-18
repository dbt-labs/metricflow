from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Tuple

from dbt_semantic_interfaces.references import MetricReference
from more_itertools import is_sorted


class WhereFilterLocationType(Enum):
    """Describes the location where the filter is defined."""

    QUERY = "query"
    METRIC = "metric"
    INPUT_METRIC = "input_metric"


@dataclass(frozen=True)
class WhereFilterLocation:
    """Describe the location of a where filter.

    When describing a query, the metric references are the metrics in the query.
    When describing a metric, the metric reference just the metric where the filter is defined.
    When describing an input metric, the metric references are the outer metric and the input metric.

    Since this is used to resolve valid group-by items for a where filter, this is sufficient as the valid group-by
    items depend only on the metrics queried.
    """

    location_type: WhereFilterLocationType
    # These should be sorted for consistency in comparisons.
    metric_references: Tuple[MetricReference, ...]

    def __post_init__(self) -> None:  # noqa: D105
        assert is_sorted(self.metric_references)

    @staticmethod
    def for_query(metric_references: Sequence[MetricReference]) -> WhereFilterLocation:  # noqa: D102
        return WhereFilterLocation(
            metric_references=tuple(sorted(metric_references)), location_type=WhereFilterLocationType.QUERY
        )

    @staticmethod
    def for_metric(metric_reference: MetricReference) -> WhereFilterLocation:  # noqa: D102
        return WhereFilterLocation(metric_references=(metric_reference,), location_type=WhereFilterLocationType.METRIC)

    @staticmethod
    def for_input_metric(input_metric_reference: MetricReference) -> WhereFilterLocation:  # noqa: D102
        return WhereFilterLocation(
            metric_references=(input_metric_reference,), location_type=WhereFilterLocationType.INPUT_METRIC
        )
