from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Tuple

from dbt_semantic_interfaces.references import MetricReference
from more_itertools import is_sorted


@dataclass(frozen=True)
class WhereFilterLocation:
    """Describe the location of a where filter.

    When describing a query, the metric references are the metrics in the query.
    When describing a metric, the metric reference is just the metric.

    Since this is used to resolve valid group-by items for a where filter, this is sufficient as the valid group-by
    items depend only on the metrics queried.
    """

    # These should be sorted for consistency in comparisons.
    metric_references: Tuple[MetricReference, ...]

    def __post_init__(self) -> None:  # noqa: D
        assert is_sorted(self.metric_references)

    @staticmethod
    def for_query(metric_references: Sequence[MetricReference]) -> WhereFilterLocation:  # noqa: D
        return WhereFilterLocation(metric_references=tuple(sorted(metric_references)))

    @staticmethod
    def for_metric(metric_reference: MetricReference) -> WhereFilterLocation:  # noqa: D
        return WhereFilterLocation(metric_references=(metric_reference,))
