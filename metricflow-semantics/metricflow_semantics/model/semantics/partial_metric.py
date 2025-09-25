from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.type_enums import AggregationType

from metricflow_semantics.collection_helpers.mf_type_aliases import AnyLengthTuple
from metricflow_semantics.experimental.dataclass_helpers import fast_frozen_dataclass

logger = logging.getLogger(__name__)


@fast_frozen_dataclass()
class PartialMetricAggregation:
    """Indirection class used for measure -> simple metric migration."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


@fast_frozen_dataclass()
class PartialMetricNonAdditiveDimension:
    """Indirection class used for measure -> simple metric migration."""

    name: str
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: AnyLengthTuple[str] = ()


@fast_frozen_dataclass()
class PartialMetric:
    """Indirection class used for measure -> simple metric migration."""

    name: str
    agg: AggregationType
    expr: Optional[str] = None
    agg_params: Optional[PartialMetricAggregation] = None
    non_additive_dimension: Optional[PartialMetricNonAdditiveDimension] = None
    agg_time_dimension: Optional[str] = None
