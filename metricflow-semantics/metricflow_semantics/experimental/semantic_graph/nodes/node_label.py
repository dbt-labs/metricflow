from __future__ import annotations

import logging
from typing import Optional

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class SemanticGraphLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class MeasureAttributeLabel(MetricflowGraphLabel):
    measure_name: Optional[str]


@singleton_dataclass()
class GroupByAttributeLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class DsiEntityLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class DunderNameElementLabel(MetricflowGraphLabel):
    element_name: str


@singleton_dataclass()
class TimeDimensionLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class MetricTimeLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class GroupByAttributeRootLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class AggregationLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class JoinFromLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class JoinViaLabel(MetricflowGraphLabel):
    pass
