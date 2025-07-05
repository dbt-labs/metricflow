from __future__ import annotations

import logging
from typing import Optional

from dbt_semantic_interfaces.references import MeasureReference

from metricflow_semantics.experimental.mf_graph.graph_labeling import MetricflowGraphLabel
from metricflow_semantics.experimental.singleton_decorator import singleton_dataclass

logger = logging.getLogger(__name__)


class SemanticGraphLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class MeasureAttributeLabel(MetricflowGraphLabel):
    """The name of the measure or `None` for any measure."""

    measure_name: Optional[str]

    def get_instance(self, measure_name: str) -> MeasureAttributeLabel:  # noqa: D102
        return MeasureAttributeLabel(measure_name=measure_name)

    def get_instance_from_reference(self, measure_reference: MeasureReference) -> MeasureAttributeLabel:  # noqa: D102
        return MeasureAttributeLabel(measure_name=measure_reference.element_name)


@singleton_dataclass()
class MetricAttributeLabel(MetricflowGraphLabel):
    """The name of the metric or `None` for any metric."""

    metric_name: Optional[str]


@singleton_dataclass()
class GroupByAttributeLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class DsiEntityLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class DsiEntityKeyAttributeLabel(MetricflowGraphLabel):
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


@singleton_dataclass()
class SemanticModelLabel(MetricflowGraphLabel):
    pass
