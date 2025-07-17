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
class MeasureLabel(MetricflowGraphLabel):
    """The name of the measure or `None` for any measure."""

    measure_name: Optional[str]

    @staticmethod
    def get_instance(measure_name: Optional[str] = None) -> MeasureLabel:  # noqa: D102
        return MeasureLabel(measure_name=measure_name)

    def get_instance_from_reference(self, measure_reference: MeasureReference) -> MeasureLabel:  # noqa: D102
        return MeasureLabel(measure_name=measure_reference.element_name)


@singleton_dataclass()
class GroupByMetricLabel(MetricflowGraphLabel):
    """The name of the metric or `None` for any metric."""

    metric_name: Optional[str]

    @staticmethod
    def get_instance(metric_name: Optional[str] = None) -> GroupByMetricLabel:
        return GroupByMetricLabel(metric_name=metric_name)


@singleton_dataclass()
class GroupByAttributeLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> GroupByAttributeLabel:
        return GroupByAttributeLabel()


@singleton_dataclass()
class DsiEntityLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> DsiEntityLabel:
        return DsiEntityLabel()


@singleton_dataclass()
class DunderNameElementLabel(MetricflowGraphLabel):
    element_name: str


@singleton_dataclass()
class TimeDimensionLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class TimeClusterLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> TimeClusterLabel:
        return TimeClusterLabel()


@singleton_dataclass()
class MetricTimeLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> MetricTimeLabel:
        return MetricTimeLabel()


@singleton_dataclass()
class MetricLabel(MetricflowGraphLabel):
    metric_name: Optional[str]

    @staticmethod
    def get_instance(metric_name: Optional[str] = None) -> MetricLabel:
        return MetricLabel(metric_name=metric_name)


@singleton_dataclass()
class BaseMetricLabel(MetricflowGraphLabel):
    metric_name: Optional[str]

    @staticmethod
    def get_instance(metric_name: Optional[str] = None) -> BaseMetricLabel:
        return BaseMetricLabel(metric_name=metric_name)


@singleton_dataclass()
class DerivedMetricLabel(MetricflowGraphLabel):
    metric_name: Optional[str]

    @staticmethod
    def get_instance(metric_name: Optional[str] = None) -> DerivedMetricLabel:
        return DerivedMetricLabel(metric_name=metric_name)


@singleton_dataclass()
class CumulativeMeasureLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> CumulativeMeasureLabel:
        return CumulativeMeasureLabel()


@singleton_dataclass()
class GroupByAttributeRootLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class AggregationLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> AggregationLabel:
        return AggregationLabel()


@singleton_dataclass()
class JoinFromLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class JoinViaLabel(MetricflowGraphLabel):
    pass


@singleton_dataclass()
class JoinedModelLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> JoinedModelLabel:
        return JoinedModelLabel()


@singleton_dataclass()
class LocalModelLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> LocalModelLabel:
        return LocalModelLabel()


@singleton_dataclass()
class KeyEntityClusterLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> KeyEntityClusterLabel:
        return KeyEntityClusterLabel()


@singleton_dataclass()
class KeyAttributeLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> KeyAttributeLabel:
        return KeyAttributeLabel()


@singleton_dataclass()
class KeyEntityLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> KeyEntityLabel:
        return KeyEntityLabel()


@singleton_dataclass()
class DenyDatePartLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> DenyDatePartLabel:
        return DenyDatePartLabel()


@singleton_dataclass()
class DenyEntityKeyQueryResolutionLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> DenyEntityKeyQueryResolutionLabel:
        return DenyEntityKeyQueryResolutionLabel()


@singleton_dataclass()
class DenyVisibleAttributesLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> DenyVisibleAttributesLabel:
        return DenyVisibleAttributesLabel()


@singleton_dataclass()
class ConversionMeasureLabel(MetricflowGraphLabel):
    @staticmethod
    def get_instance() -> ConversionMeasureLabel:
        return ConversionMeasureLabel()
