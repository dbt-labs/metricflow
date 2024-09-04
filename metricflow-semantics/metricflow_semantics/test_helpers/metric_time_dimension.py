from __future__ import annotations

from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.unique_valid_name import MetricFlowReservedKeywords

from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec
from metricflow_semantics.time.granularity import ExpandedTimeGranularity

# Shortcuts for referring to the metric time dimension.
MTD = MetricFlowReservedKeywords.METRIC_TIME.value
MTD_REFERENCE = TimeDimensionReference(element_name=MetricFlowReservedKeywords.METRIC_TIME.value)
MTD_SPEC_DAY = TimeDimensionSpec(
    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
    entity_links=(),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.DAY),
)
MTD_SPEC_WEEK = TimeDimensionSpec(
    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
    entity_links=(),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.WEEK),
)
MTD_SPEC_MONTH = TimeDimensionSpec(
    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
    entity_links=(),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.MONTH),
)
MTD_SPEC_QUARTER = TimeDimensionSpec(
    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
    entity_links=(),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.QUARTER),
)
MTD_SPEC_YEAR = TimeDimensionSpec(
    element_name=MetricFlowReservedKeywords.METRIC_TIME.value,
    entity_links=(),
    time_granularity=ExpandedTimeGranularity.from_time_granularity(TimeGranularity.YEAR),
)
