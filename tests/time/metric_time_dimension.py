from __future__ import annotations

from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

from metricflow.dataset.dataset import DataSet

# Shortcuts for referring to the metric time dimension.
MTD = DataSet.metric_time_dimension_name()
MTD_REFERENCE = DataSet.metric_time_dimension_reference()
MTD_SPEC_DAY = DataSet.metric_time_dimension_spec(TimeGranularity.DAY)
MTD_SPEC_WEEK = DataSet.metric_time_dimension_spec(TimeGranularity.WEEK)
MTD_SPEC_MONTH = DataSet.metric_time_dimension_spec(TimeGranularity.MONTH)
MTD_SPEC_QUARTER = DataSet.metric_time_dimension_spec(TimeGranularity.QUARTER)
MTD_SPEC_YEAR = DataSet.metric_time_dimension_spec(TimeGranularity.YEAR)
