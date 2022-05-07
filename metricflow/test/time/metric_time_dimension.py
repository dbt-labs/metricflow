from metricflow.dataset.dataset import DataSet
from metricflow.time.time_granularity import TimeGranularity

# Shortcuts for referring to the plot time dimension.
MTD = DataSet.metric_time_dimension_name()
MTD_REFERENCE = DataSet.metric_time_dimension_reference()
MTD_SPEC_DAY = DataSet.metic_time_dimension_spec(TimeGranularity.DAY)
MTD_SPEC_MONTH = DataSet.metic_time_dimension_spec(TimeGranularity.MONTH)
MTD_SPEC_YEAR = DataSet.metic_time_dimension_spec(TimeGranularity.YEAR)
