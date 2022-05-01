from metricflow.dataset.dataset import DataSet
from metricflow.time.time_granularity import TimeGranularity

# Shortcuts for referring to the plot time dimension.
PTD = DataSet.plot_time_dimension_name()
PTD_REFERENCE = DataSet.plot_time_dimension_reference()
PTD_SPEC_DAY = DataSet.plot_time_dimension_spec(TimeGranularity.DAY)
PTD_SPEC_MONTH = DataSet.plot_time_dimension_spec(TimeGranularity.MONTH)
PTD_SPEC_YEAR = DataSet.plot_time_dimension_spec(TimeGranularity.YEAR)
