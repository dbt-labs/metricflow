test_name: test_no_metric_custom_granularity_metric_time
test_filename: test_custom_granularity.py
sql_engine: DuckDB
---
-- Metric Time Dimension 'ds'
-- Join to Custom Granularity Dataset
-- Pass Only Elements: ['metric_time__martian_day',]
SELECT
  nr_subq_2.martian_day AS metric_time__martian_day
FROM ***************************.mf_time_spine time_spine_src_28006
LEFT OUTER JOIN
  ***************************.mf_time_spine nr_subq_2
ON
  time_spine_src_28006.ds = nr_subq_2.ds
GROUP BY
  nr_subq_2.martian_day
