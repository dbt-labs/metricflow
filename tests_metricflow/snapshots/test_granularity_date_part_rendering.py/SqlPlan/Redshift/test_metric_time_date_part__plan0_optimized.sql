test_name: test_metric_time_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: Redshift
---
-- Read From Time Spine 'mf_time_spine'
-- Metric Time Dimension 'ds'
-- Pass Only Elements: ['metric_time__extract_year']
-- Pass Only Elements: ['metric_time__extract_year']
-- Write to DataTable
SELECT
  EXTRACT(year FROM ds) AS metric_time__extract_year
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  EXTRACT(year FROM ds)
