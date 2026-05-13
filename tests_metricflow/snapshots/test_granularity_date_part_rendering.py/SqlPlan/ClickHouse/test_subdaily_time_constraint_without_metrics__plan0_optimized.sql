test_name: test_subdaily_time_constraint_without_metrics
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  ts AS metric_time__second
FROM ***************************.mf_time_spine_second time_spine_src_28003
WHERE ts BETWEEN '2020-01-01 00:00:02' AND '2020-01-01 00:00:08'
GROUP BY
  ts
