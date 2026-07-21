test_name: test_metric_time_date_part
test_filename: test_granularity_date_part_rendering.py
sql_engine: ClickHouse
---
SELECT
  toYear(ds) AS metric_time__extract_year
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  toYear(ds)
