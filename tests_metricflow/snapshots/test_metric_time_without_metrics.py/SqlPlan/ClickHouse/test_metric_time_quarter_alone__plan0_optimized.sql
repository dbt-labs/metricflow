test_name: test_metric_time_quarter_alone
test_filename: test_metric_time_without_metrics.py
sql_engine: ClickHouse
---
SELECT
  toStartOfQuarter(ds) AS metric_time__quarter
FROM ***************************.mf_time_spine time_spine_src_28006
GROUP BY
  toStartOfQuarter(ds)
