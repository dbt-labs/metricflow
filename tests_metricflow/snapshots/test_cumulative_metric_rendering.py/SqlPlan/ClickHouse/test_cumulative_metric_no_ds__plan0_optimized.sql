test_name: test_cumulative_metric_no_ds
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a cumulative metric with no time dimension specified.
sql_engine: ClickHouse
---
SELECT
  SUM(revenue) AS trailing_2_months_revenue
FROM ***************************.fct_revenue revenue_src_28000
SETTINGS join_use_nulls = 1
