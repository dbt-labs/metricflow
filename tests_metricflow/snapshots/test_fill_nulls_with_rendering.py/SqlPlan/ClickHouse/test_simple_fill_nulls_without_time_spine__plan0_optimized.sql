test_name: test_simple_fill_nulls_without_time_spine
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , COALESCE(__bookings_fill_nulls_with_0_without_time_spine, 0) AS bookings_fill_nulls_with_0_without_time_spine
FROM (
  SELECT
    metric_time__day
    , SUM(__bookings_fill_nulls_with_0_without_time_spine) AS __bookings_fill_nulls_with_0_without_time_spine
  FROM (
    SELECT
      toStartOfDay(ds) AS metric_time__day
      , 1 AS __bookings_fill_nulls_with_0_without_time_spine
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  GROUP BY
    metric_time__day
) subq_10
