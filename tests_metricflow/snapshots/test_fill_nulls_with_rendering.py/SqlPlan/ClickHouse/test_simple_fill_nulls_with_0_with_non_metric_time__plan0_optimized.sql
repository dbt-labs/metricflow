test_name: test_simple_fill_nulls_with_0_with_non_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: ClickHouse
---
SELECT
  booking__paid_at__day
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  SELECT
    booking__paid_at__day
    , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
  FROM (
    SELECT
      toStartOfDay(paid_at) AS booking__paid_at__day
      , 1 AS __bookings_fill_nulls_with_0
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  GROUP BY
    booking__paid_at__day
) subq_10
