test_name: test_simple_metric_constraint_with_single_expr_and_alias
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , delayed_bookings * 2 AS double_counted_delayed_bookings
FROM (
  SELECT
    metric_time__day
    , SUM(__bookings) AS delayed_bookings
  FROM (
    SELECT
      metric_time__day
      , bookings AS __bookings
    FROM (
      SELECT
        toStartOfDay(ds) AS metric_time__day
        , is_instant AS booking__is_instant
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_10
    WHERE NOT booking__is_instant
  ) subq_12
  GROUP BY
    metric_time__day
) subq_14
