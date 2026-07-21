test_name: test_common_semantic_model
test_filename: test_query_rendering.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , SUM(__bookings) AS bookings
  , SUM(__booking_value) AS booking_value
FROM (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , 1 AS __bookings
    , booking_value AS __booking_value
  FROM ***************************.fct_bookings bookings_source_src_28000
) subq_15
GROUP BY
  metric_time__day
