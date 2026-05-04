test_name: test_render_query
test_filename: test_rendered_query.py
sql_engine: ClickHouse
---
SELECT
  metric_time__day
  , SUM(__bookings) AS bookings
FROM (
  SELECT
    toStartOfDay(ds) AS metric_time__day
    , 1 AS __bookings
  FROM ***************************.fct_bookings bookings_source_src_10000
) subq_3
GROUP BY
  metric_time__day
