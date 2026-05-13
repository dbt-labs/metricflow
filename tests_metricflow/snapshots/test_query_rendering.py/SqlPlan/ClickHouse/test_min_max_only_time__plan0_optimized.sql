test_name: test_min_max_only_time
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension.
sql_engine: ClickHouse
---
SELECT
  MIN(booking__paid_at__day) AS booking__paid_at__day__min
  , MAX(booking__paid_at__day) AS booking__paid_at__day__max
FROM (
  SELECT
    toStartOfDay(paid_at) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    toStartOfDay(paid_at)
) subq_6
