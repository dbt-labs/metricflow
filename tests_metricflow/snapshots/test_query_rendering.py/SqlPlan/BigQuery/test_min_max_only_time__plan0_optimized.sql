test_name: test_min_max_only_time
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension.
sql_engine: BigQuery
---
-- Calculate min and max
-- Write to DataTable
SELECT
  MIN(booking__paid_at__day) AS booking__paid_at__day__min
  , MAX(booking__paid_at__day) AS booking__paid_at__day__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__day']
  -- Pass Only Elements: ['booking__paid_at__day']
  SELECT
    DATETIME_TRUNC(paid_at, day) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    booking__paid_at__day
) subq_6
