test_name: test_min_max_only_time
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension.
sql_engine: Databricks
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
    DATE_TRUNC('day', paid_at) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', paid_at)
) subq_6
