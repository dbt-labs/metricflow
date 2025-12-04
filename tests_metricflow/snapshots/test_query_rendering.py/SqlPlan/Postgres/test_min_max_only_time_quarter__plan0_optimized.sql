test_name: test_min_max_only_time_quarter
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension and non-default granularity.
sql_engine: Postgres
---
-- Calculate min and max
-- Write to DataTable
SELECT
  MIN(booking__paid_at__quarter) AS booking__paid_at__quarter__min
  , MAX(booking__paid_at__quarter) AS booking__paid_at__quarter__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__quarter']
  -- Pass Only Elements: ['booking__paid_at__quarter']
  SELECT
    DATE_TRUNC('quarter', paid_at) AS booking__paid_at__quarter
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('quarter', paid_at)
) subq_6
