test_name: test_min_max_only_time_quarter
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension and non-default granularity.
sql_engine: BigQuery
---
-- Calculate min and max
SELECT
  MIN(booking__paid_at__quarter) AS booking__paid_at__quarter__min
  , MAX(booking__paid_at__quarter) AS booking__paid_at__quarter__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__quarter']
  SELECT
    DATETIME_TRUNC(paid_at, quarter) AS booking__paid_at__quarter
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    booking__paid_at__quarter
) subq_3
