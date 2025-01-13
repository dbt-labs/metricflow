test_name: test_min_max_only_time_quarter
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension and non-default granularity.
sql_engine: Clickhouse
---
-- Calculate min and max
SELECT
  MIN(booking__paid_at__quarter) AS booking__paid_at__quarter__min
  , MAX(booking__paid_at__quarter) AS booking__paid_at__quarter__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__quarter',]
  SELECT
    DATE_TRUNC('quarter', paid_at) AS booking__paid_at__quarter
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('quarter', paid_at)
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_3
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
