test_name: test_min_max_only_time
test_filename: test_query_rendering.py
docstring:
  Tests a min max only query with a time dimension.
sql_engine: Clickhouse
---
-- Calculate min and max
SELECT
  MIN(booking__paid_at__day) AS booking__paid_at__day__min
  , MAX(booking__paid_at__day) AS booking__paid_at__day__max
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Pass Only Elements: ['booking__paid_at__day',]
  SELECT
    DATE_TRUNC('day', paid_at) AS booking__paid_at__day
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', paid_at)
  SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
) subq_3
SETTINGS allow_experimental_join_condition = 1, allow_experimental_analyzer = 1, join_use_nulls = 0
