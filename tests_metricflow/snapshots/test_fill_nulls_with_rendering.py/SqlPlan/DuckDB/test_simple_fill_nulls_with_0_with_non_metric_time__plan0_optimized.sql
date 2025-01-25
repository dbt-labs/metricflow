test_name: test_simple_fill_nulls_with_0_with_non_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Compute Metrics via Expressions
SELECT
  booking__paid_at__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Measures
  SELECT
    booking__paid_at__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'booking__paid_at__day']
    SELECT
      DATE_TRUNC('day', paid_at) AS booking__paid_at__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_4
  GROUP BY
    booking__paid_at__day
) nr_subq_5
