test_name: test_simple_fill_nulls_with_0_with_non_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__paid_at__day
  , COALESCE(bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    booking__paid_at__day
    , SUM(bookings_fill_nulls_with_0) AS bookings_fill_nulls_with_0
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings_fill_nulls_with_0', 'booking__paid_at__day']
    SELECT
      DATETIME_TRUNC(paid_at, day) AS booking__paid_at__day
      , 1 AS bookings_fill_nulls_with_0
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_7
  GROUP BY
    booking__paid_at__day
) subq_8
