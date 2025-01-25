test_name: test_simple_fill_nulls_with_0_with_categorical_dimension
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Compute Metrics via Expressions
SELECT
  booking__is_instant
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Measures
  SELECT
    booking__is_instant
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['bookings', 'booking__is_instant']
    SELECT
      is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_4
  GROUP BY
    booking__is_instant
) nr_subq_5
