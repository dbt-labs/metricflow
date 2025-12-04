test_name: test_simple_fill_nulls_with_0_with_categorical_dimension
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Databricks
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__is_instant
  , COALESCE(__bookings_fill_nulls_with_0, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Aggregate Inputs for Simple Metrics
  SELECT
    booking__is_instant
    , SUM(__bookings_fill_nulls_with_0) AS __bookings_fill_nulls_with_0
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'booking__is_instant']
    -- Pass Only Elements: ['__bookings_fill_nulls_with_0', 'booking__is_instant']
    SELECT
      is_instant AS booking__is_instant
      , 1 AS __bookings_fill_nulls_with_0
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  GROUP BY
    booking__is_instant
) subq_10
