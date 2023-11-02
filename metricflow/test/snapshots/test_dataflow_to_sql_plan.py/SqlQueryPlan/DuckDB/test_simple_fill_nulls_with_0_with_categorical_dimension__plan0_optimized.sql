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
    -- Pass Only Elements:
    --   ['bookings', 'booking__is_instant']
    SELECT
      is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_6
  GROUP BY
    booking__is_instant
) subq_7
