-- Compute Metrics via Expressions
SELECT
  COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  -- Pass Only Elements: ['bookings',]
  -- Aggregate Measures
  SELECT
    SUM(1) AS bookings
  FROM ***************************.fct_bookings bookings_source_src_28000
  WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
) subq_9
