-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , delayed_bookings * 2 AS double_counted_delayed_bookings
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements:
  --   ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , SUM(bookings) AS delayed_bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'booking__is_instant', 'metric_time__day']
    SELECT
      DATE_TRUNC(ds, day) AS metric_time__day
      , is_instant AS booking__is_instant
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_9
  WHERE NOT booking__is_instant
  GROUP BY
    metric_time__day
) subq_13
