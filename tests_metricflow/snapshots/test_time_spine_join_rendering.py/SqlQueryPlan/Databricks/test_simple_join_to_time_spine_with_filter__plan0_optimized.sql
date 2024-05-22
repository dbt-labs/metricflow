-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_24.ds AS metric_time__day
    , subq_22.bookings AS bookings
  FROM ***************************.mf_time_spine subq_24
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
      SELECT
        metric_time__day
        , booking__is_instant
        , bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , is_instant AS booking__is_instant
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
      ) subq_17
      WHERE booking__is_instant
    ) subq_19
    WHERE booking__is_instant
    GROUP BY
      metric_time__day
  ) subq_22
  ON
    subq_24.ds = subq_22.metric_time__day
) subq_25
