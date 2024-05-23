-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , booking__is_instant
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Constrain Output with WHERE
  SELECT
    metric_time__day
    , booking__is_instant
    , bookings
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_19.ds AS metric_time__day
      , subq_17.booking__is_instant AS booking__is_instant
      , subq_17.bookings AS bookings
    FROM ***************************.mf_time_spine subq_19
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Aggregate Measures
      SELECT
        metric_time__day
        , booking__is_instant
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
        ) subq_13
        WHERE booking__is_instant
      ) subq_15
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
        , booking__is_instant
    ) subq_17
    ON
      subq_19.ds = subq_17.metric_time__day
  ) subq_20
  WHERE booking__is_instant
) subq_21
