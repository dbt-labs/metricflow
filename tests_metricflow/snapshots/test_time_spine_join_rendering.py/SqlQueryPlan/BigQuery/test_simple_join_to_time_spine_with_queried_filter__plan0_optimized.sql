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
      subq_17.ds AS metric_time__day
      , subq_15.booking__is_instant AS booking__is_instant
      , subq_15.bookings AS bookings
    FROM ***************************.mf_time_spine subq_17
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
            DATETIME_TRUNC(ds, day) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_11
        WHERE booking__is_instant
      ) subq_13
      WHERE booking__is_instant
      GROUP BY
        metric_time__day
        , booking__is_instant
    ) subq_15
    ON
      subq_17.ds = subq_15.metric_time__day
  ) subq_18
  WHERE booking__is_instant
) subq_19
