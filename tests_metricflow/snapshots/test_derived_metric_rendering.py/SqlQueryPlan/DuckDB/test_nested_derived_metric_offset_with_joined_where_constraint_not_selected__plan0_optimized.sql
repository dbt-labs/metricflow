-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day', 'bookings_offset_once']
  SELECT
    metric_time__day
    , bookings_offset_once
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_21.ds AS metric_time__day
      , subq_19.booking__is_instant AS booking__is_instant
      , subq_19.bookings_offset_once AS bookings_offset_once
    FROM ***************************.mf_time_spine subq_21
    INNER JOIN (
      -- Compute Metrics via Expressions
      SELECT
        metric_time__day
        , booking__is_instant
        , 2 * bookings AS bookings_offset_once
      FROM (
        -- Join to Time Spine Dataset
        -- Pass Only Elements: ['bookings', 'booking__is_instant', 'metric_time__day']
        -- Aggregate Measures
        -- Compute Metrics via Expressions
        SELECT
          subq_15.ds AS metric_time__day
          , subq_13.booking__is_instant AS booking__is_instant
          , SUM(subq_13.bookings) AS bookings
        FROM ***************************.mf_time_spine subq_15
        INNER JOIN (
          -- Read Elements From Semantic Model 'bookings_source'
          -- Metric Time Dimension 'ds'
          SELECT
            DATE_TRUNC('day', ds) AS metric_time__day
            , is_instant AS booking__is_instant
            , 1 AS bookings
          FROM ***************************.fct_bookings bookings_source_src_28000
        ) subq_13
        ON
          subq_15.ds - INTERVAL 5 day = subq_13.metric_time__day
        GROUP BY
          subq_15.ds
          , subq_13.booking__is_instant
      ) subq_18
    ) subq_19
    ON
      subq_21.ds - INTERVAL 2 day = subq_19.metric_time__day
  ) subq_22
  WHERE booking__is_instant
) subq_23
