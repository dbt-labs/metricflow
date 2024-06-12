-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Constrain Output with WHERE
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    metric_time__day
    , bookings
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      subq_21.metric_time__day AS metric_time__day
      , subq_20.bookings AS bookings
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
      FROM ***************************.mf_time_spine subq_22
      WHERE ds BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    ) subq_21
    LEFT OUTER JOIN (
      -- Constrain Output with WHERE
      -- Aggregate Measures
      SELECT
        metric_time__day
        , SUM(bookings) AS bookings
      FROM (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
        -- Pass Only Elements: ['bookings', 'metric_time__day']
        SELECT
          DATE_TRUNC('day', ds) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_28000
        WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
      ) subq_18
      WHERE metric_time__day > '2020-01-01'
      GROUP BY
        metric_time__day
    ) subq_20
    ON
      subq_21.metric_time__day = subq_20.metric_time__day
  ) subq_23
  WHERE (
    metric_time__day BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
  ) AND (
    metric_time__day > '2020-01-01'
  )
) subq_25
