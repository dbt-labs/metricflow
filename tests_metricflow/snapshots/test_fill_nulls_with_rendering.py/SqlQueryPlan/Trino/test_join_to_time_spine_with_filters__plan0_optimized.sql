-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    subq_18.metric_time__day AS metric_time__day
    , subq_17.bookings AS bookings
  FROM (
    -- Time Spine
    SELECT
      ds AS metric_time__day
      , DATE_TRUNC('week', ds) AS metric_time__week
    FROM ***************************.mf_time_spine subq_19
    WHERE (
      metric_time__week > '2020-01-01'
    ) AND (
      ds BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    )
  ) subq_18
  LEFT OUTER JOIN (
    -- Constrain Output with WHERE
    -- Pass Only Elements: ['bookings', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
      -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__week']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , DATE_TRUNC('week', ds) AS metric_time__week
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE DATE_TRUNC('day', ds) BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
    ) subq_14
    WHERE metric_time__week > '2020-01-01'
    GROUP BY
      metric_time__day
  ) subq_17
  ON
    subq_18.metric_time__day = subq_17.metric_time__day
  WHERE subq_18.metric_time__day BETWEEN timestamp '2020-01-03' AND timestamp '2020-01-05'
) subq_21
