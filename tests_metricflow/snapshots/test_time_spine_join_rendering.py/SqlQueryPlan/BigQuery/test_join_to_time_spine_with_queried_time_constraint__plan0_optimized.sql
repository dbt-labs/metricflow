-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COALESCE(bookings, 0) AS bookings_fill_nulls_with_0
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2020-01-03T00:00:00, 2020-01-05T00:00:00]
  SELECT
    subq_14.metric_time__day AS metric_time__day
    , subq_13.bookings AS bookings
  FROM (
    -- Time Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_15
    WHERE ds BETWEEN '2020-01-03' AND '2020-01-05'
  ) subq_14
  LEFT OUTER JOIN (
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
        DATETIME_TRUNC(ds, day) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
      WHERE DATETIME_TRUNC(ds, day) BETWEEN '2020-01-03' AND '2020-01-05'
    ) subq_12
    GROUP BY
      metric_time__day
  ) subq_13
  ON
    subq_14.metric_time__day = subq_13.metric_time__day
  WHERE subq_14.metric_time__day BETWEEN '2020-01-03' AND '2020-01-05'
) subq_17
