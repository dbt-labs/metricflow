-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , 2 * bookings_offset_once AS bookings_offset_twice
FROM (
  -- Join to Time Spine Dataset
  SELECT
    subq_21.metric_time__day AS metric_time__day
    , subq_20.bookings_offset_once AS bookings_offset_once
  FROM (
    -- Date Spine
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine subq_22
    WHERE ds BETWEEN '2020-01-12' AND '2020-01-13'
  ) subq_21
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      metric_time__day
      , 2 * bookings AS bookings_offset_once
    FROM (
      -- Join to Time Spine Dataset
      -- Pass Only Elements:
      --   ['bookings', 'metric_time__day']
      -- Aggregate Measures
      -- Compute Metrics via Expressions
      SELECT
        subq_15.ds AS metric_time__day
        , SUM(subq_13.bookings) AS bookings
      FROM ***************************.mf_time_spine subq_15
      INNER JOIN (
        -- Read Elements From Semantic Model 'bookings_source'
        -- Metric Time Dimension 'ds'
        SELECT
          DATE_TRUNC(ds, day) AS metric_time__day
          , 1 AS bookings
        FROM ***************************.fct_bookings bookings_source_src_10001
      ) subq_13
      ON
        DATE_SUB(CAST(subq_15.ds AS DATETIME), INTERVAL 5 day) = subq_13.metric_time__day
      GROUP BY
        metric_time__day
    ) subq_19
  ) subq_20
  ON
    DATE_SUB(CAST(subq_21.metric_time__day AS DATETIME), INTERVAL 2 day) = subq_20.metric_time__day
) subq_23
