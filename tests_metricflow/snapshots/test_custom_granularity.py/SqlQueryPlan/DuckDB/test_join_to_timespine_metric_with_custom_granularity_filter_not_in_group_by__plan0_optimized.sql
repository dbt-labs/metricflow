-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_18.metric_time__day AS metric_time__day
  , subq_17.bookings AS bookings_join_to_time_spine
FROM (
  -- Filter Time Spine
  SELECT
    metric_time__day
  FROM (
    -- Time Spine
    SELECT
      ds AS metric_time__day
      , ds AS metric_time__martian_day
    FROM ***************************.mf_time_spine subq_19
  ) subq_20
  WHERE metric_time__martian_day = '2020-01-01'
) subq_18
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Pass Only Elements: ['bookings', 'metric_time__day', 'metric_time__day']
    -- Join to Custom Granularity Dataset
    SELECT
      subq_12.metric_time__day AS metric_time__day
      , subq_12.bookings AS bookings
      , subq_13.martian_day AS metric_time__martian_day
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_12
    LEFT OUTER JOIN
      ***************************.mf_time_spine subq_13
    ON
      subq_12.metric_time__day = subq_13.ds
  ) subq_14
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__day
) subq_17
ON
  subq_18.metric_time__day = subq_17.metric_time__day
