-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_15.metric_time__martian_day AS metric_time__martian_day
  , subq_14.bookings AS bookings_join_to_time_spine
FROM (
  -- Time Spine
  SELECT
    martian_day AS metric_time__martian_day
  FROM ***************************.mf_time_spine subq_16
  GROUP BY
    martian_day
) subq_15
LEFT OUTER JOIN (
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  SELECT
    subq_11.martian_day AS metric_time__martian_day
    , SUM(subq_10.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_11
  ON
    subq_10.metric_time__day = subq_11.ds
  GROUP BY
    subq_11.martian_day
) subq_14
ON
  subq_15.metric_time__martian_day = subq_14.metric_time__martian_day
