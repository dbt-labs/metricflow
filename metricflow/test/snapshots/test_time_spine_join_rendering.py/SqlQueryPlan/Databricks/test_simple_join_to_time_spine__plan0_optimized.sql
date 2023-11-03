-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_12.ds AS metric_time__day
  , subq_10.bookings AS bookings_join_to_time_spine
FROM ***************************.mf_time_spine subq_12
LEFT OUTER JOIN (
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements:
    --   ['bookings', 'metric_time__day']
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_10001
  ) subq_9
  GROUP BY
    metric_time__day
) subq_10
ON
  subq_12.ds = subq_10.metric_time__day
