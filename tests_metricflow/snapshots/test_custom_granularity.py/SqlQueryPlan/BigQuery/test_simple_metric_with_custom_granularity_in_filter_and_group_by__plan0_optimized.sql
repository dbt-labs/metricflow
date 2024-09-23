-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , SUM(bookings) AS bookings
FROM (
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Join to Custom Granularity Dataset
  SELECT
    subq_8.bookings AS bookings
    , subq_9.martian_day AS metric_time__martian_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_8
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_9
  ON
    subq_8.metric_time__day = subq_9.ds
) subq_10
WHERE metric_time__martian_day = '2020-01-01'
GROUP BY
  metric_time__martian_day
