-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings', 'metric_time__martian_day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__martian_day
  , SUM(bookings) AS bookings
FROM (
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Join to Custom Granularity Dataset
  SELECT
    subq_5.bookings AS bookings
    , subq_6.martian_day AS metric_time__martian_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_5
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_6
  ON
    subq_5.ds__day = subq_6.ds
) subq_7
WHERE metric_time__martian_day = '2020-01-01'
GROUP BY
  metric_time__martian_day
