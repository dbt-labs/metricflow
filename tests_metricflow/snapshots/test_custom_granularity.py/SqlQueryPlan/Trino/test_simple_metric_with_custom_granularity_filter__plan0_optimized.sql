-- Constrain Output with WHERE
-- Pass Only Elements: ['bookings',]
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  SUM(bookings) AS bookings
FROM (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  SELECT
    subq_6.bookings AS bookings
    , subq_7.martian_day AS metric_time__martian_day
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_6
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_7
  ON
    subq_6.ds__day = subq_7.ds
) subq_8
WHERE metric_time__martian_day = '2020-01-01'
