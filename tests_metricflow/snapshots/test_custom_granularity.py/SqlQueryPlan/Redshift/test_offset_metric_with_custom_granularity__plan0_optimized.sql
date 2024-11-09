test_name: test_offset_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
SELECT
  booking__ds__martian_day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__martian_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_13.martian_day AS booking__ds__martian_day
    , SUM(subq_10.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine subq_12
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  ON
    DATEADD(day, -5, subq_12.ds) = subq_10.booking__ds__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_13
  ON
    subq_12.ds = subq_13.ds
  GROUP BY
    subq_13.martian_day
) subq_17
