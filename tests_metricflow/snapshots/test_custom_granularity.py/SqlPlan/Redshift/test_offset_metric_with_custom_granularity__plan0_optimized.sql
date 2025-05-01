test_name: test_offset_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Redshift
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__ds__alien_day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'booking__ds__alien_day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_16.alien_day AS booking__ds__alien_day
    , SUM(subq_12.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  ON
    DATEADD(day, -5, time_spine_src_28006.ds) = subq_12.booking__ds__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_16
  ON
    time_spine_src_28006.ds = subq_16.ds
  GROUP BY
    subq_16.alien_day
) subq_20
