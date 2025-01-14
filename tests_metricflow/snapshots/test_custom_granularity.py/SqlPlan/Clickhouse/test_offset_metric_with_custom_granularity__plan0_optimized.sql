test_name: test_offset_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Clickhouse
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
    subq_15.martian_day AS booking__ds__martian_day
    , SUM(subq_11.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      date_trunc('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_11
  ON
    DATEADD(day, -5, time_spine_src_28006.ds) = subq_11.booking__ds__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_15
  ON
    time_spine_src_28006.ds = subq_15.ds
  GROUP BY
    booking__ds__martian_day
) subq_19
