test_name: test_offset_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  booking__ds__alien_day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['__bookings', 'booking__ds__alien_day']
  -- Pass Only Elements: ['__bookings', 'booking__ds__alien_day']
  -- Aggregate Inputs for Simple Metrics
  -- Compute Metrics via Expressions
  SELECT
    subq_19.alien_day AS booking__ds__alien_day
    , SUM(subq_14.__bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS __bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_14
  ON
    time_spine_src_28006.ds - MAKE_INTERVAL(days => 5) = subq_14.booking__ds__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_19
  ON
    time_spine_src_28006.ds = subq_19.ds
  GROUP BY
    subq_19.alien_day
) subq_24
