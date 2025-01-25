test_name: test_offset_metric_with_custom_granularity
test_filename: test_custom_granularity.py
sql_engine: Trino
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
    nr_subq_13.martian_day AS booking__ds__martian_day
    , SUM(nr_subq_9.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_9
  ON
    DATE_ADD('day', -5, time_spine_src_28006.ds) = nr_subq_9.booking__ds__day
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_13
  ON
    time_spine_src_28006.ds = nr_subq_13.ds
  GROUP BY
    nr_subq_13.martian_day
) nr_subq_17
