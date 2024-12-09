test_name: test_join_to_time_spine_metric_grouped_by_custom_grain
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_13.metric_time__martian_day AS metric_time__martian_day
  , subq_12.bookings AS bookings_join_to_time_spine
FROM (
  -- Read From Time Spine 'mf_time_spine'
  SELECT
    martian_day AS metric_time__martian_day
  FROM ***************************.mf_time_spine subq_14
  GROUP BY
    martian_day
) subq_13
LEFT OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  SELECT
    subq_9.martian_day AS metric_time__martian_day
    , SUM(subq_8.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_8
  LEFT OUTER JOIN
    ***************************.mf_time_spine subq_9
  ON
    subq_8.ds__day = subq_9.ds
  GROUP BY
    subq_9.martian_day
) subq_12
ON
  subq_13.metric_time__martian_day = subq_12.metric_time__martian_day
