test_name: test_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_12.ds AS metric_time__day
    , SUM(subq_10.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine subq_12
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_10
  ON
    DATE_ADD('day', -5, subq_12.ds) = subq_10.metric_time__day
  WHERE subq_12.ds BETWEEN timestamp '2019-12-19' AND timestamp '2020-01-02'
  GROUP BY
    subq_12.ds
) subq_17
