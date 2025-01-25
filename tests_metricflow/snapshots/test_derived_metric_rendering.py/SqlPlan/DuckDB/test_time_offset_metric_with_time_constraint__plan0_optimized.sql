test_name: test_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
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
    time_spine_src_28006.ds AS metric_time__day
    , SUM(nr_subq_9.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_9
  ON
    time_spine_src_28006.ds - INTERVAL 5 day = nr_subq_9.metric_time__day
  WHERE time_spine_src_28006.ds BETWEEN '2019-12-19' AND '2020-01-02'
  GROUP BY
    time_spine_src_28006.ds
) nr_subq_17
