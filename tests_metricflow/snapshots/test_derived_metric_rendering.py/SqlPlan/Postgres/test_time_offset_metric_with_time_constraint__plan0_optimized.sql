test_name: test_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Postgres
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , bookings_5_days_ago AS bookings_5_day_lag
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Compute Metrics via Expressions
  SELECT
    subq_19.metric_time__day AS metric_time__day
    , subq_15.bookings AS bookings_5_days_ago
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_19
  INNER JOIN (
    -- Aggregate Inputs for Simple Metrics
    SELECT
      metric_time__day
      , SUM(bookings) AS bookings
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      -- Metric Time Dimension 'ds'
      -- Pass Only Elements: ['bookings', 'metric_time__day']
      SELECT
        DATE_TRUNC('day', ds) AS metric_time__day
        , 1 AS bookings
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) subq_14
    GROUP BY
      metric_time__day
  ) subq_15
  ON
    subq_19.metric_time__day - MAKE_INTERVAL(days => 5) = subq_15.metric_time__day
  WHERE subq_19.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
) subq_22
