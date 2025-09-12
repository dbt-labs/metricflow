test_name: test_cumulative_time_offset_metric_with_time_constraint
test_filename: test_derived_metric_rendering.py
sql_engine: Snowflake
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
  -- Compute Metrics via Expressions
  SELECT
    subq_27.metric_time__day AS metric_time__day
    , subq_23.bookers AS every_2_days_bookers_2_days_ago
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['metric_time__day']
    SELECT
      ds AS metric_time__day
    FROM ***************************.mf_time_spine time_spine_src_28006
    WHERE ds BETWEEN '2019-12-19' AND '2020-01-02'
  ) subq_27
  INNER JOIN (
    -- Join Self Over Time Range
    -- Constrain Time Range to [2019-12-19T00:00:00, 2020-01-02T00:00:00]
    -- Pass Only Elements: ['bookers', 'metric_time__day']
    -- Aggregate Measures
    SELECT
      subq_19.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_19.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_19.ds)
      )
    WHERE subq_19.ds BETWEEN '2019-12-19' AND '2020-01-02'
    GROUP BY
      subq_19.ds
  ) subq_23
  ON
    DATEADD(day, -2, subq_27.metric_time__day) = subq_23.metric_time__day
  WHERE subq_27.metric_time__day BETWEEN '2019-12-19' AND '2020-01-02'
) subq_30
