test_name: test_derived_offset_cumulative_metric
test_filename: test_derived_metric_rendering.py
sql_engine: Trino
---
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  metric_time__day
  , every_2_days_bookers_2_days_ago AS every_2_days_bookers_2_days_ago
FROM (
  -- Join to Time Spine Dataset
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  SELECT
    time_spine_src_28006.ds AS metric_time__day
    , subq_23.__bookers AS every_2_days_bookers_2_days_ago
  FROM ***************************.mf_time_spine time_spine_src_28006
  INNER JOIN (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['__bookers', 'metric_time__day']
    -- Pass Only Elements: ['__bookers', 'metric_time__day']
    -- Aggregate Inputs for Simple Metrics
    SELECT
      subq_19.ds AS metric_time__day
      , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS __bookers
    FROM ***************************.mf_time_spine subq_19
    INNER JOIN
      ***************************.fct_bookings bookings_source_src_28000
    ON
      (
        DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_19.ds
      ) AND (
        DATE_TRUNC('day', bookings_source_src_28000.ds) > DATE_ADD('day', -2, subq_19.ds)
      )
    GROUP BY
      subq_19.ds
  ) subq_23
  ON
    DATE_ADD('day', -2, time_spine_src_28006.ds) = subq_23.metric_time__day
) subq_30
