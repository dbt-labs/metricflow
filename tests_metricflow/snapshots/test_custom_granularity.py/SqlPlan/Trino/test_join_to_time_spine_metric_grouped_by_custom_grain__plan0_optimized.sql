test_name: test_join_to_time_spine_metric_grouped_by_custom_grain
test_filename: test_custom_granularity.py
sql_engine: Trino
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  nr_subq_14.metric_time__martian_day AS metric_time__martian_day
  , nr_subq_11.bookings AS bookings_join_to_time_spine
FROM (
  -- Read From Time Spine 'mf_time_spine'
  -- Change Column Aliases
  -- Pass Only Elements: ['metric_time__martian_day',]
  SELECT
    martian_day AS metric_time__martian_day
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    martian_day
) nr_subq_14
LEFT OUTER JOIN (
  -- Metric Time Dimension 'ds'
  -- Join to Custom Granularity Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  SELECT
    nr_subq_8.martian_day AS metric_time__martian_day
    , SUM(nr_subq_28002.bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    SELECT
      1 AS bookings
      , DATE_TRUNC('day', ds) AS ds__day
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) nr_subq_28002
  LEFT OUTER JOIN
    ***************************.mf_time_spine nr_subq_8
  ON
    nr_subq_28002.ds__day = nr_subq_8.ds
  GROUP BY
    nr_subq_8.martian_day
) nr_subq_11
ON
  nr_subq_14.metric_time__martian_day = nr_subq_11.metric_time__martian_day
