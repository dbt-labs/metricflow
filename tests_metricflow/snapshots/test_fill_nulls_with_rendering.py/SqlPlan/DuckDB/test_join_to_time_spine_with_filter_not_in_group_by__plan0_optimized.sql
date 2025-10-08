test_name: test_join_to_time_spine_with_filter_not_in_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: DuckDB
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_19.metric_time__day AS metric_time__day
  , subq_15.bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      ds AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_17
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (metric_time__month > '2020-01-01')
) subq_19
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings_join_to_time_spine_with_tiered_filters', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__day
    , SUM(bookings_join_to_time_spine_with_tiered_filters) AS bookings_join_to_time_spine_with_tiered_filters
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , DATE_TRUNC('month', ds) AS metric_time__month
      , 1 AS bookings_join_to_time_spine_with_tiered_filters
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_12
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (metric_time__month > '2020-01-01')
  GROUP BY
    metric_time__day
) subq_15
ON
  subq_19.metric_time__day = subq_15.metric_time__day
