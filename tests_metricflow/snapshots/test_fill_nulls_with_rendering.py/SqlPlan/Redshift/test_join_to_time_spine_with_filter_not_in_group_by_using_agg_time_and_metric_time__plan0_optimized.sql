test_name: test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time_and_metric_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Redshift
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
-- Write to DataTable
SELECT
  subq_23.metric_time__day AS metric_time__day
  , subq_18.__bookings_join_to_time_spine_with_tiered_filters AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day']
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    -- Pass Only Elements: ['metric_time__day', 'booking__ds__month']
    SELECT
      ds AS metric_time__day
      , DATE_TRUNC('month', ds) AS booking__ds__month
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_21
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (booking__ds__month > '2020-01-01')
) subq_23
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['__bookings_join_to_time_spine_with_tiered_filters', 'metric_time__day']
  -- Aggregate Inputs for Simple Metrics
  SELECT
    metric_time__day
    , SUM(bookings_join_to_time_spine_with_tiered_filters) AS __bookings_join_to_time_spine_with_tiered_filters
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    -- Pass Only Elements: ['__bookings_join_to_time_spine_with_tiered_filters', 'metric_time__day', 'booking__ds__month']
    SELECT
      DATE_TRUNC('month', ds) AS booking__ds__month
      , DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings_join_to_time_spine_with_tiered_filters
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_15
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (booking__ds__month > '2020-01-01')
  GROUP BY
    metric_time__day
) subq_18
ON
  subq_23.metric_time__day = subq_18.metric_time__day
