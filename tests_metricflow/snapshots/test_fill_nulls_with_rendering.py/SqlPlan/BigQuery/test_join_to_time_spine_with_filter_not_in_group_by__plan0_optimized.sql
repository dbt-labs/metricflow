test_name: test_join_to_time_spine_with_filter_not_in_group_by
test_filename: test_fill_nulls_with_rendering.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_18.metric_time__day AS metric_time__day
  , subq_14.bookings AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__day',]
  SELECT
    metric_time__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      ds AS metric_time__day
      , DATETIME_TRUNC(ds, month) AS metric_time__month
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_16
  WHERE (metric_time__day <= '2020-01-02') AND (metric_time__month > '2020-01-01')
) subq_18
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  SELECT
    metric_time__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATETIME_TRUNC(ds, day) AS metric_time__day
      , DATETIME_TRUNC(ds, month) AS metric_time__month
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_11
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (metric_time__month > '2020-01-01')
  GROUP BY
    metric_time__day
) subq_14
ON
  subq_18.metric_time__day = subq_14.metric_time__day