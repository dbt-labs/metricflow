test_name: test_join_to_time_spine_with_filter_not_in_group_by_using_agg_time
test_filename: test_fill_nulls_with_rendering.py
sql_engine: Databricks
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  subq_18.booking__ds__day AS booking__ds__day
  , subq_14.bookings AS bookings_join_to_time_spine_with_tiered_filters
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['booking__ds__day',]
  SELECT
    booking__ds__day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      ds AS booking__ds__day
      , ds AS metric_time__day
      , DATE_TRUNC('month', ds) AS booking__ds__month
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) subq_16
  WHERE (metric_time__day <= '2020-01-02') AND (booking__ds__month > '2020-01-01')
) subq_18
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'booking__ds__day']
  -- Aggregate Measures
  SELECT
    booking__ds__day
    , SUM(bookings) AS bookings
  FROM (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS booking__ds__day
      , DATE_TRUNC('month', ds) AS booking__ds__month
      , DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_11
  WHERE ((metric_time__day >= '2020-01-02') AND (metric_time__day <= '2020-01-02')) AND (booking__ds__month > '2020-01-01')
  GROUP BY
    booking__ds__day
) subq_14
ON
  subq_18.booking__ds__day = subq_14.booking__ds__day
