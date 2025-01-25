test_name: test_join_to_timespine_metric_with_custom_granularity_filter
test_filename: test_custom_granularity.py
sql_engine: BigQuery
---
-- Join to Time Spine Dataset
-- Compute Metrics via Expressions
SELECT
  nr_subq_18.metric_time__martian_day AS metric_time__martian_day
  , nr_subq_14.bookings AS bookings_join_to_time_spine
FROM (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['metric_time__martian_day',]
  SELECT
    metric_time__martian_day
  FROM (
    -- Read From Time Spine 'mf_time_spine'
    -- Change Column Aliases
    SELECT
      martian_day AS metric_time__martian_day
    FROM ***************************.mf_time_spine time_spine_src_28006
  ) nr_subq_16
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__martian_day
) nr_subq_18
LEFT OUTER JOIN (
  -- Constrain Output with WHERE
  -- Pass Only Elements: ['bookings', 'metric_time__martian_day']
  -- Aggregate Measures
  SELECT
    metric_time__martian_day
    , SUM(bookings) AS bookings
  FROM (
    -- Metric Time Dimension 'ds'
    -- Join to Custom Granularity Dataset
    SELECT
      nr_subq_28002.bookings AS bookings
      , nr_subq_10.martian_day AS metric_time__martian_day
    FROM (
      -- Read Elements From Semantic Model 'bookings_source'
      SELECT
        1 AS bookings
        , DATETIME_TRUNC(ds, day) AS ds__day
      FROM ***************************.fct_bookings bookings_source_src_28000
    ) nr_subq_28002
    LEFT OUTER JOIN
      ***************************.mf_time_spine nr_subq_10
    ON
      nr_subq_28002.ds__day = nr_subq_10.ds
  ) nr_subq_11
  WHERE metric_time__martian_day = '2020-01-01'
  GROUP BY
    metric_time__martian_day
) nr_subq_14
ON
  nr_subq_18.metric_time__martian_day = nr_subq_14.metric_time__martian_day
