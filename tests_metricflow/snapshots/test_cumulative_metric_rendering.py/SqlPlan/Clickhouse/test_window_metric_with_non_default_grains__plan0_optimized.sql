test_name: test_window_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grains.

      Uses both metric_time and agg_time_dimension. Excludes default grain.
sql_engine: Clickhouse
---
-- Re-aggregate Metric via Group By
SELECT
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , booking__ds__month
    , FIRST_VALUE(COALESCE(bookers, 0)) OVER (
      PARTITION BY
        metric_time__week
        , booking__ds__month
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS every_two_days_bookers_fill_nulls_with_0
  FROM (
    -- Join to Time Spine Dataset
    SELECT
      time_spine_src_28006.ds AS metric_time__day
      , date_trunc('week', time_spine_src_28006.ds) AS metric_time__week
      , date_trunc('month', time_spine_src_28006.ds) AS booking__ds__month
      , subq_19.bookers AS bookers
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        date_trunc('month', subq_16.ds) AS booking__ds__month
        , subq_16.ds AS metric_time__day
        , date_trunc('week', subq_16.ds) AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM ***************************.mf_time_spine subq_16
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          date_trunc('day', bookings_source_src_28000.ds) <= subq_16.ds
        ) AND (
          date_trunc('day', bookings_source_src_28000.ds) > DATEADD(day, -2, subq_16.ds)
        )
      GROUP BY
        booking__ds__month
        , metric_time__day
        , metric_time__week
    ) subq_19
    ON
      time_spine_src_28006.ds = subq_19.metric_time__day
  ) subq_23
) subq_25
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
