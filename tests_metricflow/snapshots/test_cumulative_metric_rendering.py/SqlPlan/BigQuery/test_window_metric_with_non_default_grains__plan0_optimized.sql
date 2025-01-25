test_name: test_window_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grains.

      Uses both metric_time and agg_time_dimension. Excludes default grain.
sql_engine: BigQuery
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
      , DATETIME_TRUNC(time_spine_src_28006.ds, isoweek) AS metric_time__week
      , DATETIME_TRUNC(time_spine_src_28006.ds, month) AS booking__ds__month
      , nr_subq_17.bookers AS bookers
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['bookers', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Measures
      SELECT
        DATETIME_TRUNC(nr_subq_14.ds, month) AS booking__ds__month
        , nr_subq_14.ds AS metric_time__day
        , DATETIME_TRUNC(nr_subq_14.ds, isoweek) AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS bookers
      FROM ***************************.mf_time_spine nr_subq_14
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATETIME_TRUNC(bookings_source_src_28000.ds, day) <= nr_subq_14.ds
        ) AND (
          DATETIME_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(nr_subq_14.ds AS DATETIME), INTERVAL 2 day)
        )
      GROUP BY
        booking__ds__month
        , metric_time__day
        , metric_time__week
    ) nr_subq_17
    ON
      time_spine_src_28006.ds = nr_subq_17.metric_time__day
  ) nr_subq_21
) nr_subq_23
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
