test_name: test_window_metric_with_non_default_grains
test_filename: test_cumulative_metric_rendering.py
docstring:
  Tests rendering a query for a cumulative window metric queried with non-default grains.

      Uses both metric_time and agg_time_dimension. Excludes default grain.
sql_engine: Trino
---
-- Re-aggregate Metric via Group By
-- Write to DataTable
SELECT
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
FROM (
  -- Compute Metrics via Expressions
  -- Compute Metrics via Expressions
  -- Window Function for Metric Re-aggregation
  SELECT
    metric_time__week
    , booking__ds__month
    , FIRST_VALUE(COALESCE(__bookers_fill_nulls_with_0_join_to_timespine, 0)) OVER (
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
      , DATE_TRUNC('week', time_spine_src_28006.ds) AS metric_time__week
      , DATE_TRUNC('month', time_spine_src_28006.ds) AS booking__ds__month
      , subq_24.__bookers_fill_nulls_with_0_join_to_timespine AS __bookers_fill_nulls_with_0_join_to_timespine
    FROM ***************************.mf_time_spine time_spine_src_28006
    LEFT OUTER JOIN (
      -- Join Self Over Time Range
      -- Pass Only Elements: ['__bookers_fill_nulls_with_0_join_to_timespine', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Pass Only Elements: ['__bookers_fill_nulls_with_0_join_to_timespine', 'metric_time__week', 'booking__ds__month', 'metric_time__day']
      -- Aggregate Inputs for Simple Metrics
      SELECT
        DATE_TRUNC('month', subq_20.ds) AS booking__ds__month
        , subq_20.ds AS metric_time__day
        , DATE_TRUNC('week', subq_20.ds) AS metric_time__week
        , COUNT(DISTINCT bookings_source_src_28000.guest_id) AS __bookers_fill_nulls_with_0_join_to_timespine
      FROM ***************************.mf_time_spine subq_20
      INNER JOIN
        ***************************.fct_bookings bookings_source_src_28000
      ON
        (
          DATE_TRUNC('day', bookings_source_src_28000.ds) <= subq_20.ds
        ) AND (
          DATE_TRUNC('day', bookings_source_src_28000.ds) > DATE_ADD('day', -2, subq_20.ds)
        )
      GROUP BY
        DATE_TRUNC('month', subq_20.ds)
        , subq_20.ds
        , DATE_TRUNC('week', subq_20.ds)
    ) subq_24
    ON
      time_spine_src_28006.ds = subq_24.metric_time__day
  ) subq_29
) subq_32
GROUP BY
  metric_time__week
  , booking__ds__month
  , every_two_days_bookers_fill_nulls_with_0
